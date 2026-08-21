"""
Servidor de sinalização para chat estilo Omegle.

Responsabilidades:
- Parear dois usuários, priorizando país/interesses em comum quando possível
  (preferência, não filtro rígido — se não achar ninguém parecido rápido,
  pareia com quem estiver disponível)
- Repassar mensagens de sinalização WebRTC (offer/answer/candidate) entre o par
- Repassar mensagens de chat de texto entre o par
- Tratar "next" (pular pra outro estranho) e desconexões
- Expor um contador simples de quantas pessoas estão online agora

O vídeo/áudio em si NÃO passa por este servidor: depois que o WebRTC é
negociado, a mídia trafega direto entre os dois navegadores (P2P).
Este servidor só ajuda os dois lados a se "apresentarem".
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("omegle-clone")

app = FastAPI()


class Peer:
    def __init__(self, ws: WebSocket):
        self.id = str(uuid.uuid4())[:8]
        self.ws = ws
        self.partner: Optional["Peer"] = None
        self.mode = "video"
        self.country = "any"
        self.tags: set[str] = set()
        self.last_seen = time.monotonic()

    async def send(self, payload: dict) -> bool:
        try:
            await self.ws.send_text(json.dumps(payload))
            return True
        except Exception:
            return False

    def set_preferences(self, mode: str, country: str, interests: list[str]):
        self.mode = mode if mode in ("video", "text") else "video"
        self.country = (country or "any").upper() if country != "any" else "any"
        self.tags = {t.strip().lower() for t in interests if t.strip()}


def match_score(a: Peer, b: Peer) -> tuple[int, list[str]]:
    """Quanto maior o score, mais compatível. `shared` é o que os dois têm em
    comum, só pra mostrar na tela ('em comum: brasil, games')."""
    shared: list[str] = []
    score = 0
    if a.country != "any" and a.country == b.country:
        score += 5
        shared.append(a.country)
    common_tags = a.tags & b.tags
    score += len(common_tags)
    shared.extend(sorted(common_tags))
    return score, shared


class Matchmaker:
    """Mantém a fila de espera e os pares ativos."""

    def __init__(self):
        self.waiting: list[Peer] = []
        self.lock = asyncio.Lock()

    async def enqueue(self, peer: Peer):
        async with self.lock:
            # Vídeo só casa com vídeo, texto só com texto — misturar quebra
            # a chamada (um lado nunca vai ter câmera pra negociar).
            same_mode = [p for p in self.waiting if p.mode == peer.mode]
            if same_mode:
                # Escolhe quem tem mais em comum com o peer que chegou.
                # Se ninguém tiver nada em comum, casa com o primeiro
                # compatível mesmo assim — preferência, não trava.
                best_partner = None
                best_score = -1
                best_shared: list[str] = []
                for candidate in same_mode:
                    score, shared = match_score(peer, candidate)
                    if score > best_score:
                        best_score = score
                        best_partner = candidate
                        best_shared = shared

                self.waiting.remove(best_partner)
                partner = best_partner
                peer.partner = partner
                partner.partner = peer
                await peer.send({"type": "matched", "role": "callee", "peer_id": partner.id, "shared": best_shared})
                await partner.send({"type": "matched", "role": "caller", "peer_id": peer.id, "shared": best_shared})
                logger.info(f"Pareados: {peer.id} <-> {partner.id} (score={best_score})")
            else:
                self.waiting.append(peer)
                await peer.send({"type": "waiting"})

    async def leave_queue(self, peer: Peer):
        async with self.lock:
            if peer in self.waiting:
                self.waiting.remove(peer)

    async def partner_unreachable(self, peer: Peer):
        """Chamado quando o envio pro parceiro falha — a conexão dele morreu
        sem o servidor perceber (comum em redes móveis instáveis). Devolve
        quem ainda está vivo (peer) pra fila."""
        peer.partner = None
        await peer.send({"type": "partner-left"})
        await self.enqueue(peer)

    async def disconnect(self, peer: Peer):
        """Chamado quando o peer sai ou fecha a conexão."""
        await self.leave_queue(peer)
        partner = peer.partner
        if partner:
            partner.partner = None
            peer.partner = None
            await partner.send({"type": "partner-left"})
            # Devolve o parceiro pra fila automaticamente
            await self.enqueue(partner)


matchmaker = Matchmaker()

# Todo mundo com uma conexão WebSocket aberta agora (pareado ou esperando),
# só pra alimentar o contador de "online" na landing page.
active_peers: set[str] = set()

# Registro de todos os peers vivos, pra "tarefa de vigia" conseguir checar
# quem parou de dar sinal de vida (celular que travou, app em segundo plano,
# rede que caiu sem avisar o servidor).
peer_registry: dict[str, Peer] = {}

STALE_TIMEOUT = 40  # segundos sem nenhuma mensagem = considera morto


async def reap_stale_peers():
    """Roda pra sempre em segundo plano: fecha conexões que pararam de
    responder há tempo demais. Isso resolve o bug de "um lado acha que está
    conectado, mas o outro nunca recebe nada" — a conexão zumbi finalmente é
    encerrada de verdade, liberando quem ainda está vivo pra ser re-pareado."""
    while True:
        await asyncio.sleep(10)
        now = time.monotonic()
        for peer_id, peer in list(peer_registry.items()):
            if now - peer.last_seen > STALE_TIMEOUT:
                logger.info(f"Peer {peer_id} sem sinal de vida há {STALE_TIMEOUT}s+, encerrando")
                try:
                    await peer.ws.close()
                except Exception:
                    pass


@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(reap_stale_peers())


@app.get("/api/stats")
async def stats():
    return {"online": len(active_peers)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    peer = Peer(websocket)
    active_peers.add(peer.id)
    peer_registry[peer.id] = peer

    try:
        # Primeira mensagem esperada do cliente: as preferências de matchmaking.
        raw = await websocket.receive_text()
        peer.last_seen = time.monotonic()
        data = json.loads(raw)
        if data.get("type") == "join":
            peer.set_preferences(data.get("mode"), data.get("country"), data.get("interests", []))

        await matchmaker.enqueue(peer)

        while True:
            raw = await websocket.receive_text()
            peer.last_seen = time.monotonic()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            if msg_type == "next":
                # Pula pro próximo estranho, mantendo as mesmas preferências
                await matchmaker.disconnect(peer)
                await matchmaker.enqueue(peer)

            elif msg_type in ("offer", "answer", "candidate", "chat"):
                if peer.partner:
                    ok = await peer.partner.send({**data, "from": peer.id})
                    if not ok:
                        await matchmaker.partner_unreachable(peer)

            elif msg_type == "typing":
                if peer.partner:
                    await peer.partner.send({"type": "typing"})

            elif msg_type == "ping":
                # Só recebe a mensagem mesmo — mantém a conexão "viva" e evita
                # que proxies/plataformas de hospedagem derrubem WebSockets
                # ociosos por timeout.
                await peer.send({"type": "pong"})

    except WebSocketDisconnect:
        pass
    finally:
        active_peers.discard(peer.id)
        peer_registry.pop(peer.id, None)
        await matchmaker.disconnect(peer)
        logger.info(f"Peer {peer.id} desconectado")


# Serve o frontend estático (index.html = landing, chat.html = sala de chat)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
