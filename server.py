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

    async def send(self, payload: dict):
        try:
            await self.ws.send_text(json.dumps(payload))
        except Exception:
            pass

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
            if self.waiting:
                # Escolhe quem tem mais em comum com o peer que chegou.
                # Se ninguém tiver nada em comum, casa com o primeiro da fila
                # mesmo assim — preferência, não trava.
                best_partner = None
                best_score = -1
                best_shared: list[str] = []
                for candidate in self.waiting:
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


@app.get("/api/stats")
async def stats():
    return {"online": len(active_peers)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    peer = Peer(websocket)
    active_peers.add(peer.id)

    try:
        # Primeira mensagem esperada do cliente: as preferências de matchmaking.
        raw = await websocket.receive_text()
        data = json.loads(raw)
        if data.get("type") == "join":
            peer.set_preferences(data.get("mode"), data.get("country"), data.get("interests", []))

        await matchmaker.enqueue(peer)

        while True:
            raw = await websocket.receive_text()
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
                    await peer.partner.send({**data, "from": peer.id})

            elif msg_type == "typing":
                if peer.partner:
                    await peer.partner.send({"type": "typing"})

    except WebSocketDisconnect:
        pass
    finally:
        active_peers.discard(peer.id)
        await matchmaker.disconnect(peer)
        logger.info(f"Peer {peer.id} desconectado")


# Serve o frontend estático (index.html = landing, chat.html = sala de chat)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
