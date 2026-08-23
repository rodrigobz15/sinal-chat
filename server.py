"""
Servidor de sinalização para chat estilo Omegle — agora com suporte a salas
de 2 a 4 pessoas (não só pares).

Responsabilidades:
- Agrupar usuários em salas do tamanho pedido (2, 3 ou 4), priorizando
  país/interesses em comum quando possível (preferência, não filtro rígido)
- Repassar mensagens de sinalização WebRTC (offer/answer/candidate) entre
  pares específicos dentro da sala — cada participante se conecta
  diretamente com todos os outros (malha P2P)
- Repassar mensagens de chat de texto pra sala inteira
- Tratar "next" (pular) e desconexões — se alguém sai, a sala inteira se
  desfaz e o resto volta pra fila
- Expor um contador simples de quantas pessoas estão online agora

O vídeo/áudio em si NÃO passa por este servidor: depois que o WebRTC é
negociado, a mídia trafega direto entre os navegadores (P2P). Este servidor
só ajuda todo mundo a se "apresentar".
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


class Room:
    """Um grupo de 2 a 4 peers conversando entre si."""

    def __init__(self, mode: str, size: int = 0):
        self.id = str(uuid.uuid4())[:8]
        self.mode = mode
        self.size = size
        self.peers: list["Peer"] = []


class Peer:
    def __init__(self, ws: WebSocket):
        self.id = str(uuid.uuid4())[:8]
        self.ws = ws
        self.room: Optional[Room] = None
        # Sala "em formação" — usada só enquanto o servidor ainda está
        # esperando mais gente entrar num grupo (ver GROUP_COLLECT_SECONDS).
        self.pending_room: Optional[Room] = None
        self.mode = "video"
        self.country = "any"
        self.tags: set[str] = set()
        self.group_size = 2
        self.last_seen = time.monotonic()

    async def send(self, payload: dict) -> bool:
        try:
            await self.ws.send_text(json.dumps(payload))
            return True
        except Exception:
            return False

    def set_preferences(self, mode: str, country: str, interests: list[str], group_size):
        self.mode = mode if mode in ("video", "text") else "video"
        self.country = (country or "any").upper() if country != "any" else "any"
        self.tags = {t.strip().lower() for t in interests if t.strip()}
        try:
            size = int(group_size)
        except (TypeError, ValueError):
            size = 2
        self.group_size = max(2, min(4, size))


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


GROUP_COLLECT_SECONDS = 5  # quanto tempo esperar juntando gente pro grupo antes de fechar com quem tiver


class Matchmaker:
    """Mantém a fila de espera e forma as salas.

    "1 estranho" (group_size <= 2) continua imediato, como sempre foi: casa
    assim que aparecer outra pessoa compatível.

    "grupo" (group_size > 2) é diferente: em vez de casar na hora com quem
    estiver disponível (o que na prática quase sempre formaria só duplas,
    já que dificilmente 4 pessoas chegam no exato mesmo instante), o
    servidor junta todo mundo que pediu grupo numa "sala pendente" e espera
    alguns segundos coletando mais gente antes de fechar — ou fecha na hora
    se já bater o teto de 4.
    """

    def __init__(self):
        self.waiting: list[Peer] = []
        self.lock = asyncio.Lock()
        self.pending_group: dict[str, Room] = {}  # mode -> sala em formação
        self.pending_timers: dict[str, asyncio.Task] = {}

    async def enqueue(self, peer: Peer):
        if peer.group_size <= 2:
            await self._enqueue_pair(peer)
        else:
            await self._enqueue_group(peer)

    async def _enqueue_pair(self, peer: Peer):
        async with self.lock:
            compatible = [p for p in self.waiting if p.mode == peer.mode and p.group_size <= 2]
            if compatible:
                partner = max(compatible, key=lambda c: match_score(peer, c)[0])
                self.waiting.remove(partner)
                room = Room(peer.mode, 2)
                room.peers = [peer, partner]
                for p in room.peers:
                    p.room = room
                for p in room.peers:
                    others = [o.id for o in room.peers if o.id != p.id]
                    await p.send({"type": "room-ready", "you": p.id, "peers": others})
                logger.info(f"Sala formada (2 pessoas): {[p.id for p in room.peers]}")
            else:
                self.waiting.append(peer)
                await peer.send({"type": "waiting"})

    async def _enqueue_group(self, peer: Peer):
        async with self.lock:
            pending = self.pending_group.get(peer.mode)
            if pending is None:
                pending = Room(peer.mode)
                pending.peers = [peer]
                peer.pending_room = pending
                self.pending_group[peer.mode] = pending
                self.pending_timers[peer.mode] = asyncio.create_task(
                    self._finalize_after_delay(peer.mode, pending)
                )
                await peer.send({"type": "waiting"})
            else:
                pending.peers.append(peer)
                peer.pending_room = pending
                if len(pending.peers) >= 4:
                    timer = self.pending_timers.pop(peer.mode, None)
                    if timer:
                        timer.cancel()
                    self.pending_group.pop(peer.mode, None)
                    await self._finalize_room(pending)
                else:
                    await peer.send({"type": "waiting"})

    async def _finalize_after_delay(self, mode: str, room: Room):
        try:
            await asyncio.sleep(GROUP_COLLECT_SECONDS)
        except asyncio.CancelledError:
            return

        lone_peer = None
        should_finalize = False
        async with self.lock:
            if self.pending_group.get(mode) is room:
                self.pending_group.pop(mode, None)
                self.pending_timers.pop(mode, None)
                if len(room.peers) >= 2:
                    should_finalize = True
                elif room.peers:
                    lone_peer = room.peers[0]
                    lone_peer.pending_room = None

        if should_finalize:
            await self._finalize_room(room)
        elif lone_peer:
            # Ninguém mais apareceu no prazo — devolve essa pessoa pra
            # tentativa normal (o watchdog do cliente vai reenviar 'next'
            # daqui a pouco, ou alguém novo pode chegar e casar na hora).
            await self.enqueue(lone_peer)

    async def _finalize_room(self, room: Room):
        room.size = len(room.peers)
        for p in room.peers:
            p.room = room
            p.pending_room = None
        for p in room.peers:
            others = [o.id for o in room.peers if o.id != p.id]
            await p.send({"type": "room-ready", "you": p.id, "peers": others})
        logger.info(f"Sala formada ({room.size} pessoas): {[p.id for p in room.peers]}")

    async def leave_queue(self, peer: Peer):
        async with self.lock:
            if peer in self.waiting:
                self.waiting.remove(peer)

            pending = peer.pending_room
            if pending:
                peer.pending_room = None
                if peer in pending.peers:
                    pending.peers.remove(peer)
                if not pending.peers and self.pending_group.get(pending.mode) is pending:
                    self.pending_group.pop(pending.mode, None)
                    timer = self.pending_timers.pop(pending.mode, None)
                    if timer:
                        timer.cancel()

    async def dissolve_room(self, peer: Peer):
        """Peer saiu de uma sala ativa (desconectou, deu erro, ou clicou
        'próximo') — avisa o resto da sala e devolve os sobreviventes pra
        fila, mantendo as preferências de cada um."""
        room = peer.room
        peer.room = None
        if not room:
            return

        survivors = [p for p in room.peers if p.id != peer.id]
        for p in survivors:
            p.room = None
            await p.send({"type": "partner-left", "peer_id": peer.id})

        for p in survivors:
            await self.enqueue(p)

    async def disconnect(self, peer: Peer):
        """Chamado quando o peer sai ou fecha a conexão."""
        await self.leave_queue(peer)
        await self.dissolve_room(peer)


matchmaker = Matchmaker()

# Todo mundo com uma conexão WebSocket aberta agora (em sala ou esperando),
# só pra alimentar o contador de "online" na landing page.
active_peers: set[str] = set()

# Registro de todos os peers vivos, pra "tarefa de vigia" conseguir checar
# quem parou de dar sinal de vida (celular que travou, app em segundo plano,
# rede que caiu sem avisar o servidor), e pra rotear mensagens de sinalização
# WebRTC direcionadas a um peer específico dentro de uma sala.
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
            peer.set_preferences(
                data.get("mode"), data.get("country"), data.get("interests", []), data.get("group_size", 2)
            )

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
                # Pula pra outra sala, mantendo as mesmas preferências
                await matchmaker.disconnect(peer)
                await matchmaker.enqueue(peer)

            elif msg_type in ("offer", "answer", "candidate"):
                # Sinalização WebRTC é sempre direcionada a um peer específico
                # dentro da sala (cada par se conecta diretamente).
                target_id = data.get("to")
                target = peer_registry.get(target_id)
                if target and peer.room and target in peer.room.peers:
                    ok = await target.send({**data, "from": peer.id})
                    if not ok:
                        await matchmaker.dissolve_room(peer)

            elif msg_type == "chat":
                # Chat de texto vai pra sala inteira
                if peer.room:
                    for p in peer.room.peers:
                        if p.id != peer.id:
                            await p.send({**data, "from": peer.id})

            elif msg_type == "typing":
                if peer.room:
                    for p in peer.room.peers:
                        if p.id != peer.id:
                            await p.send({"type": "typing"})

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
