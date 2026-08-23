"""
Servidor de sinalização para chat estilo Omegle — com suporte a "1 estranho"
(par de 2, como sempre foi) e "grupo" (salas abertas de até 4 pessoas).

Responsabilidades:
- "1 estranho": casa duas pessoas na hora; se uma sai, a outra volta pra fila
- "grupo": mantém salas ABERTAS de até 4 pessoas — quem pede grupo entra
  direto numa sala com vaga (ou cria uma nova, se não tiver nenhuma aberta);
  se alguém sai, a sala continua rodando pra quem ficou, com uma vaga aberta
  esperando a próxima pessoa
- Repassar mensagens de sinalização WebRTC (offer/answer/candidate) entre
  pares específicos dentro da sala — cada participante se conecta
  diretamente com todos os outros (malha P2P)
- Repassar mensagens de chat de texto pra sala inteira
- Detectar conexões mortas (celular que trava, rede que cai) e liberar quem
  ainda está vivo
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
    """Uma sala de conversa. Par normal = capacidade 2 (fecha e dissolve
    quando alguém sai). Grupo = capacidade 4, fica aberta indefinidamente."""

    def __init__(self, mode: str, capacity: int):
        self.id = str(uuid.uuid4())[:8]
        self.mode = mode
        self.capacity = capacity
        self.peers: list["Peer"] = []

    @property
    def is_group(self) -> bool:
        return self.capacity > 2

    def has_room(self) -> bool:
        return len(self.peers) < self.capacity


class Peer:
    def __init__(self, ws: WebSocket):
        self.id = str(uuid.uuid4())[:8]
        self.ws = ws
        self.room: Optional[Room] = None
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


def match_score(a: Peer, b: Peer) -> int:
    """Quanto maior, mais compatível (país/interesses em comum)."""
    score = 0
    if a.country != "any" and a.country == b.country:
        score += 5
    score += len(a.tags & b.tags)
    return score


class Matchmaker:
    """Cuida do pareamento normal (par de 2) e das salas de grupo (até 4,
    sempre abertas pra quem quiser entrar)."""

    def __init__(self):
        self.waiting: list[Peer] = []          # fila de quem quer "1 estranho"
        self.group_rooms: dict[str, list[Room]] = {}  # modo -> salas de grupo ativas
        self.lock = asyncio.Lock()

    async def enqueue(self, peer: Peer):
        if peer.group_size <= 2:
            await self._enqueue_pair(peer)
        else:
            await self._enqueue_group(peer)

    async def _enqueue_pair(self, peer: Peer):
        async with self.lock:
            compatible = [p for p in self.waiting if p.mode == peer.mode and p.group_size <= 2]
            if compatible:
                partner = max(compatible, key=lambda c: match_score(peer, c))
                self.waiting.remove(partner)
                room = Room(peer.mode, capacity=2)
                room.peers = [peer, partner]
                for p in room.peers:
                    p.room = room
                for p in room.peers:
                    others = [o.id for o in room.peers if o.id != p.id]
                    await p.send({"type": "room-ready", "you": p.id, "peers": others})
                logger.info(f"Par formado: {[p.id for p in room.peers]}")
            else:
                self.waiting.append(peer)
                await peer.send({"type": "waiting"})

    async def _enqueue_group(self, peer: Peer):
        async with self.lock:
            rooms = self.group_rooms.setdefault(peer.mode, [])
            target = self._pick_open_room(peer, rooms)

            if target is None:
                target = Room(peer.mode, capacity=4)
                rooms.append(target)

            existing = list(target.peers)
            target.peers.append(peer)
            peer.room = target

            # Avisa o recém-chegado quem já está na sala (pra ele iniciar a
            # conexão WebRTC com cada um).
            await peer.send({"type": "room-ready", "you": peer.id, "peers": [p.id for p in existing]})

            # Avisa quem já estava lá que uma pessoa nova chegou — eles não
            # precisam fazer nada agora, só vão receber uma oferta de conexão
            # dessa pessoa em seguida.
            for p in existing:
                await p.send({"type": "peer-joined", "peer_id": peer.id})

            logger.info(f"{peer.id} entrou na sala {target.id} ({len(target.peers)}/{target.capacity})")

    def _pick_open_room(self, peer: Peer, rooms: list[Room]) -> Optional[Room]:
        open_rooms = [r for r in rooms if r.has_room()]
        if not open_rooms:
            return None
        if len(open_rooms) == 1:
            return open_rooms[0]

        def room_score(r: Room) -> float:
            if not r.peers:
                return 0
            return sum(match_score(peer, p) for p in r.peers) / len(r.peers)

        return max(open_rooms, key=room_score)

    async def leave_queue(self, peer: Peer):
        async with self.lock:
            if peer in self.waiting:
                self.waiting.remove(peer)

    async def leave_room(self, peer: Peer):
        """Peer saiu de uma sala (desconectou, deu erro, ou clicou
        'próximo'). Par normal: dissolve e devolve quem sobrou pra fila.
        Grupo: só tira essa pessoa — o resto continua rodando normalmente,
        com uma vaga aberta pra próxima pessoa que pedir grupo."""
        room = peer.room
        peer.room = None
        if not room:
            return

        room.peers = [p for p in room.peers if p.id != peer.id]

        if room.is_group:
            for p in room.peers:
                await p.send({"type": "peer-left", "peer_id": peer.id})
            if not room.peers:
                rooms = self.group_rooms.get(room.mode, [])
                if room in rooms:
                    rooms.remove(room)
        else:
            for p in room.peers:
                p.room = None
                await p.send({"type": "partner-left", "peer_id": peer.id})
            for p in room.peers:
                await self.enqueue(p)

    async def disconnect(self, peer: Peer):
        """Chamado quando o peer sai ou fecha a conexão."""
        await self.leave_queue(peer)
        await self.leave_room(peer)


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
                # Sai da sala/fila atual e tenta de novo, mantendo as mesmas preferências
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
                        await matchmaker.leave_room(peer)

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
