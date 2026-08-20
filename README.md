# sinal — chat aleatório estilo Omegle (texto + vídeo)

## O que tem aqui

- `server.py` — backend FastAPI. Faz o matchmaking e repassa as mensagens de
  sinalização WebRTC (offer/answer/candidate) e o chat de texto entre os dois
  usuários pareados. O matchmaking prioriza país e interesses em comum
  (como uma preferência — se não achar ninguém parecido rápido, pareia com
  quem estiver disponível mesmo assim, pra ninguém ficar esperando pra
  sempre). Também expõe `GET /api/stats` com quantas pessoas estão online.
- `static/index.html` — landing page. Escolha de modo (texto/vídeo), país de
  preferência e interesses, contador de gente online.
- `static/chat.html` — a sala de chat em si. Lê as preferências vindas da
  landing pela URL (`?mode=video&country=BR&interests=games,musica`), pede
  câmera/mic quando o modo é vídeo, conecta no WebSocket e negocia o WebRTC.

O vídeo em si **não passa pelo seu servidor** — depois que os dois lados se
"apresentam" via WebSocket, a chamada de vídeo/áudio vai direto de navegador
pra navegador (peer-to-peer). Isso é bom (menos custo de banda pra você) mas
significa que você precisa de STUN/TURN pra ajudar os dois lados a se
encontrarem atrás de NAT/firewall (mais sobre isso abaixo).

## Rodando localmente

```bash
pip install -r requirements.txt
uvicorn server:app --reload --port 8000
```

Abra `http://localhost:8000` em duas abas (ou dois navegadores) pra testar
os dois lados do chat.

⚠️ Em `localhost` o navegador libera câmera/mic mesmo sem HTTPS. Em produção
isso **não funciona** — ver próximo tópico.

## Colocando no ar de graça (Render + subdomínio grátis)

Esse caminho não custa nada: hospedagem no Render (tier free) + TURN grátis
do Open Relay Project. HTTPS vem de graça junto com o subdomínio do Render,
então nem precisa mexer com Certbot/Nginx.

### 1. Suba o código pro GitHub

```bash
cd sinal-chat
git init
git add .
git commit -m "primeiro commit"
```

Crie um repositório novo no GitHub e faça o push (`git remote add origin ...`
e `git push -u origin main`).

### 2. Crie o serviço no Render

1. Entre em [render.com](https://render.com) e crie uma conta grátis (não pede cartão pro tier free).
2. **New +** → **Web Service** → conecte seu repositório do GitHub.
3. Configuração:
   - **Runtime:** Python 3
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn server:app --host 0.0.0.0 --port $PORT`
   - **Instance Type:** Free
4. Clique em **Create Web Service**.

Em alguns minutos você tem uma URL tipo `https://sinal-chat.onrender.com` —
já com HTTPS, já com WebSocket funcionando, já pronta pra usar a webcam.

O `Procfile` incluído no projeto já tem esse comando de start, então o Render
também detecta automaticamente sem precisar configurar nada manual.

**Sobre o "sono" do tier free:** se ninguém acessa por ~15 minutos, o Render
coloca o serviço pra dormir. O próximo acesso demora uns 30-50s pra acordar
(a pessoa só vê a página carregando). Pra um projeto pessoal/pra mostrar
pros amigos, não é um problema real — só avisa quem for testar que o
primeiro load pode demorar um pouco.

### 2. TURN gratuito já configurado

O `static/index.html` já vem com o TURN público do **Open Relay Project**
(rodado pela Metered) configurado, sem precisar de conta:

```js
{ urls: 'turn:openrelay.metered.ca:443', username: 'openrelayproject', credential: 'openrelayproject' }
```

É uma credencial compartilhada publicamente (todo mundo que usa esse serviço
usa a mesma), então tem cota e pode oscilar em dias de pico. Se o projeto
crescer e isso virar gargalo, é só criar uma conta grátis em
[metered.ca](https://www.metered.ca) (tier free costuma vir com uns 20GB/mês)
e trocar pelas suas próprias credenciais — mesmo formato, só troca usuário/senha.

### 3. Domínio próprio depois (opcional)

Quando quiser deixar de usar `.onrender.com` e usar um domínio seu, no Render
mesmo tem uma aba **Settings → Custom Domain** onde você aponta o DNS do seu
domínio pra lá — o certificado HTTPS é gerado automaticamente, sem custo
extra além do preço do domínio em si.

**Atenção:** matchmaking em memória (como está no `server.py`) só funciona
com **1 instância**. O tier free do Render já roda só 1 instância por padrão,
então tá tudo certo — só não escale pra múltiplas instâncias sem antes mover
a fila de espera pra algo compartilhado (Redis, por exemplo).

### 3. Coisas que faltam pra um produto "de verdade" (não implementadas aqui)

Esse projeto é o esqueleto funcional — pareamento, sinalização, vídeo e chat
funcionam. Pra virar algo public-facing sério, você vai querer adicionar:

- **Moderação/relatório de abuso** — Omegle levou anos de crítica por causa
  disso. Sem alguma forma de moderação, esse tipo de site vira imã de
  conteúdo problemático.
- **Rate limiting / anti-bot** — pra evitar spam na fila de matchmaking.
- **Verificação de idade / termos de uso** — relevante legalmente dependendo
  de onde você hospeda e de quem usa.
- **Filtro de interesses** (o Omegle original deixava digitar tags tipo
  "música", "games" pra parear com gente parecida) — daria pra estender a
  fila do `Matchmaker` pra isso.

Se quiser, posso ajudar a implementar qualquer um desses em seguida.

## Sobre a assinatura premium (planejado, ainda não implementado)

A landing page já tem uma "isca" visual (`em breve: assinatura premium`) mas
nenhum pagamento de verdade acontece ainda. Quando for a hora de implementar,
o caminho mais direto é:

1. **Stripe Checkout** — cria uma sessão de pagamento no backend, redireciona
   o usuário, e o Stripe avisa seu servidor via webhook quando o pagamento
   confirma. Não precisa guardar dado de cartão você mesmo.
2. Guardar em algum banco (Postgres/SQLite/Redis) quem é assinante — o
   `server.py` atual não tem banco de dados nenhum, é tudo em memória.
3. No matchmaking, dar prioridade real (não só preferência) pra quem é
   assinante: por exemplo, deixar a escolha de país ser uma trava garantida
   em vez de só um bônus de score.

Isso é um pedaço de trabalho razoável (autenticação básica de usuário +
integração de pagamento + persistência), então faz sentido como uma etapa
separada depois que o resto estiver estável no ar.
