# GigaMQ 🚀

> [🇺🇸 English](./README.md) | 🇧🇷 Português

📚 **Recursos Técnicos de Engenharia:**
*   [**Especificação da Arquitetura (ARCHITECTURE.md)**](./ARCHITECTURE.md) — Mecanismos de roteamento, lock de concorrência e estratégias de backpressure.
*   [**Governança & Manual da IA (CLAUDE.md)**](./CLAUDE.md) — Regras corporativas, guardrails de desenvolvimento e governança técnica de IA.

**Um Message Queue de alta performance construído em Go — sem brokers externos, latência sub-milissegundo.**

![Go](https://img.shields.io/badge/Linguagem-Go-00ADD8.svg)
![Concorrência](https://img.shields.io/badge/Padrão-Fan--Out-green.svg)
![Latência](https://img.shields.io/badge/Latência-Sub--ms-brightgreen.svg)
![Docker](https://img.shields.io/badge/Infra-Docker--Compose-2496ED.svg)

---

## Visão Geral da Arquitetura

O GigaMQ é um broker pub/sub construído inteiramente sobre as primitivas de concorrência nativas do Go — sem Kafka, sem RabbitMQ, sem dependências externas. Expõe um **protocolo TCP customizado** e implementa um **dispatcher fan-out** respaldado por um pool de workers com channel bufferizado.

```
┌──────────────────────────────────────────────────────┐
│                   SERVIDOR GigaMQ                    │
│                                                      │
│  Clientes TCP                                        │
│  PUB orders → ┌─────────────┐                       │
│               │   Channel   │  bufferizado           │
│               │   Inbound   │ (10.000 capacidade)    │
│               └──────┬──────┘                        │
│                      │  fan-out                      │
│         ┌────────────┼────────────┐                  │
│         ▼            ▼            ▼                  │
│     Worker 1     Worker 2    Worker N                │
│         │            │            │                  │
│         └────────────┼────────────┘                  │
│                      │  roteamento por tópico        │
│               ┌──────▼──────┐                        │
│               │  Mapa de    │  (RWMutex)             │
│               │  Tópicos    │                        │
│               │ ┌─────────┐ │                        │
│               │ │ orders  │→│ Assinante A            │
│               │ │ metrics │→│ Assinantes B, C        │
│               │ └─────────┘ │                        │
│               └─────────────┘                        │
└──────────────────────────────────────────────────────┘
```

### Engine (`internal/queue/engine.go`)

O núcleo do GigaMQ. A struct `Engine` mantém:
- **Channel `Inbound`**: bufferizado, recebe todas as mensagens publicadas.
- **Pool de workers**: N goroutines consomem do `Inbound` concorrentemente.
- **Mapa de tópicos**: `map[string]*Topic` protegido por `sync.RWMutex` com double-checked locking para leituras sem lock.

### Tópico e Fan-Out (`internal/queue/topic.go`)

Cada tópico mantém um `map[string]Subscriber` de conexões ativas. No `Broadcast`, o tópico itera todos os assinantes sob um read lock — zero bloqueio no lado do publicador.

### Protocolo TCP (`internal/protocol/`, `internal/network/`)

Protocolo simples e legível por humanos:

```
PUB <tópico>\n<payload>\n
SUB <tópico>\n
```

O `TCPServer` aceita conexões e despacha comandos para o engine. Cada conexão de assinante é encapsulada em uma struct `Client` que implementa a interface `Subscriber`.

---

## Como Usar

### Rodar com Docker (Recomendado)

```bash
git clone https://github.com/Leanza-dev/GigaMQ.git
cd GigaMQ
docker compose up --build
```

### Rodar Localmente

```bash
go run ./cmd/server/
```

O servidor inicia escutando na **porta 9000**.

### Conectar um Cliente

```bash
# Assinar um tópico
echo -e "SUB orders\n" | nc localhost 9000

# Publicar uma mensagem
echo -e "PUB orders\nhello world\n" | nc localhost 9000
```

---

## Estrutura do Projeto

```
GigaMQ/
├── cmd/server/
│   └── main.go                  # Ponto de entrada — logger, engine, TCP, graceful shutdown
├── internal/
│   ├── domain/
│   │   └── message.go           # Struct Message + interface Subscriber
│   ├── network/
│   │   ├── tcp_server.go        # Listener TCP, handler de conexão, despacho pub/sub
│   │   └── client.go            # Cliente TCP implementando Subscriber
│   ├── protocol/
│   │   └── parser.go            # Parser do protocolo wire (comandos PUB/SUB)
│   └── queue/
│       ├── engine.go            # Dispatcher central: pool de workers + roteamento
│       ├── topic.go             # Broadcaster fan-out com RWMutex
│       └── engine_test.go       # Testes unitários: pub/sub + concorrência
├── go.mod
└── docker-compose.yml
```

---

## Rodando os Testes

```bash
go test ./internal/queue/... -v -race
```

A flag `-race` ativa o detector de condições de corrida do Go — todos os testes passam limpos.

---

## Roadmap

- [ ] Persistência de mensagens (Append-Only File / WAL)
- [ ] Confirmação de mensagens (ACK/NACK)
- [ ] Consumer groups com rastreamento de offset
- [ ] Endpoint de métricas Prometheus

---

*Desenvolvido por Pedro Leanza — Back-End de Alta Performance e Sistemas Distribuídos.*
