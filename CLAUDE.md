# CLAUDE.md - GigaMQ AI Guide

Este documento estabelece as diretrizes de governança, arquitetura e desenvolvimento para qualquer agente de IA operando no repositório **GigaMQ**. Como Tech Lead deste projeto, exijo conformidade absoluta com as regras abaixo.

---

## 🏗️ Diretrizes de Arquitetura (Go Concurrencia)

O **GigaMQ** é um motor de mensageria concorrente em Go desenvolvido para performance extrema, throughput de mensagens ultra-alto e baixíssima latência.

*   **Padrões de Concorrência:** Uso estruturado de `Goroutines`, `Channels` e primitivas de sincronização do pacote `sync`.
*   **Worker Pools:** Canais de trabalho (`Worker Pools`) para gerenciar e limitar o processamento concorrente de tópicos e filas.
*   **Gerenciamento de Threads:** Foco em manter estruturas thread-safe sem travar o runtime do scheduler do Go.

---

## 🚫 Regras Inquebráveis (Guardrails)

1.  **Proibição de Goroutine Leaks:** Toda goroutine criada deve ter um ciclo de vida estrito e um canal ou contexto (`context.Context`) bem definido para cancelamento e encerramento seguro.
2.  **Thread-Safety Incondicional:** Acesso concorrente a mapas ou buffers de tópicos de mensagens DEVE ser protegido por `sync.Mutex` ou `sync.RWMutex`. Nunca exponha acesso de leitura/escrita simultâneo desprotegido.
3.  **Proibição de Deadlocks:** Sempre libere os Mutexes usando `defer mutex.Unlock()` imediatamente após a aquisição se o fluxo for complexo, garantindo a prevenção contra pânico e travamento.
4.  **No Busy-Waiting:** Loopings vazios aguardando a chegada de mensagens estão proibidos. Use seleções em canais (`select { case <-ch: }`) ou variáveis de condição (`sync.Cond`).

---

## 🛠️ Comandos Frequentes

*   **Build:** `go build -v ./...`
*   **Executar Servidor:** `go run cmd/server/main.go`
*   **Executar Testes:** `go test -v ./...`
*   **Executar Teste de Corrida (Race):** `go test -race ./...` (essencial antes de qualquer commit!)
*   **Formatação:** `go fmt ./...`
*   **Linting:** `golangci-lint run` (se disponível) ou `go vet ./...`
