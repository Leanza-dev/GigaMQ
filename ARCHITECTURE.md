# Engineering at Scale: GigaMQ Architecture

Bem-vindo ao *Architecture Decision Record* (ADR) do GigaMQ. Desenhamos este *Message Broker* com um único objetivo: maximizar a vazão (*Throughput*) minimizando as pausas de coleta de lixo (*Garbage Collection*) sob altíssima pressão.

## 1. Zero-Copy Routing
Em brokers inexperientes, fazer *fan-out* de uma mensagem com um `[]byte` grande para 10.000 clientes copia a struct de memória 10.000 vezes, fritando a RAM e engasgando a CPU com *GC Stop-The-World*.
- **A Solução:** Toda a pipeline de mensageria (Engine -> Topic -> Client) trafega estritamente ponteiros `*domain.Message`. Independentemente do tamanho do payload, o broker aloca e move apenas 8 bytes. A imutabilidade do dado é garantida como uma premissa arquitetural.

## 2. Fan-out Assíncrono e O(1) Engine Unblocking
O roteador central (`Engine`) não pode ficar preso aguardando a iteração de milhares de subscritores durante um *Broadcast*.
- **Defesa Ativa (Implementada):** No método `Broadcast`, realizamos um *Snapshot* quase instantâneo do mapa de subscritores e liberamos o `RWMutex`. O *fan-out* em si (disparos de `.Send()`) é despachado para uma *Worker Goroutine* dedicada em background. A Engine volta a processar a próxima mensagem em tempo O(1), erradicando qualquer *Starvation*.

## 3. Resiliência: Proteção contra Head-of-Line Blocking
Se um consumidor lento estiver com a banda saturada, seu *buffer* interno de rede encherá. Se o broker tentar escrever num *buffer* bloqueado, toda a *Goroutine* trava.
- **A Solução:** As chamadas de envio (`c.outbound <- msg`) operam em canais não-bloqueantes (`select { case ... default: }`). Consumidores lentos terão suas mensagens ejetadas ativamente em favor da sobrevivência do nó e da baixa latência dos consumidores rápidos. Adicionalmente, mitigamos ataques *Slowloris* com *Deadlines* rígidos no TCP (derrubando *Sockets* ociosos para evitar vazamentos de memória (OOM)).
