# Skill: Channel Safety Checker (Go)

Esta habilidade orienta a IA na verificação rigorosa de operações com canais (`channels`) em Go, que são a espinha dorsal de comunicação e passagem de mensagens no **GigaMQ**.

---

## 🎯 Objetivo de Auditoria

Prevenir pânicos fatais do runtime do Go causados por escritas em canais fechados, fechamentos duplicados de canais ou leitura bloqueante indefinida.

---

## 🔍 Regras de Verificação Tática

Ao analisar o tráfego de canais Go, verifique:

1.  **Regra de Ouro dos Canais:**
    *   *Apenas o remetente (sender) deve fechar o canal.* Nunca feche um canal a partir do receptor (receiver), a menos que haja sincronização estrita e coordenada.
    *   Escrever em um canal fechado (`ch <- msg` onde `ch` está fechado) resulta em um **pânico irrecuperável**.
2.  **Fechamento Único:**
    *   Certifique-se de que nenhum canal seja fechado duas vezes (`close(ch)` duplicado provoca pânico).
    *   Utilize primitivas como `sync.Once` para garantir que o fechamento do canal de término ou do canal de tópicos ocorra apenas uma única vez na desalocação dos recursos da fila.
3.  **Não-Bloqueio Tático (Non-blocking Operations):**
    *   Ao despachar mensagens para consumidores de forma assíncrona, use `select` com cláusula `default` ou buffers eficientes se a perda temporária de mensagens for aceitável no protocolo ou se for necessário implementar políticas de recusa de tráfego (*backpressure*).

---

## 💬 Prompt de Sistema para o Auditor

> "Você é um especialista em padrões de comunicação por canais em Go. Analise o fluxo de dados assíncrono entre Goroutines, verifique a governança do fechamento de canais e confirme se todas as operações de envio estão protegidas de cenários onde o canal destino já foi fechado pelo ciclo de encerramento do servidor."
