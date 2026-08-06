# fiapx-video-processor-worker

Worker Rust 1.88 (compatível com o requisito mínimo 1.85+) que consome eventos de vídeo, extrai PNGs com FFmpeg, grava o ZIP no MinIO e publica transições de estado.

## Organização

- `src/lib.rs` — envelope dos eventos, política de retentativa, coleta de quadros e compactação. Sem I/O de rede, então é testável sem RabbitMQ nem MinIO.
- `src/main.rs` — conexão com o broker, acesso ao S3 e laço de consumo.

## Concorrência

Cada mensagem é tratada em uma task própria, limitada pelo `basic_qos`. O padrão é 4
vídeos simultâneos por réplica, ajustável pela variável `PREFETCH`.

## Retentativas

Uma falha de processamento republica o evento em `video.received` com `attempt`
incrementado, esperando 1s, 2s e 4s. Esgotadas as 3 tentativas, o worker marca o vídeo
como `ERROR` e emite `video.failed`, que o notification-service converte em e-mail.

## Testes

```sh
cargo test
```

`tests/events.rs` confere os eventos publicados contra os campos obrigatórios de
`contracts/video-event.schema.json`; `tests/packaging.rs` cobre a coleta de quadros e o ZIP.
Nenhum dos dois precisa de FFmpeg instalado.
