# fiapx-video-processor-worker

O trabalho pesado do **FIAP X**: consome os vídeos enfileirados, extrai um quadro por
segundo com FFmpeg, compacta tudo em um `.zip` e devolve ao object storage, publicando cada
transição de estado no barramento.

**Rust 1.94 (Edition 2024)** · Tokio · lapin · zip · aws-sdk-s3

> A versão mínima do edital é 1.85+; a toolchain fixada acompanha o piso exigido pelas
> dependências da AWS.

## Repositórios do projeto

| Repositório | Linguagem | Papel |
| :--- | :--- | :--- |
| [fiapx-platform](https://github.com/rodolfomedeiros/fiapx-platform) | — | Compose, Kubernetes, contratos, topologia do broker |
| [fiapx-auth-service](https://github.com/rodolfomedeiros/fiapx-auth-service) | Java 25 · Spring Boot 4 | Cadastro, login, emissão e introspecção de JWT |
| [fiapx-video-management-service](https://github.com/rodolfomedeiros/fiapx-video-management-service) | Python 3.13 · FastAPI | Upload, listagem, download e WebSocket de tempo real |
| **fiapx-video-processor-worker** *(você está aqui)* | Rust 1.94 · Tokio | Extração de quadros com FFmpeg e compactação em `.zip` |
| [fiapx-notification-service](https://github.com/rodolfomedeiros/fiapx-notification-service) | Go 1.25 | Consumo da DLQ e envio de e-mail de falha |
| [fiapx-web](https://github.com/rodolfomedeiros/fiapx-web) | React 19 · TypeScript 6 | Interface de upload, acompanhamento e download |

> Para subir o sistema inteiro, use o **fiapx-platform**. Este repositório sozinho precisa
> de RabbitMQ e MinIO acessíveis, além do FFmpeg no `PATH`.

## Onde este serviço entra

```mermaid
flowchart TB
    VMS["video-management-service<br/>Python 3.13"]
    Worker["video-processor-worker<br/>Rust 1.94 · Tokio"]
    Notif["notification-service<br/>Go 1.25"]
    MQ{{"RabbitMQ<br/>video.events"}}
    S3[("MinIO · S3")]
    FF["FFmpeg"]

    VMS -->|"video.received"| MQ
    MQ -->|"video-processing-queue"| Worker
    Worker -->|"video.status.changed<br/>PROCESSING · COMPLETED · ERROR"| MQ
    Worker -->|"video.received<br/>retentativa"| MQ
    Worker -->|"video.failed<br/>tentativas esgotadas"| MQ
    MQ -->|"video-status-queue"| VMS
    MQ -->|"video-processing-dlq"| Notif
    Worker -->|"baixa o original"| S3
    Worker -->|"grava o .zip"| S3
    Worker <--> FF

    classDef atual stroke:#e8590c,stroke-width:4px
    class Worker atual
```

## O que acontece com cada mensagem

```mermaid
flowchart LR
    A["video.received"] --> Z{"ZIP já está<br/>no bucket?"}
    Z -->|"sim (reentrega)"| H
    Z -->|não| B["publica PROCESSING"]
    B --> C["baixa o original<br/>do object storage"]
    C --> D["FFmpeg<br/>-vf fps=1"]
    D --> E{"gerou<br/>quadros?"}
    E -->|sim| F["ordena e compacta<br/>em frames.zip"]
    F --> G["grava em<br/>processed/user/video.zip"]
    G --> H["publica COMPLETED<br/>com frame_count"]
    E -->|não| I{"ainda há<br/>tentativas?"}
    I -->|sim| J["espera 1s · 2s · 4s"]
    J --> K["republica video.received<br/>com attempt + 1"]
    I -->|não| L["publica ERROR"]
    L --> M["publica video.failed"]
```

Os quadros são **ordenados** antes de entrar no `.zip`, para que a sequência do vídeo seja
preservada — `read_dir` não garante ordem alfabética.

## Concorrência

Cada mensagem é tratada em uma **task Tokio própria**, e não em um laço sequencial. É isso
que atende ao requisito de processar mais de um vídeo ao mesmo tempo: com um laço
sequencial, o `sleep` do backoff de uma retentativa travaria a fila inteira.

O paralelismo por réplica é limitado pelo `basic_qos` e configurável em `PREFETCH`
(padrão 4). Horizontalmente, basta subir mais réplicas:

```sh
docker compose up -d --scale video-processor-worker=4
```

No Kubernetes, o HPA cuida disso — com `terminationGracePeriodSeconds: 120`, para que o
vídeo em curso termine antes do pod sair. Quando a janela não basta, a mensagem volta à fila
e a checagem de reentrega abaixo evita refazer o trabalho.

## Retentativas

Uma falha republica o evento em `video.received` com `attempt` incrementado, esperando 1s,
2s e 4s. Esgotadas as 3 tentativas, o worker marca o vídeo como `ERROR` e emite
`video.failed`, que o notification-service converte em e-mail.

Falha ao **publicar** o resultado é tratada à parte: a mensagem vai para a DLQ em vez de
voltar à fila, porque devolvê-la criaria um laço quente contra um broker que já está com
problema — e a DLQ, por si só, já notifica o usuário.

## Reentrega

O RabbitMQ entrega ao menos uma vez. Uma réplica derrubada antes do `ack` — scale-down do
HPA, evicção do pod — devolve a mensagem à fila, e sem cuidado o vídeo passaria pelo FFmpeg
outra vez.

Antes de processar, o worker consulta o bucket pela chave de resultado daquele vídeo. Se o
ZIP já está lá, o trabalho foi concluído e só falta reanunciar: ele publica `COMPLETED` e
confirma a mensagem. O `frame_count` viaja como metadado do objeto, gravado junto ao ZIP,
para não ser preciso reabrir o arquivo só para contar os quadros.

O próprio resultado é a marca de idempotência, o que dispensa uma tabela de eventos
processados. ZIP gravado por uma versão anterior não tem o metadado; nesse caso o vídeo é
reprocessado, que é o desfecho seguro.

## Configuração

| Variável | Padrão | Descrição |
| :--- | :--- | :--- |
| `RABBITMQ_URL` | `amqp://fiapx:fiapx@localhost:5672/%2F` | Barramento |
| `S3_ENDPOINT_URL` | `http://localhost:9000` | Endpoint do object storage |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `fiapx` / `fiapx-minio-password` | Credenciais |
| `S3_BUCKET` | `videos` | Bucket |
| `PREFETCH` | `4` | Vídeos simultâneos por réplica |
| `METRICS_PORT` | `9100` | Porta do endpoint `/metrics` |

O worker espera pelo RabbitMQ no boot (20 tentativas, 3s entre elas), então pode subir
antes do broker sem falhar.

## Organização

| Arquivo | Responsabilidade |
| :--- | :--- |
| `src/lib.rs` | Envelope dos eventos, política de retentativa, coleta de quadros e compactação. **Sem I/O de rede**, então é testável sem RabbitMQ nem MinIO. |
| `src/main.rs` | Conexão com o broker, acesso ao S3, métricas e laço de consumo. |

Essa separação é o que permite cobrir a lógica com testes rápidos e determinísticos.

## Métricas

Servidor HTTP próprio em `:9100/metrics` — sem ele o Prometheus não teria como raspar um
processo que não é um servidor web.

| Métrica | O que mede |
| :--- | :--- |
| `fiapx_worker_videos_total{outcome}` | Vídeos por desfecho: `completed`, `retried`, `failed`, `deduplicated` |
| `fiapx_worker_videos_in_flight` | Vídeos sendo processados agora — o sinal natural para o HPA |
| `fiapx_worker_processing_duration_seconds` | Histograma da duração do processamento |
| `fiapx_worker_frames_extracted_total` | Total de quadros extraídos |

Os buckets do histograma são explícitos: sem isso o exporter publicaria *summaries*, e o
painel de p95 do Grafana não encontraria as séries `_bucket` que `histogram_quantile` pede.

## Executar

```sh
cargo run --release
```

Exige FFmpeg no `PATH`. Com a infraestrutura do Compose de pé, os padrões das variáveis
funcionam sem configuração adicional.

## Testes

```sh
cargo test
```

16 testes: `tests/events.rs` confere os eventos publicados contra os campos obrigatórios de
`contracts/video-event.schema.json` e cobre a política de retentativa; `tests/packaging.rs`
cobre a coleta ordenada de quadros e a compactação. **Nenhum dos dois precisa de FFmpeg
instalado nem de rede.**

O CI ainda roda `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` e um piso de
**80% de cobertura de linhas**:

```sh
cargo llvm-cov --ignore-filename-regex 'src/main\.rs' --fail-under-lines 80
```

`main.rs` fica de fora porque é transporte — RabbitMQ, S3, métricas —, e exercitá-lo pediria
os dois serviços de pé. O piso protege `lib.rs`, onde mora a regra de negócio, hoje em 90%.
É o mesmo recorte que o `gear-up` faz ao excluir `drivers/` do JaCoCo.

O `rustfmt.toml` fixa o estilo do repositório (indentação de 2 espaços, largura de 110): sem
ele, o padrão do rustfmt reformataria o arquivo inteiro.

## Contrato de eventos

`contracts/video-event.schema.json` é uma cópia do contrato canônico mantido em
[fiapx-platform](https://github.com/rodolfomedeiros/fiapx-platform).

Este serviço **consome** `video.received` e **publica** `video.status.changed`,
`video.received` (retentativa) e `video.failed`. Campos opcionais vazios são omitidos da
serialização, porque o schema os declara como `string` e enviar `null` violaria o contrato.
