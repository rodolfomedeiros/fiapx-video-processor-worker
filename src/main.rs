//! Consome `video.received`, extrai os quadros com FFmpeg e devolve o ZIP ao object storage.
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, config::Region, primitives::ByteStream};
use futures_util::StreamExt;
use lapin::{
  BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
  options::*,
  types::{AMQPValue, FieldTable},
};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::{
  env, fs,
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::{Duration, Instant},
};
use tempfile::tempdir;
use worker::{
  EXCHANGE, FAILED_KEY, Outcome, PROCESSING_QUEUE, RECEIVED_KEY, VideoEvent, decide, extract_frames,
  result_key, zip_frames,
};

fn bucket() -> String {
  env::var("S3_BUCKET").unwrap_or_else(|_| "videos".into())
}

/// Total de quadros gravado junto ao ZIP, para que uma reentrega possa reanunciar o
/// resultado sem reabrir o arquivo.
const FRAME_COUNT_METADATA: &str = "frame-count";

/// Resultado já presente no bucket, quando a mensagem é a reentrega de um vídeo que o
/// worker concluiu mas não chegou a confirmar. Ausente o metadado — ZIP gravado por uma
/// versão anterior —, o vídeo é reprocessado, que é o desfecho seguro.
async fn existing_result(s3: &S3Client, event: &VideoEvent) -> Option<(String, u32)> {
  let destination = result_key(&event.user_id, &event.video_id);
  let head = s3.head_object().bucket(bucket()).key(&destination).send().await.ok()?;
  let frame_count = head.metadata()?.get(FRAME_COUNT_METADATA)?.parse().ok()?;
  Some((destination, frame_count))
}

async fn publish(channel: &Channel, routing_key: &str, event: &VideoEvent) -> Result<()> {
  let body = serde_json::to_vec(event)?;
  channel
    .basic_publish(
      EXCHANGE,
      routing_key,
      BasicPublishOptions::default(),
      &body,
      BasicProperties::default().with_delivery_mode(2).with_content_type("application/json".into()),
    )
    .await?
    .await?;
  Ok(())
}

async fn s3_client() -> S3Client {
  let endpoint = env::var("S3_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:9000".into());
  let credentials = Credentials::new(
    env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "fiapx".into()),
    env::var("S3_SECRET_KEY").unwrap_or_else(|_| "fiapx-minio-password".into()),
    None,
    None,
    "env",
  );
  let shared = aws_config::defaults(BehaviorVersion::latest())
    .region(Region::new("us-east-1"))
    .credentials_provider(credentials)
    .endpoint_url(endpoint)
    .load()
    .await;
  S3Client::from_conf(aws_sdk_s3::config::Builder::from(&shared).force_path_style(true).build())
}

/// Baixa o original, extrai os quadros, compacta e devolve a chave do ZIP e o total de quadros.
async fn process(s3: &S3Client, event: &VideoEvent) -> Result<(String, u32)> {
  let bucket = bucket();
  let key = event.raw_file_path.as_ref().context("evento sem raw_file_path")?;

  let directory = tempdir()?;
  let input = directory.path().join("input");
  let frames_dir = directory.path().join("frames");
  fs::create_dir(&frames_dir)?;

  let object = s3.get_object().bucket(&bucket).key(key).send().await?;
  fs::write(&input, object.body.collect().await?.into_bytes())?;

  let frames = extract_frames(&input, &frames_dir)?;
  let zip_path = directory.path().join("frames.zip");
  let frame_count = zip_frames(&frames, &zip_path)?;

  let destination = result_key(&event.user_id, &event.video_id);
  s3.put_object()
    .bucket(bucket)
    .key(&destination)
    .body(ByteStream::from_path(&zip_path).await?)
    .content_type("application/zip")
    .metadata(FRAME_COUNT_METADATA, frame_count.to_string())
    .send()
    .await?;
  Ok((destination, frame_count))
}

async fn handle(channel: &Channel, s3: &S3Client, event: VideoEvent) -> Result<()> {
  // O RabbitMQ entrega ao menos uma vez: um worker derrubado antes do ack — scale-down do
  // HPA, evicção do pod — faz a mensagem voltar para a fila. Refazer a extração custaria
  // outro FFmpeg inteiro, então quando o ZIP já está no bucket só falta reanunciar.
  if let Some((zip_path, frame_count)) = existing_result(s3, &event).await {
    eprintln!("vídeo {} já estava processado, reanunciando o resultado", event.video_id);
    counter!("fiapx_worker_videos_total", "outcome" => "deduplicated").increment(1);
    return publish(channel, worker::STATUS_KEY, &event.completed(zip_path, frame_count)).await;
  }

  eprintln!("processando vídeo {} (tentativa {})", event.video_id, event.attempt);
  publish(channel, worker::STATUS_KEY, &event.processing()).await?;

  gauge!("fiapx_worker_videos_in_flight").increment(1.0);
  let started = Instant::now();
  let result = process(s3, &event).await;
  gauge!("fiapx_worker_videos_in_flight").decrement(1.0);
  histogram!("fiapx_worker_processing_duration_seconds").record(started.elapsed().as_secs_f64());

  match result {
    Ok((zip_path, frame_count)) => {
      eprintln!("vídeo {} concluído com {frame_count} quadros", event.video_id);
      counter!("fiapx_worker_videos_total", "outcome" => "completed").increment(1);
      counter!("fiapx_worker_frames_extracted_total").increment(u64::from(frame_count));
      publish(channel, worker::STATUS_KEY, &event.completed(zip_path, frame_count)).await
    }
    Err(error) => {
      let message = format!("{error:#}");
      match decide(&event, &message) {
        Outcome::Retry { event: retry, backoff } => {
          eprintln!("vídeo {} falhou ({message}), retentando em {:?}", event.video_id, backoff);
          counter!("fiapx_worker_videos_total", "outcome" => "retried").increment(1);
          tokio::time::sleep(backoff).await;
          publish(channel, RECEIVED_KEY, &retry).await
        }
        Outcome::GiveUp { status, failure } => {
          eprintln!("vídeo {} esgotou as tentativas: {message}", event.video_id);
          counter!("fiapx_worker_videos_total", "outcome" => "failed").increment(1);
          publish(channel, worker::STATUS_KEY, &status).await?;
          publish(channel, FAILED_KEY, &failure).await
        }
      }
    }
  }
}

/// Declara a fila com os mesmos argumentos de `fiapx-platform/rabbitmq/definitions.json`.
/// Divergir daqui faz o broker responder PRECONDITION_FAILED e derruba o worker no boot.
fn processing_queue_arguments() -> FieldTable {
  let mut arguments = FieldTable::default();
  arguments.insert("x-dead-letter-exchange".into(), AMQPValue::LongString(EXCHANGE.into()));
  arguments.insert("x-dead-letter-routing-key".into(), AMQPValue::LongString(FAILED_KEY.into()));
  arguments
}

fn connection_properties() -> ConnectionProperties {
  ConnectionProperties::default()
    .with_executor(tokio_executor_trait::Tokio::current())
    .with_reactor(tokio_reactor_trait::Tokio)
}

async fn connect(url: &str) -> Result<Connection> {
  let mut last_error = None;
  for attempt in 1..=20 {
    match Connection::connect(url, connection_properties()).await {
      Ok(connection) => return Ok(connection),
      Err(error) => {
        eprintln!("RabbitMQ indisponível (tentativa {attempt}/20): {error}");
        last_error = Some(error);
        tokio::time::sleep(Duration::from_secs(3)).await;
      }
    }
  }
  Err(anyhow::anyhow!("não foi possível conectar ao RabbitMQ")).context(last_error.unwrap())
}

/// Sobe o endpoint /metrics; sem servidor HTTP próprio o Prometheus não teria como raspar o worker.
fn install_metrics() -> Result<()> {
  let port: u16 = env::var("METRICS_PORT").ok().and_then(|value| value.parse().ok()).unwrap_or(9100);
  // Sem buckets explícitos o exporter publica summaries, e o dashboard não teria as
  // séries _bucket de que histogram_quantile precisa.
  const BUCKETS: [f64; 9] = [0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0];
  PrometheusBuilder::new()
    .set_buckets(&BUCKETS)
    .context("buckets inválidos")?
    .with_http_listener(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
    .install()
    .context("não foi possível expor as métricas")?;
  eprintln!("métricas em :{port}/metrics");
  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
  install_metrics()?;
  let url = env::var("RABBITMQ_URL").unwrap_or_else(|_| "amqp://fiapx:fiapx@localhost:5672/%2F".into());
  let connection = connect(&url).await?;
  let channel = connection.create_channel().await?;

  channel
    .exchange_declare(
      EXCHANGE,
      ExchangeKind::Topic,
      ExchangeDeclareOptions { durable: true, ..Default::default() },
      FieldTable::default(),
    )
    .await?;
  channel
    .queue_declare(
      PROCESSING_QUEUE,
      QueueDeclareOptions { durable: true, ..Default::default() },
      processing_queue_arguments(),
    )
    .await?;
  channel
    .queue_bind(PROCESSING_QUEUE, EXCHANGE, RECEIVED_KEY, QueueBindOptions::default(), FieldTable::default())
    .await?;

  let prefetch: u16 = env::var("PREFETCH").ok().and_then(|value| value.parse().ok()).unwrap_or(4);
  channel.basic_qos(prefetch, BasicQosOptions::default()).await?;

  let s3 = s3_client().await;
  let mut consumer = channel
    .basic_consume(PROCESSING_QUEUE, "fiapx-worker", BasicConsumeOptions::default(), FieldTable::default())
    .await?;
  eprintln!("worker pronto, processando até {prefetch} vídeos simultâneos");

  while let Some(delivery) = consumer.next().await {
    let delivery = delivery?;
    let (channel, s3) = (channel.clone(), s3.clone());
    // Uma task por mensagem: o laço sequencial anterior processava um vídeo de cada vez,
    // e o sleep do backoff bloqueava a fila inteira.
    tokio::spawn(async move {
      let event: VideoEvent = match serde_json::from_slice(&delivery.data) {
        Ok(event) => event,
        Err(error) => {
          eprintln!("evento ilegível, descartando: {error}");
          let _ = delivery.reject(BasicRejectOptions { requeue: false }).await;
          return;
        }
      };
      match handle(&channel, &s3, event).await {
        Ok(()) => {
          let _ = delivery.ack(BasicAckOptions::default()).await;
        }
        Err(error) => {
          // Só chega aqui se a própria publicação falhou. Devolver à fila criaria um laço
          // quente, então a mensagem vai para a DLQ e o usuário é avisado por e-mail.
          eprintln!("falha ao publicar o resultado: {error:#}");
          let _ = delivery.reject(BasicRejectOptions { requeue: false }).await;
        }
      }
    });
  }
  Ok(())
}
