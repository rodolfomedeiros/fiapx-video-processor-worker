use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use chrono::Utc;
use futures_util::StreamExt;
use lapin::{options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use serde::{Deserialize, Serialize};
use std::{env, fs::{self, File}, io::{Read, Write}, process::Command, time::Duration};
use tempfile::tempdir;
use tokio_amqp::*;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VideoEvent { event_id: Uuid, event_type: String, occurred_at: String, video_id: Uuid, user_id: Uuid, attempt: u8, raw_file_path: Option<String>, zip_file_path: Option<String>, frame_count: Option<u32>, status: Option<String>, error_message: Option<String>, user_email: Option<String> }
fn now() -> String { Utc::now().to_rfc3339() }
fn status_event(source: &VideoEvent, state: &str, extra: impl FnOnce(&mut VideoEvent)) -> VideoEvent { let mut event=VideoEvent { event_id:Uuid::new_v4(), event_type:"video.status.changed".into(), occurred_at:now(), video_id:source.video_id, user_id:source.user_id, attempt:source.attempt, raw_file_path:None, zip_file_path:None, frame_count:None, status:Some(state.into()), error_message:None, user_email:source.user_email.clone() }; extra(&mut event); event }
async fn publish(channel: &Channel, key: &str, event: &VideoEvent) -> Result<()> {
  let body = serde_json::to_vec(event)?;
  channel.basic_publish("video.events", key, BasicPublishOptions::default(), &body, BasicProperties::default().with_delivery_mode(2)).await?.await?;
  Ok(())
}
async fn s3_client() -> S3Client { let endpoint=env::var("S3_ENDPOINT_URL").unwrap_or_else(|_|"http://localhost:9000".into()); let creds=Credentials::new(env::var("S3_ACCESS_KEY").unwrap_or_else(|_|"fiapx".into()),env::var("S3_SECRET_KEY").unwrap_or_else(|_|"fiapx-minio-password".into()),None,None,"env"); let shared=aws_config::defaults(BehaviorVersion::latest()).region(Region::new("us-east-1")).credentials_provider(creds).endpoint_url(endpoint).load().await; let config=aws_sdk_s3::config::Builder::from(&shared).force_path_style(true).build(); S3Client::from_conf(config) }
async fn process(s3: &S3Client, event: &VideoEvent) -> Result<(String,u32)> {
  let bucket=env::var("S3_BUCKET").unwrap_or_else(|_|"videos".into()); let key=event.raw_file_path.as_ref().context("evento sem raw_file_path")?;
  let dir=tempdir()?; let input=dir.path().join("input"); let output=dir.path().join("frames"); fs::create_dir(&output)?;
  let object=s3.get_object().bucket(&bucket).key(key).send().await?; let bytes=object.body.collect().await?.into_bytes(); fs::write(&input,bytes)?;
  let frame_pattern=output.join("frame_%04d.png"); let result=Command::new("ffmpeg").args(["-i"]).arg(&input).args(["-vf","fps=1","-y"]).arg(&frame_pattern).output()?;
  if !result.status.success() { anyhow::bail!("ffmpeg falhou: {}",String::from_utf8_lossy(&result.stderr)); }
  let frames: Vec<_>=fs::read_dir(&output)?.filter_map(|e|e.ok()).filter(|e|e.path().extension().is_some_and(|x|x=="png")).collect(); if frames.is_empty(){ anyhow::bail!("nenhum frame extraído"); }
  let zip_path=dir.path().join("frames.zip"); let zip_file=File::create(&zip_path)?; let mut zip=zip::ZipWriter::new(zip_file); let options=zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
  for frame in &frames { let name=frame.file_name().to_string_lossy().to_string(); zip.start_file(name,options)?; let mut file=File::open(frame.path())?; let mut buffer=Vec::new(); file.read_to_end(&mut buffer)?; zip.write_all(&buffer)?; } zip.finish()?;
  let result_key=format!("processed/{}/{}.zip",event.user_id,event.video_id); s3.put_object().bucket(bucket).key(&result_key).body(ByteStream::from_path(&zip_path).await?).content_type("application/zip").send().await?; Ok((result_key,frames.len() as u32))
}
async fn handle(channel: &Channel, s3: &S3Client, event: VideoEvent) -> Result<()> { eprintln!("processing video {} (attempt {})", event.video_id, event.attempt); publish(channel,"video.status.changed",&status_event(&event,"PROCESSING", |_| {})).await?; match process(s3,&event).await { Ok((zip,frames)) => { eprintln!("completed video {}", event.video_id); publish(channel,"video.status.changed",&status_event(&event,"COMPLETED",|e| { e.zip_file_path=Some(zip); e.frame_count=Some(frames); })).await }, Err(_error) if event.attempt < 3 => { eprintln!("retrying video {}", event.video_id); tokio::time::sleep(Duration::from_secs(2u64.pow(event.attempt as u32))).await; let mut retry=event.clone(); retry.event_id=Uuid::new_v4(); retry.occurred_at=now(); retry.attempt+=1; publish(channel,"video.received",&retry).await }, Err(error) => { let message=error.to_string(); eprintln!("failing video {}: {}", event.video_id, message); publish(channel,"video.status.changed",&status_event(&event,"ERROR",|e| e.error_message=Some(message.clone()))).await?; let mut failed=event.clone(); failed.event_id=Uuid::new_v4(); failed.event_type="video.failed".into(); failed.occurred_at=now(); failed.error_message=Some(message); publish(channel,"video.failed",&failed).await } } }
#[tokio::main]
async fn main() -> Result<()> {
  let rabbit = env::var("RABBITMQ_URL").unwrap_or_else(|_| "amqp://fiapx:fiapx@localhost:5672/%2F".into());
  let connection = Connection::connect(&rabbit, ConnectionProperties::default().with_tokio()).await?;
  let channel = connection.create_channel().await?;
  channel.exchange_declare("video.events", ExchangeKind::Topic, ExchangeDeclareOptions { durable: true, ..Default::default() }, FieldTable::default()).await?;
  let queue = channel.queue_declare("video-processing-queue", QueueDeclareOptions { durable: true, ..Default::default() }, FieldTable::default()).await?;
  channel.queue_bind(queue.name().as_str(), "video.events", "video.received", QueueBindOptions::default(), FieldTable::default()).await?;
  channel.basic_qos(4, BasicQosOptions::default()).await?;
  let s3 = s3_client().await;
  let mut consumer = channel.basic_consume(queue.name().as_str(), "fiapx-worker", BasicConsumeOptions::default(), FieldTable::default()).await?;
  while let Some(delivery) = consumer.next().await {
    let delivery = delivery?;
    let event: VideoEvent = serde_json::from_slice(&delivery.data)?;
    match handle(&channel, &s3, event).await {
      Ok(()) => delivery.ack(BasicAckOptions::default()).await?,
      Err(error) => {
        eprintln!("processing error: {error:#}");
        delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await?;
      }
    }
  }
  Ok(())
}
