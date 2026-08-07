//! Lógica de processamento isolada do transporte, para poder ser testada sem RabbitMQ nem S3.
use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
  fs::{self, File},
  io::{Read, Write},
  path::{Path, PathBuf},
  process::Command,
  time::Duration,
};
use uuid::Uuid;

pub const EXCHANGE: &str = "video.events";
pub const PROCESSING_QUEUE: &str = "video-processing-queue";
pub const RECEIVED_KEY: &str = "video.received";
pub const STATUS_KEY: &str = "video.status.changed";
pub const FAILED_KEY: &str = "video.failed";
/// Número de tentativas antes de desistir e notificar o usuário.
pub const MAX_ATTEMPTS: u8 = 3;

/// Envelope compartilhado com os demais serviços; ver `contracts/video-event.schema.json`.
///
/// Os opcionais são omitidos quando vazios: o contrato declara os campos como `string`,
/// e publicar `null` violaria o schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VideoEvent {
  pub event_id: Uuid,
  pub event_type: String,
  pub occurred_at: String,
  pub video_id: Uuid,
  pub user_id: Uuid,
  pub attempt: u8,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub raw_file_path: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub zip_file_path: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub frame_count: Option<u32>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub status: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub error_message: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub user_email: Option<String>,
}

fn now() -> String {
  Utc::now().to_rfc3339()
}

impl VideoEvent {
  /// Base para qualquer evento derivado deste: novo id, novo instante, mesmo vídeo e dono.
  fn derive(&self, event_type: &str) -> VideoEvent {
    VideoEvent {
      event_id: Uuid::new_v4(),
      event_type: event_type.to_string(),
      occurred_at: now(),
      video_id: self.video_id,
      user_id: self.user_id,
      attempt: self.attempt,
      raw_file_path: None,
      zip_file_path: None,
      frame_count: None,
      status: None,
      error_message: None,
      user_email: self.user_email.clone(),
    }
  }

  pub fn processing(&self) -> VideoEvent {
    VideoEvent { status: Some("PROCESSING".into()), ..self.derive(STATUS_KEY) }
  }

  pub fn completed(&self, zip_file_path: String, frame_count: u32) -> VideoEvent {
    VideoEvent {
      status: Some("COMPLETED".into()),
      zip_file_path: Some(zip_file_path),
      frame_count: Some(frame_count),
      ..self.derive(STATUS_KEY)
    }
  }

  pub fn errored(&self, message: &str) -> VideoEvent {
    VideoEvent {
      status: Some("ERROR".into()),
      error_message: Some(message.to_string()),
      ..self.derive(STATUS_KEY)
    }
  }

  /// Reenfileira o mesmo trabalho com o contador de tentativas incrementado.
  pub fn retried(&self) -> VideoEvent {
    VideoEvent {
      attempt: self.attempt + 1,
      raw_file_path: self.raw_file_path.clone(),
      ..self.derive(RECEIVED_KEY)
    }
  }

  pub fn failed(&self, message: &str) -> VideoEvent {
    VideoEvent {
      raw_file_path: self.raw_file_path.clone(),
      error_message: Some(message.to_string()),
      ..self.derive(FAILED_KEY)
    }
  }
}

/// O que fazer depois de uma falha de processamento.
///
/// As variantes têm tamanhos bem diferentes, mas só são construídas quando um vídeo
/// falha — indireção por Box aqui só adicionaria alocação sem ganho mensurável.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq)]
pub enum Outcome {
  /// Ainda há tentativas: republica em `video.received` após esperar o backoff.
  Retry { event: VideoEvent, backoff: Duration },
  /// Esgotou as tentativas: marca ERROR e emite `video.failed` para o notification-service.
  GiveUp { status: VideoEvent, failure: VideoEvent },
}

/// Espera exponencial entre tentativas: 1s, 2s, 4s.
pub fn backoff(attempt: u8) -> Duration {
  Duration::from_secs(1u64 << attempt.min(6))
}

pub fn decide(event: &VideoEvent, message: &str) -> Outcome {
  if event.attempt + 1 < MAX_ATTEMPTS {
    Outcome::Retry { event: event.retried(), backoff: backoff(event.attempt) }
  } else {
    Outcome::GiveUp { status: event.errored(message), failure: event.failed(message) }
  }
}

pub fn result_key(user_id: &Uuid, video_id: &Uuid) -> String {
  format!("processed/{user_id}/{video_id}.zip")
}

/// Argumentos do FFmpeg para extrair um quadro por segundo.
pub fn frame_extraction_args(input: &Path, pattern: &Path) -> Vec<String> {
  vec![
    "-i".into(),
    input.to_string_lossy().into_owned(),
    "-vf".into(),
    "fps=1".into(),
    "-y".into(),
    pattern.to_string_lossy().into_owned(),
  ]
}

pub fn extract_frames(input: &Path, output_dir: &Path) -> Result<Vec<PathBuf>> {
  let pattern = output_dir.join("frame_%04d.png");
  let result = Command::new("ffmpeg")
    .args(frame_extraction_args(input, &pattern))
    .output()
    .context("não foi possível executar o ffmpeg")?;
  if !result.status.success() {
    anyhow::bail!("ffmpeg falhou: {}", String::from_utf8_lossy(&result.stderr));
  }
  collect_frames(output_dir)
}

/// Lista os PNGs gerados em ordem, para que o ZIP saia com os quadros na sequência do vídeo.
pub fn collect_frames(output_dir: &Path) -> Result<Vec<PathBuf>> {
  let mut frames: Vec<PathBuf> = fs::read_dir(output_dir)?
    .filter_map(|entry| entry.ok())
    .map(|entry| entry.path())
    .filter(|path| path.extension().is_some_and(|extension| extension == "png"))
    .collect();
  frames.sort();
  if frames.is_empty() {
    anyhow::bail!("nenhum frame extraído");
  }
  Ok(frames)
}

/// Compacta os quadros em um único ZIP e devolve quantos entraram.
pub fn zip_frames(frames: &[PathBuf], destination: &Path) -> Result<u32> {
  let mut zip = zip::ZipWriter::new(File::create(destination)?);
  let options = zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
  let mut buffer = Vec::new();
  for frame in frames {
    let name = frame.file_name().context("frame sem nome de arquivo")?.to_string_lossy().into_owned();
    zip.start_file(name, options)?;
    buffer.clear();
    File::open(frame)?.read_to_end(&mut buffer)?;
    zip.write_all(&buffer)?;
  }
  zip.finish()?;
  Ok(frames.len() as u32)
}
