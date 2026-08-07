//! Testes do envelope de eventos e da decisão de retentativa.
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;
use worker::{MAX_ATTEMPTS, Outcome, VideoEvent, backoff, decide, result_key};

fn recebido(attempt: u8) -> VideoEvent {
  VideoEvent {
    event_id: Uuid::new_v4(),
    event_type: "video.received".into(),
    occurred_at: "2026-08-06T10:00:00+00:00".into(),
    video_id: Uuid::new_v4(),
    user_id: Uuid::new_v4(),
    attempt,
    raw_file_path: Some("raw/ana/ferias.mp4".into()),
    zip_file_path: None,
    frame_count: None,
    status: Some("RECEIVED".into()),
    error_message: None,
    user_email: Some("ana@example.com".into()),
  }
}

fn contrato() -> Value {
  serde_json::from_str(include_str!("../contracts/video-event.schema.json")).unwrap()
}

fn campos_obrigatorios() -> Vec<String> {
  contrato()["required"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_string()).collect()
}

#[test]
fn eventos_publicados_trazem_todos_os_campos_obrigatorios_do_contrato() {
  let origem = recebido(0);
  let publicados = [
    origem.processing(),
    origem.completed("processed/ana/pronto.zip".into(), 42),
    origem.errored("ffmpeg falhou"),
    origem.retried(),
    origem.failed("ffmpeg falhou"),
  ];

  for evento in publicados {
    let json: Value = serde_json::from_slice(&serde_json::to_vec(&evento).unwrap()).unwrap();
    for campo in campos_obrigatorios() {
      assert!(json.get(&campo).is_some(), "{} não trouxe {campo}", evento.event_type);
    }
  }
}

#[test]
fn campos_opcionais_vazios_sao_omitidos_em_vez_de_virarem_null() {
  let json: Value = serde_json::from_str(&serde_json::to_string(&recebido(0).processing()).unwrap()).unwrap();

  assert!(json.get("zip_file_path").is_none());
  assert!(json.get("error_message").is_none());
  assert!(json.as_object().unwrap().values().all(|valor| !valor.is_null()));
}

#[test]
fn transicoes_preservam_video_dono_e_email_e_ganham_id_proprio() {
  let origem = recebido(1);
  let processando = origem.processing();

  assert_eq!(processando.video_id, origem.video_id);
  assert_eq!(processando.user_id, origem.user_id);
  assert_eq!(processando.user_email, origem.user_email);
  assert_eq!(processando.status.as_deref(), Some("PROCESSING"));
  assert_eq!(processando.event_type, "video.status.changed");
  assert_ne!(processando.event_id, origem.event_id);
}

#[test]
fn conclusao_carrega_o_zip_e_a_contagem_de_quadros() {
  let concluido = recebido(0).completed("processed/ana/pronto.zip".into(), 42);

  assert_eq!(concluido.status.as_deref(), Some("COMPLETED"));
  assert_eq!(concluido.zip_file_path.as_deref(), Some("processed/ana/pronto.zip"));
  assert_eq!(concluido.frame_count, Some(42));
}

#[test]
fn retentativa_incrementa_o_contador_e_mantem_o_arquivo_original() {
  let retentado = recebido(0).retried();

  assert_eq!(retentado.attempt, 1);
  assert_eq!(retentado.event_type, "video.received");
  assert_eq!(retentado.raw_file_path.as_deref(), Some("raw/ana/ferias.mp4"));
}

#[test]
fn primeiras_falhas_geram_retentativa_com_espera_exponencial() {
  for tentativa in 0..MAX_ATTEMPTS - 1 {
    match decide(&recebido(tentativa), "ffmpeg falhou") {
      Outcome::Retry { event, backoff: espera } => {
        assert_eq!(event.attempt, tentativa + 1);
        assert_eq!(espera, Duration::from_secs(1u64 << tentativa));
      }
      outro => panic!("tentativa {tentativa} deveria retentar, veio {outro:?}"),
    }
  }
}

#[test]
fn ultima_falha_marca_erro_e_emite_video_failed() {
  match decide(&recebido(MAX_ATTEMPTS - 1), "ffmpeg falhou") {
    Outcome::GiveUp { status, failure } => {
      assert_eq!(status.status.as_deref(), Some("ERROR"));
      assert_eq!(status.error_message.as_deref(), Some("ffmpeg falhou"));
      assert_eq!(failure.event_type, "video.failed");
      assert_eq!(failure.error_message.as_deref(), Some("ffmpeg falhou"));
      assert_eq!(
        failure.user_email.as_deref(),
        Some("ana@example.com"),
        "o e-mail é o destino da notificação"
      );
    }
    outro => panic!("deveria desistir, veio {outro:?}"),
  }
}

#[test]
fn backoff_cresce_mas_nao_transborda() {
  assert_eq!(backoff(0), Duration::from_secs(1));
  assert_eq!(backoff(3), Duration::from_secs(8));
  assert_eq!(backoff(200), Duration::from_secs(64));
}

#[test]
fn evento_recebido_do_python_e_desserializado_sem_os_campos_opcionais() {
  let publicado_pelo_python = r#"{
    "event_id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
    "event_type": "video.received",
    "occurred_at": "2026-08-06T10:00:00+00:00",
    "video_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "user_id": "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
    "attempt": 0,
    "raw_file_path": "raw/ana/ferias.mp4",
    "status": "RECEIVED",
    "user_email": "ana@example.com"
  }"#;

  let evento: VideoEvent = serde_json::from_str(publicado_pelo_python).unwrap();

  assert_eq!(evento.raw_file_path.as_deref(), Some("raw/ana/ferias.mp4"));
  assert_eq!(evento.zip_file_path, None);
  assert_eq!(evento.frame_count, None);
}

#[test]
fn zip_fica_sob_o_prefixo_do_dono_do_video() {
  let user = Uuid::parse_str("6ba7b811-9dad-11d1-80b4-00c04fd430c8").unwrap();
  let video = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();

  assert_eq!(
    result_key(&user, &video),
    "processed/6ba7b811-9dad-11d1-80b4-00c04fd430c8/6ba7b810-9dad-11d1-80b4-00c04fd430c8.zip"
  );
}
