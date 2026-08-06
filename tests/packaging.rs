//! Testes da coleta de quadros e da compactação em ZIP.
use std::{fs, io::Read, path::Path};
use tempfile::tempdir;
use worker::{collect_frames, frame_extraction_args, zip_frames};

fn escreve_quadros(dir: &Path, nomes: &[&str]) {
  for (indice, nome) in nomes.iter().enumerate() {
    fs::write(dir.join(nome), vec![indice as u8; 128]).unwrap();
  }
}

#[test]
fn compacta_todos_os_quadros_preservando_os_nomes() {
  let dir = tempdir().unwrap();
  escreve_quadros(dir.path(), &["frame_0001.png", "frame_0002.png", "frame_0003.png"]);
  let destino = dir.path().join("frames.zip");

  let total = zip_frames(&collect_frames(dir.path()).unwrap(), &destino).unwrap();

  assert_eq!(total, 3);
  let mut arquivo = zip::ZipArchive::new(fs::File::open(&destino).unwrap()).unwrap();
  assert_eq!(arquivo.len(), 3);
  let nomes: Vec<String> = arquivo.file_names().map(str::to_string).collect();
  for esperado in ["frame_0001.png", "frame_0002.png", "frame_0003.png"] {
    assert!(nomes.contains(&esperado.to_string()), "faltou {esperado} no zip");
  }
  let mut conteudo = Vec::new();
  arquivo.by_name("frame_0001.png").unwrap().read_to_end(&mut conteudo).unwrap();
  assert_eq!(conteudo.len(), 128);
}

#[test]
fn quadros_entram_na_ordem_do_video() {
  let dir = tempdir().unwrap();
  escreve_quadros(dir.path(), &["frame_0010.png", "frame_0002.png", "frame_0001.png"]);

  let quadros = collect_frames(dir.path()).unwrap();

  let nomes: Vec<String> = quadros.iter().map(|q| q.file_name().unwrap().to_string_lossy().into_owned()).collect();
  assert_eq!(nomes, ["frame_0001.png", "frame_0002.png", "frame_0010.png"]);
}

#[test]
fn ignora_arquivos_que_nao_sao_quadros() {
  let dir = tempdir().unwrap();
  escreve_quadros(dir.path(), &["frame_0001.png"]);
  fs::write(dir.path().join("input.mp4"), b"nao e um quadro").unwrap();
  fs::write(dir.path().join("log.txt"), b"ruido").unwrap();

  assert_eq!(collect_frames(dir.path()).unwrap().len(), 1);
}

#[test]
fn diretorio_sem_quadros_e_tratado_como_falha() {
  let dir = tempdir().unwrap();

  let erro = collect_frames(dir.path()).unwrap_err();

  assert!(erro.to_string().contains("nenhum frame extraído"), "mensagem inesperada: {erro}");
}

#[test]
fn zip_de_muitos_quadros_mantem_a_contagem() {
  let dir = tempdir().unwrap();
  let nomes: Vec<String> = (1..=250).map(|i| format!("frame_{i:04}.png")).collect();
  escreve_quadros(dir.path(), &nomes.iter().map(String::as_str).collect::<Vec<_>>());
  let destino = dir.path().join("frames.zip");

  assert_eq!(zip_frames(&collect_frames(dir.path()).unwrap(), &destino).unwrap(), 250);
  assert_eq!(zip::ZipArchive::new(fs::File::open(&destino).unwrap()).unwrap().len(), 250);
}

#[test]
fn extracao_pede_um_quadro_por_segundo_ao_ffmpeg() {
  let args = frame_extraction_args(Path::new("/tmp/input"), Path::new("/tmp/frames/frame_%04d.png"));

  assert_eq!(args, ["-i", "/tmp/input", "-vf", "fps=1", "-y", "/tmp/frames/frame_%04d.png"]);
}
