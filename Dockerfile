FROM rust:1.94-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
# O Cargo.toml declara lib e binário: os dois precisam existir para a camada que
# só compila as dependências.
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs && cargo build --release && rm -rf src
COPY src src
# Sem o touch, o Cargo às vezes não percebe que main.rs/lib.rs mudaram (mtime do COPY
# não é necessariamente mais recente que o fingerprint do build anterior) e reaproveita
# o binário dummy — que sai imediatamente com código 0, sem log nenhum.
RUN touch src/main.rs src/lib.rs && cargo build --release
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/fiapx-video-processor-worker /usr/local/bin/worker
EXPOSE 9100
ENTRYPOINT ["worker"]
