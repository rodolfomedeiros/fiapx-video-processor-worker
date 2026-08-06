FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
# O Cargo.toml declara lib e binário: os dois precisam existir para a camada que
# só compila as dependências.
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs && cargo build --release && rm -rf src
COPY src src
RUN cargo build --release
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/fiapx-video-processor-worker /usr/local/bin/worker
EXPOSE 9100
ENTRYPOINT ["worker"]
