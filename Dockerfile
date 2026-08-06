FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY src src
RUN cargo build --release
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/fiapx-video-processor-worker /usr/local/bin/worker
ENTRYPOINT ["worker"]
