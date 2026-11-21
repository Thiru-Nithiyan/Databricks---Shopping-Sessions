# Shopping Sessions: compiled single-pass pipeline (Rust). linux/amd64.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
WORKDIR /app
# DATA_DIR, OUTPUT_DIR and NCPU come from the scorer's environment.
CMD ["/app/pipeline"]
