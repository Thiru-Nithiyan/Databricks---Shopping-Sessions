# Shopping Sessions: compiled single-pass pipeline (Rust). linux/amd64.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
WORKDIR /app
# Keep freed memory in the allocator: later phases reuse it instead of faulting in fresh pages.
ENV MIMALLOC_PURGE_DELAY=-1
# DATA_DIR and OUTPUT_DIR come from the scorer's environment; NCPU optionally caps the threads.
CMD ["/app/pipeline"]
