FROM rust:1 AS builder
WORKDIR /app

# Build dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN RUSTFLAGS="-C target-cpu=skylake" cargo build --release
RUN rm -rf src

# Build application
COPY src ./src
RUN touch src/main.rs
RUN RUSTFLAGS="-C target-cpu=skylake" cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/rust_coroutines_rinha_2025 /app/
EXPOSE 8080
CMD ["./rust_coroutines_rinha_2025"]
