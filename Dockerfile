FROM rust:latest AS builder
WORKDIR /app

RUN apt-get update -yqq && apt-get install -yqq cmake g++

# Build dependencies
COPY ./ ./
RUN cargo clean
RUN RUSTFLAGS="-C target-cpu=skylake -C link-arg=-fuse-ld=lld -Z share-generics=y" cargo +nightly build --release

FROM gcr.io/distroless/cc-debian12
WORKDIR /app
COPY --from=builder /app/target/release/rust_coroutines_rinha_2025 .

EXPOSE 8080
ENTRYPOINT [ "./rust_coroutines_rinha_2025", "--server", "--workers" ]
