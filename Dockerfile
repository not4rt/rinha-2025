FROM rust:latest AS builder
WORKDIR /app

# Build dependencies
COPY Cargo.toml Cargo.lock ./
# RUN mkdir src && echo "fn main() {}" > src/backend.rs && echo "fn main() {}" > src/worker.rs
# RUN RUSTFLAGS="-C target-cpu=skylake -C link-arg=-fuse-ld=lld -Z share-generics=y" cargo +nightly build --release
# RUN rm -rf src

# Build application
COPY src ./src
# RUN touch src/backend.rs
RUN RUSTFLAGS="-C target-cpu=skylake -C link-arg=-fuse-ld=lld -Z share-generics=y" cargo +nightly build --release --bin backend

FROM gcr.io/distroless/cc-debian12
WORKDIR /app
COPY --from=builder /app/target/release/backend /app/
CMD ["./backend"]
