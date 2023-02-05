FROM rust:slim-buster AS builder

RUN apt update && apt install --yes --no-install-recommends libssl-dev pkg-config

RUN mkdir -p /opt/azure_blob_backup/src
WORKDIR  /opt/azure_blob_backup

RUN cargo install cargo-chef --locked
COPY Cargo.lock .
COPY Cargo.toml .

COPY . /opt/azure_blob_backup
RUN cargo build -r

FROM debian:buster-slim AS runner

RUN apt update && apt install --yes --no-install-recommends libssl-dev cron

COPY --from=builder /opt/azure_blob_backup/target/release/azure_blob_backup /usr/local/bin
COPY crontab /etc/crontab

ENTRYPOINT [ "crond",  "-f" ]