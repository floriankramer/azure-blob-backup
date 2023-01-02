FROM rust:alpine3.16 AS builder

RUN apk add --no-cache openssl-dev pkgconfig musl-dev

COPY . /opt/azure_blob_backup
WORKDIR /opt/azure_blob_backup
RUN cargo build -r

FROM alpine:3.16 AS runner

RUN apk add openssl

COPY --from=builder /opt/azure_blob_backup/target/release/azure_blob_backup /usr/local/bin
COPY crontab /etc/crontab

ENTRYPOINT [ "crond",  "-f" ]