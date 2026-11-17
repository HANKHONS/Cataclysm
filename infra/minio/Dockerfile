FROM golang:1.24-bookworm AS build
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
RUN git clone --filter=blob:none https://github.com/minio/minio.git /source
WORKDIR /source
RUN git checkout ${MINIO_COMMIT}
ENV CGO_ENABLED=0 GOMAXPROCS=2
RUN go build -p 2 -trimpath -o /out/minio .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /source/LICENSE /usr/share/doc/minio/LICENSE
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
