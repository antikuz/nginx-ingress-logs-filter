FROM golang:1.18.3-alpine3.16 AS builder

WORKDIR /build

COPY . ./
RUN go mod download \
 && go build -o nginx-ingress-logs-filter


FROM alpine:3.16

WORKDIR /app

COPY --from=builder /build/nginx-ingress-logs-filter ./nginx-ingress-logs-filter

ENTRYPOINT ["/app/nginx-ingress-logs-filter"]