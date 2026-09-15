# EXP-02 构建镜像：多阶段构建 + 非 root 运行
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/order-api ./cmd/order-api

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/order-api /order-api
EXPOSE 8080
ENTRYPOINT ["/order-api"]
