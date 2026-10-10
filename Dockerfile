FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/receiver ./cmd/receiver

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/receiver /receiver
EXPOSE 4317
USER nonroot:nonroot
ENTRYPOINT ["/receiver"]
