FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/receiver ./cmd/receiver
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/storage ./cmd/storage
# Provenance for load-generator runs (docs/loadgen.md scope): .git is not in
# the build context (.dockerignore), so the commit arrives as build args.
# Declared after the receiver build so changing them never rebuilds it.
ARG GIT_COMMIT=unknown
ARG GIT_DIRTY=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X main.gitCommit=${GIT_COMMIT} -X main.gitDirty=${GIT_DIRTY}" \
    -o /out/loadgen ./cmd/loadgen
# The detector stamps its version into every anomaly event (detector.version),
# so an alert can be traced to the code that raised it.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X main.version=${GIT_COMMIT}" -o /out/detector ./cmd/detector
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dashboard ./cmd/dashboard
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/queryapi ./cmd/queryapi

FROM gcr.io/distroless/static-debian12:nonroot AS loadgen
ARG GIT_COMMIT=unknown
LABEL org.opencontainers.image.revision=${GIT_COMMIT}
COPY --from=build /out/loadgen /loadgen
ENTRYPOINT ["/loadgen"]

FROM gcr.io/distroless/static-debian12:nonroot AS storage
COPY --from=build /out/storage /storage
USER nonroot:nonroot
ENTRYPOINT ["/storage"]

FROM gcr.io/distroless/static-debian12:nonroot AS detector
ARG GIT_COMMIT=unknown
LABEL org.opencontainers.image.revision=${GIT_COMMIT}
COPY --from=build /out/detector /detector
USER nonroot:nonroot
ENTRYPOINT ["/detector"]

FROM gcr.io/distroless/static-debian12:nonroot AS dashboard
COPY --from=build /out/dashboard /dashboard
EXPOSE 8088
USER nonroot:nonroot
ENTRYPOINT ["/dashboard"]

FROM gcr.io/distroless/static-debian12:nonroot AS queryapi
COPY --from=build /out/queryapi /queryapi
EXPOSE 8090
USER nonroot:nonroot
ENTRYPOINT ["/queryapi"]

# Last stage = default target, so a plain `docker build .` still gives the receiver.
FROM gcr.io/distroless/static-debian12:nonroot AS receiver
COPY --from=build /out/receiver /receiver
EXPOSE 4317
USER nonroot:nonroot
ENTRYPOINT ["/receiver"]
