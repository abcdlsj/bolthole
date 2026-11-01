FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /bolthole ./cmd/bolthole

FROM gcr.io/distroless/static:nonroot
COPY --from=build /bolthole /bolthole
EXPOSE 8787
ENTRYPOINT ["/bolthole"]
CMD ["serve", "--addr", ":8787"]
