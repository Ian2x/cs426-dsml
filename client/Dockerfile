FROM golang:1.23

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

COPY config.json /app/config.json

WORKDIR /app/client

RUN go build -o client .

ENTRYPOINT ["./client", "--config_file=/app/config.json"]