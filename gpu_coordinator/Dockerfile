FROM golang:1.23

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

WORKDIR /app/gpu_coordinator

RUN go build -o gpu_coordinator_server ./server

# Port
EXPOSE 8082

# Run
CMD ["./gpu_coordinator_server"]
