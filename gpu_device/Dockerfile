FROM golang:1.23

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

COPY config.json /app/config.json

WORKDIR /app/gpu_device

RUN go build -o gpu_device_server ./server

# Expose port -> change to command-line arg
# EXPOSE 8081

# Run
CMD ["./gpu_device_server"]
