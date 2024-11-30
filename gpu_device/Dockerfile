FROM golang:1.23

# Set working directory
WORKDIR /app

# Copy the shared go.mod and go.sum
COPY ../go.mod ../go.sum ./

# Download dependencies
RUN go mod download

# Copy the gpu_device-specific files into container
COPY gpu_device/ .
COPY proto/ ./proto

# Build
RUN go build -o gpu_device_server ./server

# Port
EXPOSE 8081

# Run
CMD ["./gpu_device_server"]
