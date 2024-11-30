FROM golang:1.23

# Set working directory
WORKDIR /app

# Copy the shared go.mod and go.sum
COPY ../go.mod ../go.sum ./

# Download dependencies
RUN go mod download

# Copy the gpu_coordinator-specific files into container
COPY gpu_coordinator/ .
COPY proto/ ./proto

# Build
RUN go build -o gpu_coordinator_server .

# Port
EXPOSE 8082

# Run
CMD ["./gpu_coordinator_server"]
