FROM golang:1.23

# Set working directory
WORKDIR /app

# Copy the current directory contents into container
COPY . .

# Build
RUN go build -o gpu_coordinator_server .

# Port
EXPOSE 8082

# Run
CMD ["./gpu_coordinator_server"]
