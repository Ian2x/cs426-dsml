FROM golang:1.23

# Set working directory
WORKDIR /app

# Copy the current directory contents into container
COPY . .

# Build
RUN go build -o gpu_device_server .

# Port
EXPOSE 8081

# Run
CMD ["./gpu_device_server"]
