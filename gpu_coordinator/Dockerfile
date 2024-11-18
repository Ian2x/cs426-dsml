# Use an official Golang runtime as a parent image
FROM golang:1.16

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Build the application
RUN go build -o gpu_coordinator_server .

# Expose port (match the port in main.go)
EXPOSE 50052

# Run the executable
CMD ["./gpu_coordinator_server"]
