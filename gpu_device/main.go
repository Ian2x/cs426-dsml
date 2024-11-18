package main

import (
    "log"
    "net"

    pb "../proto"
    "google.golang.org/grpc"
)

func main() {
    // Listen on a designated port
    lis, err := net.Listen("tcp", ":8081") // Change port if needed
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    // Create a new gRPC server
    grpcServer := grpc.NewServer()

    // Register the GPUDevice service
    pb.RegisterGPUDeviceServer(grpcServer, &gpuDeviceServer{})

    log.Printf("GPUDevice server listening at %v", lis.Addr())

    // Start serving requests
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
