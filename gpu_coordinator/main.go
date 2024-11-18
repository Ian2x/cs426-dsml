package main

import (
    "log"
    "net"

    pb "../proto"
    "google.golang.org/grpc"
)

func main() {
    // Listen on a designated port
    lis, err := net.Listen("tcp", ":50052") // Change port if needed
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    // Create a new gRPC server
    grpcServer := grpc.NewServer()

    // Register the GPUCoordinator service
    pb.RegisterGPUCoordinatorServer(grpcServer, &gpuCoordinatorServer{})

    log.Printf("GPUCoordinator server listening at %v", lis.Addr())

    // Start serving requests
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
