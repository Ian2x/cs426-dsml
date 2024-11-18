package main

import (
    "flag"
    "log"
    "net"

    pb "../proto"
    "google.golang.org/grpc"
)

var (
	port        = flag.Int("port", 8082, "The server port")
)

func main() {
    flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

    s := grpc.NewServer()

    pb.RegisterGPUCoordinatorServer(s, &gpuCoordinatorServer{

    })

    log.Printf("GPUCoordinator server listening at %v", lis.Addr())

    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}
