package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "net"

    sl "github.com/Ian2x/cs426-dsml/gpu_coordinator/server_lib"
    pb "github.com/Ian2x/cs426-dsml/proto"
    "google.golang.org/grpc"
)

var (
    port       = flag.Int("port", 8082, "The server port")
    configFile = flag.String("config_file", "config.json", "Path to config file")
)

func main() {
    flag.Parse()

    // Read config file
    configData, err := ioutil.ReadFile(*configFile)
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

    // Parse json to []DeviceConfig
    var deviceConfigs []sl.DeviceConfig // Initialized with 
    err = json.Unmarshal(configData, &deviceConfigs)
    if err != nil {
        log.Fatalf("Failed to parse config file: %v", err)
    }

    // Initialize the gpuCoordinatorServer
    gpuCoordinatorServer := sl.MakeGPUCoordinatorServer()

    // Process all the deviceConfigs
    for i, deviceConfig := range deviceConfigs {
        rank := uint32(i)
        deviceConfig.MinMemAddr = 0
        deviceConfig.MaxMemAddr = 1024 * 1024 // 1 MB memory

        gpuCoordinatorServer.Devices[deviceConfig.DeviceID] = &deviceConfig
        gpuCoordinatorServer.RankToDeviceID[rank] = deviceConfig.DeviceID
    }

    // Start the gRPC server
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    s := grpc.NewServer()

    pb.RegisterGPUCoordinatorServer(s, gpuCoordinatorServer)

    log.Printf("GPU Coordinator server listening at %v", lis.Addr())

    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
