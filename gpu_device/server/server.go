package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "net"

	sl "github.com/Ian2x/cs426-dsml/gpu_device/server_lib"
    pb "github.com/Ian2x/cs426-dsml/proto"
    utl "github.com/Ian2x/cs426-dsml/util"
    "google.golang.org/grpc"
)

/*
./gpu_device --device_id=<device_id>
*/
var (
	deviceID			= flag.Int("device_id", 0, "Unique device ID")
	configFile		= flag.String("config_file", "config.json", "Path to config file")
)

type Config struct {
    Coordinator utl.CoordinatorConfig `json:"coordinator"`
    Devices []utl.DeviceConfig `json:"devices"`
}

func main() {
    flag.Parse()

    if *deviceID == 0 {
        log.Fatalf("Usage: gpu_device --device_id=<device_id> [--config_file=<config_file>]")
    }

    // Read config file
    configData, err := ioutil.ReadFile(*configFile)
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

	// Parse json to Config
    var config Config
    err = json.Unmarshal(configData, &config)
    if err != nil {
        log.Fatalf("Failed to parse config file: %v", err)
    }

    // Get deviceConfigs
    deviceConfigs := config.Devices
    coordinatorConfig := config.Coordinator

    // Process all the deviceConfigs
    var ownConfig *utl.DeviceConfig
    peers := make(map[uint32]*sl.PeerInfo)
    for i, deviceConfig := range deviceConfigs {
        rank := uint32(i)

        if deviceConfig.DeviceID == uint64(*deviceID) {
            ownConfig = &deviceConfig
        } else {
            peers[rank] = &sl.PeerInfo{
                IpAddress: deviceConfig.IPAddress,
                Port:      deviceConfig.Port,
            }
        }
    }

    if ownConfig == nil {
        log.Fatalf("Device ID %d not found in config file", *deviceID)
    }

	// Start the gRPC server
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", ownConfig.Port))
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    s := grpc.NewServer()
	
    pb.RegisterGPUDeviceServer(s, sl.MakeGPUDeviceServer(uint64(*deviceID), coordinatorConfig, peers))

	log.Printf("GPU Device server (device ID: %d) listening at %v", *deviceID, lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
