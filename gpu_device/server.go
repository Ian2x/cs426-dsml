package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "net"

	sl "cs426-dsml/gpu_device/server_lib"
    pb "../proto"
    "google.golang.org/grpc"
)

type DeviceConfig struct {
    DeviceID  uint64	// json:"deviceId"
    IPAddress string	// json:"ipAddress"
    Port      uint64	// json:"port"
}

/*
./gpu_device --device_id=<device_id>
*/
var (
	deviceID			= flag.Int("device_id", 0, "Unique device ID")
	configFilePath		= flag.String("config_file", "config.json", "Path to config file")
)

func main() {
    flag.Parse()

    if *deviceID == 0 {
        log.Fatalf("Usage: gpu_device --device_id=<device_id> [--config_file=<config_file>]")
    }

    // Read config file
    configData, err := ioutil.ReadFile(*configFilePath)
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

	// Parse json to []DeviceConfig
    var deviceConfigs []DeviceConfig
    err = json.Unmarshal(configData, &deviceConfigs)
    if err != nil {
        log.Fatalf("Failed to parse config file: %v", err)
    }

    // Process all the deviceConfigs
    var ownConfig *DeviceConfig
    peers := make(map[uint32]*peerInfo)
    for i, dev := range deviceConfigs {
        rank := uint32(i)

        if dev.DeviceID == *deviceID {
            ownConfig = &dev
        } else {
            peers[rank] = &peerInfo{
                deviceID:  dev.DeviceID,
                ipAddress: dev.IPAddress,
                port:      dev.Port,
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
	
    pb.RegisterGPUDeviceServer(s, sl.MakeGPUDeviceServer(*deviceID, peers))

	log.Printf("GPU Device Server (Device ID: %d) listening at %v", *deviceId, lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
