package main

import (
    "context"
    "log"
    "time"

    pb "../proto" // Adjust the import path as necessary
    "google.golang.org/grpc"
)

func main() {
    // Establish a connection to the GPUCoordinator server
    conn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("Did not connect: %v", err)
    }
    defer conn.Close()

    client := pb.NewGPUCoordinatorClient(conn)

    // N vectors on "CPU / host"
    N := 4 // Example number of devices
    vecs := make([][]float64, N)
    // Fill vectors with data...
    for i := 0; i < N; i++ {
        vecs[i] = []float64{float64(i + 1), float64(i + 2)} // Example data
    }

    // Create / initialize a communicator with N GPUs
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    defer cancel()
    commInitResp, err := client.CommInit(ctx, &pb.CommInitRequest{
        NumDevices: uint32(N),
    })
    if err != nil {
        log.Fatalf("CommInit failed: %v", err)
    }
    if !commInitResp.Success {
        log.Fatalf("CommInit was unsuccessful")
    }
    commId := commInitResp.CommId
    devices := commInitResp.Devices

    // Transfer vectors to each individual GPU
    for i := 0; i < N; i++ {
        gpu := devices[i]
        // Convert vecs[i] to bytes
        dataBytes := Float64SliceToByteArray(vecs[i]) // Implement this utility function

        _, err := client.Memcpy(ctx, &pb.MemcpyRequest{
            HostToDevice: &pb.MemcpyHostToDeviceRequest{
                HostSrcData: dataBytes,
                DstDeviceId: gpu.DeviceId,
                DstMemAddr:  gpu.MinMemAddr,
            },
        })
        if err != nil {
            log.Fatalf("Memcpy failed: %v", err)
        }
    }

    // Initiate the operation
    _, err = client.GroupStart(ctx, &pb.GroupStartRequest{
        CommId: commId,
    })
    if err != nil {
        log.Fatalf("GroupStart failed: %v", err)
    }

    // Prepare memAddrs map for AllReduceRingRequest
    memAddrs := make(map[uint32]*pb.MemAddr)
    for i, gpu := range devices {
        memAddrs[uint32(i)] = gpu.MinMemAddr
    }

    _, err = client.AllReduceRing(ctx, &pb.AllReduceRingRequest{
        CommId:   commId,
        Count:    uint64(len(vecs[0])),
        Op:       pb.ReduceOp_SUM,
        MemAddrs: memAddrs,
    })
    if err != nil {
        log.Fatalf("AllReduceRing failed: %v", err)
    }

    _, err = client.GroupEnd(ctx, &pb.GroupEndRequest{
        CommId: commId,
    })
    if err != nil {
        log.Fatalf("GroupEnd failed: %v", err)
    }

    // Check status / synchronize
    for {
        commStatusResp, err := client.GetCommStatus(ctx, &pb.GetCommStatusRequest{
            CommId: commId,
        })
        if err != nil {
            log.Fatalf("GetCommStatus failed: %v", err)
        }
        if commStatusResp.Status == pb.Status_IN_PROGRESS {
            time.Sleep(time.Second)
            continue
        } else if commStatusResp.Status == pb.Status_FAILED {
            log.Fatalf("Communication failed")
        } else {
            break
        }
    }

    // Data transfer out from GPU 0 to "CPU"
    memcpyResp, err := client.Memcpy(ctx, &pb.MemcpyRequest{
        DeviceToHost: &pb.MemcpyDeviceToHostRequest{
            SrcDeviceId: devices[0].DeviceId,
            SrcMemAddr:  devices[0].MinMemAddr,
            NumBytes:    uint64(len(vecs[0]) * 8), // Assuming float64
        },
    })
    if err != nil {
        log.Fatalf("Memcpy failed: %v", err)
    }

    vecOut := ByteArrayToFloat64Slice(memcpyResp.DeviceToHost.DstData) // Implement this utility function

    // Output vecOut as final result
    log.Printf("Result vector: %v", vecOut)

    // Free any resources etc.
    // Implement CommDestroy if necessary
}

// Float64SliceToByteArray converts a slice of float64 to a byte array
func Float64SliceToByteArray(floats []float64) []byte {
    // Implement conversion
    return []byte{}
}

// ByteArrayToFloat64Slice converts a byte array to a slice of float64
func ByteArrayToFloat64Slice(data []byte) []float64 {
    // Implement conversion
    return []float64{}
}
