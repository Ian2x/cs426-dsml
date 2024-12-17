package main

import (
    "context"
    "log"
    "time"
    "math"

    pb "github.com/Ian2x/cs426-dsml/proto"
    utl "github.com/Ian2x/cs426-dsml/util"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    // Connect to GPU Coordinator
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient("coordinator:8082", opts...)
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()
    client := pb.NewGPUCoordinatorClient(conn)

    // Create data vectors
    N := 4
    vecs := make([][]float64, N)
    for i := 0; i < N; i++ {
        vecs[i] = []float64{float64(math.Pow(2, float64(i))), float64(math.Pow(2, float64(i))), float64(math.Pow(2, float64(i))), float64(math.Pow(2, float64(i)))} // Example data
    }

    // Create communicator
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

    // Memcpy vectors to each GPU
    for i := 0; i < N; i++ {
        gpu := devices[i]
        dataBytes := utl.Float64SliceToByteArray(vecs[i])

        _, err := client.Memcpy(ctx, &pb.MemcpyRequest{
            Either: &pb.MemcpyRequest_HostToDevice{
                HostToDevice: &pb.MemcpyHostToDeviceRequest{
                    HostSrcData: dataBytes,
                    DstDeviceId: gpu.DeviceId,
                    DstMemAddr:  gpu.MinMemAddr,
                },
            },
        })
        if err != nil {
            log.Fatalf("Memcpy (HostToDevice) failed: %v", err)
        }
    }

    // Start the group
    _, err = client.GroupStart(ctx, &pb.GroupStartRequest{
        CommId: commId,
    })
    if err != nil {
        log.Fatalf("GroupStart failed: %v", err)
    }

    // Execute AllReduceRing
    memAddrs := make(map[uint32]*pb.MemAddr)
    for i, gpu := range devices {
        memAddrs[uint32(i)] = gpu.MinMemAddr
    }

    _, err = client.AllReduceRing(ctx, &pb.AllReduceRingRequest{
        CommId:   commId,
        Count:    uint64(8 * len(vecs[0])), // 8 bytes per uint64 (i think)
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

    // Check status
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

    // Loop through devices 0 to 3
    for i := range 4 {

        // Perform the Memcpy operation for the current device
        memcpyResp, err := client.Memcpy(ctx, &pb.MemcpyRequest{
            Either: &pb.MemcpyRequest_DeviceToHost{
                DeviceToHost: &pb.MemcpyDeviceToHostRequest{
                    SrcDeviceId: devices[i].DeviceId,                   // Current device ID
                    SrcMemAddr:  devices[i].MinMemAddr,                  // Current device memory address
                    NumBytes:    uint64(len(vecs[i]) * 8),               // Number of bytes to copy
                },
            },
        })
        if err != nil {
            log.Fatalf("Memcpy (DeviceToHost) failed for device %d: %v", i, err)
        }

        // Print Memcpy response
        deviceToHostResp, ok := memcpyResp.Either.(*pb.MemcpyResponse_DeviceToHost)
        if !ok {
            log.Fatalf("Failed to parse Memcpy response as DeviceToHost for device %d", i)
        }

        vecOut := utl.ByteArrayToFloat64Slice(deviceToHostResp.DeviceToHost.DstData)

        log.Printf("Received data from device %d: %v", i, vecOut)

    }
}