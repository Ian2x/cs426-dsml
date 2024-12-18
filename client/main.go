package main

import (
    "context"
    "log"
    "time"
    "math/rand"
    "os"
	"strconv"
    "sync"

    pb "github.com/Ian2x/cs426-dsml/proto"
    utl "github.com/Ian2x/cs426-dsml/util"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func reduce(client pb.GPUCoordinatorClient, algo string, ctx context.Context, commId uint64, count uint64, op pb.ReduceOp, memAddrs map[uint32]*pb.MemAddr) {
    var err error
    switch algo {
    case "allreduce":
        _, err = client.AllReduce(ctx, &pb.AllReduceRequest{
            CommId:   commId,
            Count:    count,
            Op:       op,
            MemAddrs: memAddrs,
        })
    default:
        _, err = client.AllReduceRing(ctx, &pb.AllReduceRingRequest{
            CommId:   commId,
            Count:    count,
            Op:       op,
            MemAddrs: memAddrs,
        })
    }
    if err != nil {
        log.Fatalf("%s failed: %v", algo, err)
    }
}

func main() {
    var mu sync.Mutex
    // Parse TEST and ALGO
    testStr := os.Getenv("TEST")
	test, err := strconv.Atoi(testStr)
	if err != nil {
		test = 0
	}
    switch test {
    case 1, 2:
    default:
        log.Printf("TEST not properly set -> using default value (1)")
        test = 1
    }
    algo := os.Getenv("ALGO")
    switch algo {
    case "allreduce", "allreducering":
    default:
        log.Printf("ALGO not properly set -> using default value (allreducering)")
        algo = "allreducering"
    }
    fail := os.Getenv("FAIL")
    switch fail {
    case "before", "during":
    default:
        fail = "none"
    }


	// Define variables
	var N int
	var vecs [][]float64
    var ops []pb.ReduceOp

	// Which test to run
	switch test {
    case 1:
        N = 10
        vecs = [][]float64{
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        }
        ops = []pb.ReduceOp{pb.ReduceOp_SUM}
    case 2:
        N = 3
        vecs = [][]float64{
            {0, 1, 3, 9, 27},
            {0, 1, 3, 9, 27},
            {0, 1, 3, 9, 27},
        }
        ops = []pb.ReduceOp{pb.ReduceOp_SUM, pb.ReduceOp_SUM}
    case 3:
        N = 3
        vecs = [][]float64{
            {1, 6, 7},
            {2, 5, 8},
            {3, 4, 9},
        }
        ops = []pb.ReduceOp{pb.ReduceOp_MIN, pb.ReduceOp_SUM}
    default:
        N = 2
        vecs = [][]float64{
            {1, 2},
            {3, 4},
        }
        ops = []pb.ReduceOp{pb.ReduceOp_PROD} // sum, prod, min, max
    }

    // Connect to GPU Coordinator
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient("coordinator:8082", opts...)
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()
    client := pb.NewGPUCoordinatorClient(conn)

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

    // remove a device before group starts
    if fail == "before" {
        randomIndex := uint32(rand.Intn(len(devices)))
        commRemoveResp, err := client.CommRemoveDevice(
            context.Background(),
            &pb.CommRemoveDeviceRequest{
                CommId: commId,
                Rank: &pb.Rank{Value: randomIndex},
            },
        )
        if err != nil {
            log.Fatalf("CommRemoveDevice failed: %v", err)
        }
        if !commRemoveResp.Success {
            log.Fatalf("CommRemoveDevice was unsuccessful")
        }
        log.Printf("CommRemoveDevice succeeded")
        // devices = commRemoveResp.Devices
        // N = N - 1
    }

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

    // Prepare the group
    groupStart := time.Now()
    _, err = client.GroupStart(ctx, &pb.GroupStartRequest{
        CommId: commId,
    })
    if err != nil {
        log.Fatalf("GroupStart failed: %v", err)
    }

    memAddrs := make(map[uint32]*pb.MemAddr)
    for i, gpu := range devices {
        memAddrs[uint32(i)] = gpu.MinMemAddr
    }

    // End group
    for _, op := range ops {
        reduce(client, algo, ctx, commId, uint64(8 * len(vecs[0])), op, memAddrs)
    }

    // remove device while ARR is executing
    if fail == "during" {
        go func() {
            time.Sleep(100 * time.Millisecond)
            mu.Lock()
            randomIndex := uint32(rand.Intn(len(devices)))
            commRemoveResp, err := client.CommRemoveDevice(
                context.Background(),
                &pb.CommRemoveDeviceRequest{
                    CommId: commId,
                    Rank: &pb.Rank{Value: randomIndex},
                },
            )
            if err != nil {
                log.Fatalf("CommRemoveDevice failed: %v", err)
            }
            if !commRemoveResp.Success {
                log.Fatalf("CommRemoveDevice was unsuccessful")
            }
            log.Printf("CommRemoveDevice succeeded (%d)", randomIndex)
            mu.Unlock()
        }()
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
    groupDuration := time.Since(groupStart)
    log.Printf("Test %d with %s took %v to complete", test, algo, groupDuration)

    // Loop through devices 0 to N and check results
    for i := range N {

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
