package server_lib

import (
    "context"
    pb "github.com/Ian2x/cs426-dsml/proto"
    "sync"
    "fmt"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"
    "time"
    "log"

    utl "github.com/Ian2x/cs426-dsml/util"
)

// GpuCoordinatorServer implements the GPUCoordinator service
type GpuCoordinatorServer struct {
    pb.UnimplementedGPUCoordinatorServer

    // Devices and Communicators
    Devices        map[uint64]*utl.DeviceConfig       // DeviceID to DeviceConfig
    RankToDeviceID map[uint32]uint64              // Rank to DeviceID
    Communicators  map[uint64]*communicator       // CommID to Communicator

    // Other state
    NextCommID     uint64

    // Lock
    Mu             sync.Mutex     // NOTE: make RWMutex for faster reads?
    // ADDED: timeouts for each device
    deviceTimeouts map[uint64]time.Time           // DeviceID to timeout
    addDuration time.Duration
}

type communicator struct {
    commID       uint64
    devices      map[uint32]*utl.DeviceConfig // Rank to DeviceConfig
    groupStarted bool
    opQueue      []operation
    status       pb.Status
}

type operation struct {
    opType string
    opReq    interface{}
}

func MakeGPUCoordinatorServer() *GpuCoordinatorServer{
    server := GpuCoordinatorServer{
        Devices:        make(map[uint64]*utl.DeviceConfig),
        RankToDeviceID: make(map[uint32]uint64),
        Communicators:  make(map[uint64]*communicator),
        NextCommID:     1,
        DeviceTimeouts: make(map[uint64]time.Time),
        addDuration:    1000 * time.Millisecond,
    }

    // set timeouts
    t := time.Now()
    for _, d := range server.Devices {
        server.deviceTimeouts[d.DeviceID] = t + server.addDuration
    }

    go server.checkExpired()

    return &server
}

func (s *GpuCoordinatorServer) CommInit(ctx context.Context, req *pb.CommInitRequest) (*pb.CommInitResponse, error) {
    s.Mu.Lock()
    defer s.Mu.Unlock()

    if req.NumDevices == 0 {
        return nil, fmt.Errorf("Need > 0 devices in comunicator")
    }

    if uint32(len(s.Devices)) < req.NumDevices {
        return nil, fmt.Errorf("Not enough devices for communicator")
    }

    // Get new commID
    commID := s.NextCommID
    s.NextCommID++

    // Select devices for communicator
    communicator := &communicator{
        commID:  commID,
        devices: make(map[uint32]*utl.DeviceConfig),
        groupStarted: false,
        opQueue: make([]operation, 0, req.NumDevices * 100),
        status: pb.Status_IN_PROGRESS,
    }

    var devicesMetadata []*pb.DeviceMetadata
    rank := uint32(0)
    for _, device := range s.Devices {
        // Just need req.NumDevices
        if rank >= req.NumDevices {
            break
        }

        // Assign device to communicator
        communicator.devices[rank] = device

        // Add device to devicesMetadata (for response)
        devicesMetadata = append(devicesMetadata, &pb.DeviceMetadata{
            DeviceId:   &pb.DeviceId{Value: device.DeviceID},
            MinMemAddr: &pb.MemAddr{Value: device.MinMemAddr},
            MaxMemAddr: &pb.MemAddr{Value: device.MaxMemAddr},
        })

        s.RankToDeviceID[rank] = device.DeviceID
        rank++
    }

    s.Communicators[commID] = communicator

    return &pb.CommInitResponse{
        Success: true,
        CommId:  commID,
        Devices: devicesMetadata,
    }, nil
}

func (s *GpuCoordinatorServer) GetCommStatus(ctx context.Context, req *pb.GetCommStatusRequest) (*pb.GetCommStatusResponse, error) {
    s.Mu.Lock()
    defer s.Mu.Unlock()

    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    return &pb.GetCommStatusResponse{
        Status: communicator.status,
    }, nil
}

func (s *GpuCoordinatorServer) GroupStart(ctx context.Context, req *pb.GroupStartRequest) (*pb.GroupStartResponse, error) {
    s.Mu.Lock()
    defer s.Mu.Unlock()

    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    if communicator.groupStarted {
        return nil, fmt.Errorf("Group already started")
    }

    communicator.groupStarted = true

    return &pb.GroupStartResponse{
        Success: true,
    }, nil
}

func (s *GpuCoordinatorServer) GroupEnd(ctx context.Context, req *pb.GroupEndRequest) (*pb.GroupEndResponse, error) {
    s.Mu.Lock()

    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        s.Mu.Unlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    if !communicator.groupStarted {
        s.Mu.Unlock()
        return nil, fmt.Errorf("No group operation started")
    }

    // Copy queued ops
    queuedOps := communicator.opQueue

    // Clear opQueue and reset for next group
    communicator.opQueue = nil
    communicator.groupStarted = false
    s.Mu.Unlock()

    // Execute operations asynchronously
    var wg sync.WaitGroup
    errCh := make(chan error, len(queuedOps))

    for _, op := range queuedOps {
        wg.Add(1)
        go func(op operation) {
            defer wg.Done()

            switch op.opType {
                case "AllReduceRing":
                    allReduceRingRequest := op.opReq.(*pb.AllReduceRingRequest)
                    if err := s.executeAllReduceRing(allReduceRingRequest); err != nil {
                        errCh <- err
                    }
                // Other operations
                default:
                    errCh <- fmt.Errorf("Unknown operation type: %v", op.opType)
            }
        }(op)
    }

    go func() {
        wg.Wait()
        close(errCh)
    }()

    for err := range errCh {
        if err != nil {
            s.Mu.Lock()
            communicator.status = pb.Status_FAILED
            s.Mu.Unlock()
            return nil, err
        }
    }

    s.Mu.Lock()
    communicator.status = pb.Status_SUCCESS
    s.Mu.Unlock()

    return &pb.GroupEndResponse{
        Success: true,
    }, nil
}

func (s *GpuCoordinatorServer) AllReduceRing(ctx context.Context, req *pb.AllReduceRingRequest) (*pb.AllReduceRingResponse, error) {
    s.Mu.Lock()

    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        s.Mu.Unlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }
    
    if communicator.groupStarted { // If groupStarted, queue the operation.
        communicator.opQueue = append(communicator.opQueue, operation{
            opType: "AllReduceRing",
            opReq:  req,
        })
        s.Mu.Unlock()
    } else { // Else, executed immediately
        s.Mu.Unlock()
        if err := s.executeAllReduceRing(req); err != nil {
            return nil, err
        }
    }

    return &pb.AllReduceRingResponse{
        Success: true,
    }, nil
}

// invokes beginSend, beginReceive, and blocks until
// getStreamStatus returns SUCCESS or FAILED.
func (s *GpuCoordinatorServer) beginShare(
    srcRank uint32,
    srcBuffAddr uint64,
    dstRank uint32,
    dstBuffAddr uint64,
    bytesPerReq uint64,
    clients map[uint32]pb.GPUDeviceClient,
    op pb.ReduceOp,
    errCh chan error,
    wg *sync.WaitGroup,
) {
    defer wg.Done()
    // create context
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    defer cancel()
    // invoke beginSend
    beginSendResp, beginSendErr := clients[srcRank].BeginSend(
        ctx,
        &pb.BeginSendRequest{
            SendBuffAddr: &pb.MemAddr{Value: srcBuffAddr},
            NumBytes: bytesPerReq,
            DstRank: &pb.Rank{Value: dstRank},
            ReceiveOp: op,
        },
    )
    // if beginSendErr != nil {
    //     log.Fatalf("BeginSend failed: %v", beginSendErr)
    //     errCh <- beginSendErr
    //     return
    // }
    if beginSendErr != nil || !beginSendResp.Initiated {
        // handle error
        // errCh <- fmt.Errorf("BeginSend not initiated on rank=%d", srcRank)
        errCh <- utl.DeviceErrorf("BeginSend not initiated", srcRank)
        return
    }
    // invoke beginReceive
    beginRecvResp, beginRecvErr := clients[dstRank].BeginReceive(
        ctx,
        &pb.BeginReceiveRequest{
            StreamId: beginSendResp.StreamId,
            RecvBuffAddr: &pb.MemAddr{Value: dstBuffAddr},
            NumBytes: bytesPerReq,
            SrcRank: &pb.Rank{Value: srcRank},
        },
    )
    // if beginRecvErr != nil {
    //     log.Fatalf("BeginReceive failed: %v", beginRecvErr)
    //     errCh <- beginRecvErr
    //     return
    // }
    if beginRecvErr != nil || !beginRecvResp.Initiated {
        // handle error
        // errCh <- fmt.Errorf("BeginReceive not initiated on rank=%d", dstRank)
        errCh <- utl.DeviceErrorf("BeginReceive not initiated", dstRank)
        return
    }
    // check stream status
    for {
        getStatusResp, getStatusErr := clients[srcRank].GetStreamStatus(
            ctx,
            &pb.GetStreamStatusRequest{
                StreamId: beginSendResp.StreamId,
            },
        )
        if getStatusErr != nil {
            // not sure how to handle error here. maybe just resend?
            // log.Printf("GetStreamStatus error on rank=%d: %v", srcRank, getStatusErr)
            // errCh <- getStatusErr
            errCh <- utl.DeviceErrorf("Stream status FAILED", srcRank)
            return
        }
        switch getStatusResp.Status {
        case pb.Status_SUCCESS:
            return
        case pb.Status_FAILED:
            // handle error
            // errCh <- fmt.Errorf("stream status FAILED on rank=%d", srcRank)
            errCh <- utl.DeviceErrorf("Stream status FAILED", srcRank)
            return
        case pb.Status_IN_PROGRESS:
            // sleep and retry
            time.Sleep(100 * time.Millisecond)
        }
    }
}

func (s *GpuCoordinatorServer) executeAllReduceRing(req *pb.AllReduceRingRequest) error {
    // get the communicator and number of devices
    s.Mu.Lock()
    communicator := s.Communicators[req.CommId]
    if communicator == nil {
        s.Mu.Unlock()
        return fmt.Errorf("Communicator %d not found", req.CommId)
    }
    deviceClients := make(map[uint32]pb.GPUDeviceClient)
    conns := make(map[uint32]*grpc.ClientConn)
    numDevices := uint32(len(communicator.devices))

    opts := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }

    for rank, deviceConfig := range communicator.devices {
        target := fmt.Sprintf("%s:%d", deviceConfig.IPAddress, deviceConfig.Port)
        conn, err := grpc.NewClient(target, opts...)
        if err != nil {
            s.Mu.Unlock()
            return fmt.Errorf("could not create gRPC client for rank=%d device=%d: %v", rank, deviceConfig.DeviceID, err)
        }
        conns[rank] = conn
        deviceClients[rank] = pb.NewGPUDeviceClient(conn)
    }
    s.Mu.Unlock()

    // TODO: need to fix the way we index over devices if we remove expired ones
    // POSSIBLE FIX: check health of every device before performing operations
    for rank := range communicator.devices {
        // read in device memory and store it
        stream, err := deviceClients[rank].MemcpyDeviceToHost(
            context.Background(),
            &pb.MemcpyDeviceToHostRequest{
                SrcDeviceId: pb.DeviceId{Value: s.RankToDeviceID[rank]},
                SrcMemAddr: pb.MemAddr{Value: req.MemAddrs[rank]},
                NumBytes: req.NumBytes,
            }
        )
        if err != nil {
            fmt.Printf("Error starting MemcpyDeviceToHost: %v\n", err)
            return
        }
        var result []byte
        for {
            chunk, err := stream.Recv()
            if err == io.EOF {
                break
            }
            if err != nil {
                fmt.Printf("Error receiving data chunk: %v\n", err)
                return
            }
            result = append(result, chunk.Data...)
        }
    }

    // defer close connections
    defer func() {
        for _, c := range conns {
            c.Close()
        }
    }()

    // request fields
    numBytes := req.Count
    op := req.Op
    memAddrs := req.MemAddrs
    bytesPerReq := numBytes / uint64(numDevices)

    // Error channel for both phases
    errCh := make(chan error, 2 * numDevices)
    
    // begin allReduceRing
    for phase := range 2 {
        var reduceOp pb.ReduceOp
        if phase == 0 {
            // share-reduce phase
            reduceOp = op
        } else {
            // share-only phase
            reduceOp = pb.ReduceOp_WRITE
        }

        for i := uint32(0); i < numDevices - 1; i++ {
            // initialize wait group
            var wg sync.WaitGroup
            // node i sends, node i + 1 receives
            // s.Mu.Lock()
            for rank := uint32(0); rank < numDevices; rank++ {
                srcRank := uint32(rank % numDevices)
                dstRank := uint32((rank + 1) % numDevices)
                // calculate addrs for send and receive
                srcBuffAddr := uint64(memAddrs[srcRank].Value + (bytesPerReq * uint64(srcRank - i)) % numBytes)
                dstBuffAddr := uint64(memAddrs[dstRank].Value + (bytesPerReq * uint64(dstRank - i + numDevices - 1)) % numBytes)
                // invoke beginShare
                wg.Add(1)
                go s.beginShare(
                    srcRank, srcBuffAddr,
                    dstRank, dstBuffAddr,
                    bytesPerReq, deviceClients,
                    reduceOp, errCh, &wg,
                )
            }
            // s.Mu.Unlock()
            // wait for all shares to finish
            wg.Wait()
            // empty error channel and read errors
            errs := make([]*utl.DeviceError)
            DrainErrors:
            for {
                select {
                case err := <-errCh:
                    errs = append(errs, err)
                default:
                    break DrainErrors
                }
            }
            s.handleErrors(errs)
        }
    }
    close(errCh)
    
    return nil
}

func (s *GpuCoordinatorServer) Memcpy(ctx context.Context, req *pb.MemcpyRequest) (*pb.MemcpyResponse, error) {
    switch op := req.Either.(type) {
        case *pb.MemcpyRequest_HostToDevice:
            return s.handleMemcpyHostToDevice(ctx, op.HostToDevice)
        case *pb.MemcpyRequest_DeviceToHost:
            return s.handleMemcpyDeviceToHost(ctx, op.DeviceToHost)
        default:
            return nil, fmt.Errorf("Invalid Memcpy operation")
    }
}

func (s *GpuCoordinatorServer) handleMemcpyHostToDevice(ctx context.Context, req *pb.MemcpyHostToDeviceRequest) (*pb.MemcpyResponse, error) {
    s.Mu.Lock()
    deviceInfo, exists := s.Devices[req.DstDeviceId.Value]
    if !exists {
        s.Mu.Unlock()
        return nil, fmt.Errorf("Destination device not found")
    }
    s.Mu.Unlock()

    // Connect to destination device
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", deviceInfo.IPAddress, deviceInfo.Port), opts...)
    if err != nil {
        return nil, err
    }
    defer conn.Close()

    client := pb.NewGPUDeviceClient(conn)

    // Add dstMemAddr to context
    md := metadata.Pairs("dstmemaddr", fmt.Sprintf("%d", req.DstMemAddr.Value))

    stream, err := client.MemcpyHostToDevice(metadata.NewOutgoingContext(ctx, md))
    if err != nil {
        return nil, err
    }

    // Send data to device
    data := req.HostSrcData

    chunk := &pb.DataChunk{
        Data: data,
    }

    if err := stream.Send(chunk); err != nil {
        return nil, err
    }

    // Close the stream and receive the response
    resp, err := stream.CloseAndRecv()
    if err != nil || !resp.Success {
        return nil, fmt.Errorf("MemcpyHostToDevice failed")
    }

    return &pb.MemcpyResponse{
        Either: &pb.MemcpyResponse_HostToDevice{
            HostToDevice: &pb.MemcpyHostToDeviceResponse{
                Success: true,
            },
        },
    }, nil
}

func (s *GpuCoordinatorServer) handleMemcpyDeviceToHost(ctx context.Context, req *pb.MemcpyDeviceToHostRequest) (*pb.MemcpyResponse, error) {
    s.Mu.Lock()
    deviceInfo, exists := s.Devices[req.SrcDeviceId.Value]
    if !exists {
        s.Mu.Unlock()
        return nil, fmt.Errorf("Source device not found")
    }
    s.Mu.Unlock()

    // Connect to source device
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", deviceInfo.IPAddress, deviceInfo.Port), opts...)
    if err != nil {
        return nil, err
    }
    defer conn.Close()

    client := pb.NewGPUDeviceClient(conn)

    // Retrieve data from device
    stream, err := client.MemcpyDeviceToHost(context.Background(), &pb.MemcpyDeviceToHostRequest{
        SrcDeviceId: req.SrcDeviceId,
        SrcMemAddr: req.SrcMemAddr,
        NumBytes:   req.NumBytes,
    })
    if err != nil {
        return nil, err
    }

    chunk, err := stream.Recv()
    if err != nil {
        return nil, err 
    }

    return &pb.MemcpyResponse{
        Either: &pb.MemcpyResponse_DeviceToHost{
            DeviceToHost: &pb.MemcpyDeviceToHostResponse{
                DstData: chunk.Data,
            },
        },
    }, nil
}

func (s *GpuCoordinatorServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
    // lock/unlock
    s.Mu.Lock()
    defer s.Mu.Unlock()
    // check device id
    id := req.DeviceId
    // update timeout for device
    s.deviceTimeouts[id] = time.Now() + s.addDuration
    // return
    return &pb.HeartbeatResponse{Success: true}
}

// goroutine to check if timeouts are expired
func (s *GpuCoordinatorServer) checkExpired() {
    for {
        // lock
        s.Mu.Lock()
        // list of expired devices
        expired := make([]uint64)
        // check timeouts of devices
        t := time.Now()
        for _, d := range s.Devices {
            if s.deviceTimeouts[d.DeviceID].After(t) {
                expired := append(expired, d.DeviceID)
            }
        }
        // device(s) are expired, trigger restart
        if len(expired) > 0 {
            // update communicators

        }
        // unlock
        s.Mu.Unlock()
        time.Sleep(200 * time.Millisecond)
    }
}