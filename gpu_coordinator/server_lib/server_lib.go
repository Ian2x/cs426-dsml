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
    "math"

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
    }

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
    communicator.status = pb.Status_IN_PROGRESS

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

    // Execute operations in-order, asynchronously
    go func() {
        errCh := make(chan error, len(queuedOps))
        
        for _, op := range queuedOps {
            switch op.opType {
                case "AllReduceRing":
                    allReduceRingRequest := op.opReq.(*pb.AllReduceRingRequest)
                    if err := s.executeAllReduceRing(allReduceRingRequest); err != nil {
                        errCh <- err
                    }
                case "AllReduce":
                    allReduceRequest := op.opReq.(*pb.AllReduceRequest)
                    if err := s.executeAllReduce(allReduceRequest); err != nil {
                        errCh <- err
                    }
                // Other operations
                default:
                    errCh <- fmt.Errorf("Unknown operation type: %v", op.opType)
            }
        }

        close(errCh)

        for err := range errCh {
            if err != nil {
                s.Mu.Lock()
                communicator.status = pb.Status_FAILED
                s.Mu.Unlock()
                log.Printf("GroupEnd received error: %v", err)
            }
        }
        s.Mu.Lock()
        communicator.status = pb.Status_SUCCESS
        s.Mu.Unlock()
    }()

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

func (s *GpuCoordinatorServer) AllReduce(ctx context.Context, req *pb.AllReduceRequest) (*pb.AllReduceResponse, error) {
    s.Mu.Lock()

    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        s.Mu.Unlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }
    
    if communicator.groupStarted { // If groupStarted, queue the operation.
        communicator.opQueue = append(communicator.opQueue, operation{
            opType: "AllReduce",
            opReq:  req,
        })
        s.Mu.Unlock()
    } else { // Else, executed immediately
        s.Mu.Unlock()
        if err := s.executeAllReduce(req); err != nil {
            return nil, err
        }
    }

    return &pb.AllReduceResponse{
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
    if beginSendErr != nil {
        log.Fatalf("BeginSend failed: %v", beginSendErr)
        errCh <- beginSendErr
        return
    }
    if !beginSendResp.Initiated {
        // handle error
        errCh <- fmt.Errorf("BeginSend not initiated on rank=%d", srcRank)
        return
    }
    // invoke beginReceive
    log.Printf("Invoked send [%d (addr: %d) --> %d (addr: %d)] on streamID %d", srcRank, srcBuffAddr, dstRank, dstBuffAddr, beginSendResp.StreamId.Value)
    beginRecvResp, beginRecvErr := clients[dstRank].BeginReceive(
        ctx,
        &pb.BeginReceiveRequest{
            StreamId: beginSendResp.StreamId,
            RecvBuffAddr: &pb.MemAddr{Value: dstBuffAddr},
            NumBytes: bytesPerReq,
            SrcRank: &pb.Rank{Value: srcRank},
        },
    )
    if beginRecvErr != nil {
        log.Fatalf("BeginReceive failed: %v", beginRecvErr)
        errCh <- beginRecvErr
        return
    }
    if !beginRecvResp.Initiated {
        // handle error
        errCh <- fmt.Errorf("BeginReceive not initiated on rank=%d", dstRank)
        return
    }
    // check stream status
    for {
        // note: seems like stream is only recorded in the src node, not in the dst node
        // (could be correct behavior though)
        getStatusResp, getStatusErr := clients[srcRank].GetStreamStatus(
            ctx,
            &pb.GetStreamStatusRequest{
                StreamId: beginSendResp.StreamId,
            },
        )
        if getStatusErr != nil {
            // not sure how to handle error here. maybe just resend?
            log.Printf("GetStreamStatus error on rank=%d: %v", srcRank, getStatusErr)
            errCh <- getStatusErr
            return
        }
        switch getStatusResp.Status {
        case pb.Status_SUCCESS:
            return
        case pb.Status_FAILED:
            // handle error
            // errCh <- (some error)
            errCh <- fmt.Errorf("stream status FAILED on rank=%d", srcRank)
            return
        case pb.Status_IN_PROGRESS:
            // sleep and retry
            time.Sleep(100 * time.Millisecond)
        }
    }
}

func (s *GpuCoordinatorServer) executeAllReduceRing(req *pb.AllReduceRingRequest) error {
    // Get the communicator and number of devices
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

    // Close connections
    defer func() {
        for _, c := range conns {
            c.Close()
        }
    }()

    // Request fields
    // Assumptions: arrays of 8 byte data (e.g. float64)
    numBytes := uint64(math.Ceil((float64(req.Count) / 8 / float64(numDevices)))) * 8 * uint64(numDevices)
    op := req.Op
    memAddrs := req.MemAddrs
    bytesPerReq := numBytes / uint64(numDevices)

    // Error channel for both phases
    errCh := make(chan error, 2 * (numDevices - 1))
    
    // begin allReduceRing
    for phase := uint32(0); phase < 2; phase++ {
        var reduceOp pb.ReduceOp
        if phase == 0 {
            // share-reduce phase
            reduceOp = op
        } else {
            // share-only phase
            reduceOp = pb.ReduceOp_WRITE
        }

        for i := uint32(0); i < numDevices - 1; i++ {
            log.Printf("STARTING PHASE %d, STEP %d", phase, i)

            var wg sync.WaitGroup
            for rank := uint32(0); rank < numDevices; rank++ {
                srcRank := uint32(rank % numDevices)
                dstRank := uint32((rank + 1) % numDevices)

                // Calculate addrs for send and receive
                srcBuffAddr := uint64(memAddrs[srcRank].Value + (bytesPerReq * uint64((srcRank + numDevices - i + phase) % numDevices)) % numBytes)
                dstBuffAddr := uint64(memAddrs[dstRank].Value + (bytesPerReq * uint64((dstRank + 2 * numDevices - i - 1 + phase) % numDevices)) % numBytes)
                
                // invoke beginShare
                wg.Add(1)
                go s.beginShare(
                    srcRank, srcBuffAddr,
                    dstRank, dstBuffAddr,
                    bytesPerReq, deviceClients,
                    reduceOp, errCh, &wg,
                )
            }
            wg.Wait()

            // empty error channel and read errors
            DrainErrors:
            for {
                select {
                    case err := <-errCh:
                        log.Printf("Error in executeAllReduceRing: %v", err)
                    default:
                        break DrainErrors
                }
            }
        }
    }
    close(errCh)
    
    return nil
}

func (s *GpuCoordinatorServer) executeAllReduce(req *pb.AllReduceRequest) error {
    // Get the communicator and number of devices
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

    // Close connections
    defer func() {
        for _, c := range conns {
            c.Close()
        }
    }()

    // Request fields
    // Assumptions: arrays of 8 byte data (e.g. float64)
    numBytes := uint64(math.Ceil((float64(req.Count) / 8))) * 8
    op := req.Op
    memAddrs := req.MemAddrs

    // Error channel for all msgs
    errCh := make(chan error, numDevices * (numDevices - 1))

    // begin allReduce
    var wg sync.WaitGroup
    for srcRank := uint32(0); srcRank < numDevices; srcRank++ {
        for dstRank := uint32(0); dstRank < numDevices; dstRank++ {
            if srcRank == dstRank {
                continue
            }
            
            srcBuffAddr := uint64(memAddrs[srcRank].Value)
            dstBuffAddr := uint64(memAddrs[dstRank].Value)

            wg.Add(1)
            go s.beginShare(
                srcRank, srcBuffAddr,
                dstRank, dstBuffAddr,
                numBytes, deviceClients,
                op, errCh, &wg,
            )
        }
    }

    wg.Wait()

    // empty error channel and read errors
    DrainErrors:
    for {
        select {
            case err := <-errCh:
                log.Printf("Error in executeAllReduce: %v", err)
            default:
                break DrainErrors
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