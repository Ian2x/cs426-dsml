package server_lib

import (
    "context"
    pb "github.com/Ian2x/cs426-dsml/proto"
    "sync"
    "google.golang.org/grpc/metadata"
    "strconv"
    "time"
    "fmt"
    "log"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    utl "github.com/Ian2x/cs426-dsml/util"

    // "os"
)

type gpuDeviceServer struct {
    pb.UnimplementedGPUDeviceServer
    
    // DeviceMetadata
    deviceId        uint64
    minMemAddr      uint64
    maxMemAddr      uint64

    // Simulated memory
    memory          []byte

    // Other state
    coordinator         *utl.CoordinatorConfig
    coordinatorConn     *grpc.ClientConn
    peers               map[uint32]*PeerInfo
    nextStreamIndex     uint64
    streams             map[uint64]*streamInfo
    isHealthy           bool
    opQueue             chan *operation

    // Lock
    mu sync.Mutex
}

type operation struct {
    opType      string // "send" or "recv"
    reduceOp    pb.ReduceOp
    streamID    uint64
}

type PeerInfo struct {
    IpAddress  string
    Port       uint64
}

type streamInfo struct {
    status        pb.Status
    sendBuffAddr  uint64
    recvBuffAddr  uint64
    numBytes      uint64
    srcRank       uint32
    dstRank       uint32
}

func MakeGPUDeviceServer(deviceID uint64, coordinatorConfig utl.CoordinatorConfig, peers map[uint32]*PeerInfo) *gpuDeviceServer{
    // establish connection with coordinator
    opts := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }
    target := fmt.Sprintf("%s:%d", coordinatorConfig.IPAddress, coordinatorConfig.Port)
    conn, err := grpc.NewClient(target, opts...)
    if err != nil {
        log.Printf("could not create gRPC client for GPUCoordinator on device=%d: %v", deviceID, err)
        return nil
    }

    server := gpuDeviceServer{
        deviceId:           deviceID,
        minMemAddr:         0,
        maxMemAddr:         1024 * 1024, // 1 MB memory
        memory:             make([]byte, 1024*1024),
        coordinator:        &coordinatorConfig,
        coordinatorConn:    conn,
        peers:              peers, // Read in from config files
        nextStreamIndex:    1,
        streams:            make(map[uint64]*streamInfo),
        isHealthy:          true,
        opQueue:            make(chan *operation, 100), // Queue up to 100 operations
    }

    // Start processing operations
    go server.processOperations()
    // ADDED: start sending heartbeats to coordinator
    go server.sendHeartbeats()

    return &server
}

func (s *gpuDeviceServer) GetDeviceMetadata(ctx context.Context, req *pb.GetDeviceMetadataRequest) (*pb.GetDeviceMetadataResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if !s.isHealthy {
        return nil, fmt.Errorf("Device %d is unhealthy", s.deviceId)
    }

    return &pb.GetDeviceMetadataResponse{
        Metadata: &pb.DeviceMetadata{
            DeviceId:    &pb.DeviceId{Value: s.deviceId},
            MinMemAddr:  &pb.MemAddr{Value: s.minMemAddr},
            MaxMemAddr:  &pb.MemAddr{Value: s.maxMemAddr},
        },
    }, nil
}

func (s *gpuDeviceServer) BeginSend(ctx context.Context, req *pb.BeginSendRequest) (*pb.BeginSendResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if !s.isHealthy {
        return nil, fmt.Errorf("Device %d is unhealthy", s.deviceId)
    }

    // Get new streamID
    streamID := (s.deviceId << 32) | s.nextStreamIndex // Kinda rough
    s.nextStreamIndex++

    // Store & add streamInfo
    s.streams[streamID] = &streamInfo{
        status: pb.Status_IN_PROGRESS,
        sendBuffAddr: req.SendBuffAddr.Value,
        numBytes: req.NumBytes,
        dstRank: req.DstRank.Value,
    }

    // Queue the operation
    op := &operation{
        opType: "send",
        reduceOp: req.ReceiveOp,
        streamID: streamID,
    }
    s.opQueue <- op

    // Return BeginSendResponse
    return &pb.BeginSendResponse{
        Initiated: true,
        StreamId:  &pb.StreamId{Value: streamID},
    }, nil
}

func (s *gpuDeviceServer) BeginReceive(ctx context.Context, req *pb.BeginReceiveRequest) (*pb.BeginReceiveResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if !s.isHealthy {
        return nil, fmt.Errorf("Device %d is unhealthy", s.deviceId)
    }

    // Store & add streamInfo
    s.streams[req.StreamId.Value] = &streamInfo{
        status: pb.Status_IN_PROGRESS,
        recvBuffAddr: req.RecvBuffAddr.Value,
        numBytes: req.NumBytes,
        srcRank: req.SrcRank.Value,
    }

    // Queue the operation
    op := &operation{
        opType: "recv",
        // reduceOp: nil, // will get reduceOp in StreamSend from the context
        streamID: req.StreamId.Value,
    }
    s.opQueue <- op

    // Return BeginReceiveResponse
    return &pb.BeginReceiveResponse{
        Initiated: true,
    }, nil
}

func (s *gpuDeviceServer) StreamSend(stream pb.GPUDevice_StreamSendServer) error {
    // Retrieve steamId from context
    md, ok := metadata.FromIncomingContext(stream.Context())
    if !ok {
        return fmt.Errorf("Missing StreamSend metadata")
    }

    // get streamID metadata
    streamIDs := md.Get("streamid")
    if len(streamIDs) == 0 {
        return fmt.Errorf("Missing streamid from StreamSend metadata")
    }
    streamID, err := strconv.ParseUint(streamIDs[0], 10, 64)
    if err != nil {
        return fmt.Errorf("Invalid streamID from StreamSend metadata")
    }
    
    // get reduceOp metadata
    reduceOps := md.Get("reduceop")
    if len(reduceOps) == 0 {
        return fmt.Errorf("Missing reduceop from StreamSend metadata")
    }
    reduceOpStr := reduceOps[0]

    // Get streamInfo corresponding to streamID
    s.mu.Lock()
    streamInfo, exists := s.streams[streamID]
    s.mu.Unlock()
    if !exists {
        return fmt.Errorf("Stream %d not found", streamID)
    }

    // Receive stream
    message, err := stream.Recv()
    if err != nil {
        s.mu.Lock()
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return fmt.Errorf("Failed to receive stream: %v", err)
    }

    data := message.Data
    dataLen := uint64(len(data))

    // Check size of received data
    if dataLen != streamInfo.numBytes {
        s.mu.Lock()
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return fmt.Errorf("Expected %d bytes, received %d bytes", streamInfo.numBytes, dataLen)
    }

    // Check memory memory bounds
    s.mu.Lock()
    defer s.mu.Unlock()

    if streamInfo.recvBuffAddr < s.minMemAddr || streamInfo.recvBuffAddr - s.minMemAddr + dataLen > uint64(len(s.memory)) {
        s.streams[streamID].status = pb.Status_FAILED
        return fmt.Errorf("Memory write out of bounds")
    }

    memAddr := streamInfo.recvBuffAddr - s.minMemAddr

    // Write data to memory (with correct reduceOp) and send response
    dstData := utl.ByteArrayToFloat64Slice(s.memory[memAddr:memAddr+dataLen])
    srcData := utl.ByteArrayToFloat64Slice(data)
    var newData []float64
    switch reduceOpStr {
        case "SUM":
            newData = utl.AddFloat64Slices(dstData, srcData)
        case "PROD":
            newData = utl.MultiplyFloat64Slices(dstData, srcData)
        case "MIN":
            newData = utl.MinFloat64Slices(dstData, srcData)
        case "MAX":
            newData = utl.MaxFloat64Slices(dstData, srcData)
        case "WRITE":
            newData = srcData
        default:
            return fmt.Errorf("Invalid reduceop from StreamSend metadata: %s", reduceOpStr)
    }
    copy(s.memory[memAddr:memAddr+dataLen], utl.Float64SliceToByteArray(newData))
    s.streams[streamID].status = pb.Status_SUCCESS

    return stream.SendAndClose(&pb.StreamSendResponse{Success: true})
}

func (s *gpuDeviceServer) GetStreamStatus(ctx context.Context, req *pb.GetStreamStatusRequest) (*pb.GetStreamStatusResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if !s.isHealthy {
        return nil, fmt.Errorf("Device %d is unhealthy", s.deviceId)
    }

    streamInfo, exists := s.streams[req.StreamId.Value]
    if !exists {
        return nil, fmt.Errorf("Stream %d not found", req.StreamId.Value)
    }

    return &pb.GetStreamStatusResponse{
        Status: streamInfo.status,
    }, nil
}

func (s *gpuDeviceServer) MemcpyHostToDevice(stream pb.GPUDevice_MemcpyHostToDeviceServer) error {
    // Retrieve dstMemAddr from context
    md, ok := metadata.FromIncomingContext(stream.Context())
    if !ok {
        return fmt.Errorf("Missing MemcpyHostToDevice metadata")
    }
    dstMemAddrs := md.Get("dstmemaddr")
    if len(dstMemAddrs) == 0 {
        return fmt.Errorf("Missing dstmemaddr from MemcpyHostToDevice metadata")
    }
    dstMemAddr, err := strconv.ParseUint(dstMemAddrs[0], 10, 64)
    if err != nil {
        return fmt.Errorf("Invalid dstmemaddr from MemcpyHostToDevice metadata")
    }
    
    // Receive data
    message, err := stream.Recv()
    if err != nil {
        return fmt.Errorf("Failed to receive MemcpyHostToDevice stream: %v", err)
    }

    data := message.Data
    dataLen := uint64(len(data))

    // Write data to memory
    s.mu.Lock()
    memAddr := dstMemAddr - s.minMemAddr
    if memAddr < 0 || memAddr + dataLen > uint64(len(s.memory)) {
        s.mu.Unlock()
        return fmt.Errorf("MemcpyHostToDevice has write out of bounds")
    }
    copy(s.memory[memAddr:memAddr+dataLen], data)
    s.mu.Unlock()

    return stream.SendAndClose(&pb.MemcpyHostToDeviceResponse{Success: true})
}

func (s *gpuDeviceServer) MemcpyDeviceToHost(req *pb.MemcpyDeviceToHostRequest, stream pb.GPUDevice_MemcpyDeviceToHostServer) error {
    // Retrieve srcMemAddr and numBytes from req
    srcMemAddr := req.SrcMemAddr.Value
    numBytes := req.NumBytes

    // Read data from memory
    s.mu.Lock()
    memAddr := srcMemAddr - s.minMemAddr
    if memAddr < 0 || memAddr + numBytes > uint64(len(s.memory)){
        s.mu.Unlock()
        return fmt.Errorf("MemcpyDeviceToHost has read out of bounds")
    }
    data := s.memory[memAddr : memAddr+numBytes]
    s.mu.Unlock()

    // Send data
    chunk := &pb.DataChunk{
        Data: data,
    }
    if err := stream.Send(chunk); err != nil {
        return err
    }

    return nil
}

func (s *gpuDeviceServer) sendHeartbeats() {
    // create client
    client := pb.NewGPUCoordinatorClient(s.coordinatorConn)
    // loop
    for {
        if !s.isHealthy {
            log.Printf("Server is unhealthy, stopping sendHeartbeats")
            return
        }
        // send heartbeat
        client.Heartbeat( // heartbeatResp, err
            context.Background(),
            &pb.HeartbeatRequest{DeviceId: s.deviceId},
        )
        // sleep
        time.Sleep(100 * time.Millisecond)
    }
}

func (s *gpuDeviceServer) processOperations() {
    for {
        if !s.isHealthy {
            log.Printf("Server is unhealthy, stopping processOperations")
            return
        }

        select {
            case op, ok := <-s.opQueue:
                if !ok {
                    log.Printf("Operation queue closed, stopping processOperations")
                    return
                }

                switch op.opType {
                    case "send":
                        s.handleSendOperation(op.streamID, op.reduceOp)
                    case "recv":
                        // No action needed for "recv"
                        continue
                }

            default:
                time.Sleep(10 * time.Millisecond)
        }
    }
}


// client-side GRPC streaming
func (s *gpuDeviceServer) handleSendOperation(streamID uint64, reduceOp pb.ReduceOp) {
    s.mu.Lock()
    streamInfo, exists := s.streams[streamID]
    s.mu.Unlock()
    if !exists {
        log.Printf("Stream %d not found", streamID)
        return
    }

    // Get destination PeerInfo
    dstPeer, exists := s.peers[streamInfo.dstRank]
    if !exists {
        log.Printf("Destination rank %d not found", streamInfo.dstRank)
        s.mu.Lock()
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    // Get address of dstPeer
    dstAddress := fmt.Sprintf("%s:%d", dstPeer.IpAddress, dstPeer.Port)

    // Create a gRPC client to dstAddress
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient(dstAddress, opts...)
    if err != nil {
        log.Printf("Failed to connect: %v", err)
        s.mu.Lock()
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return
    }
    defer conn.Close()

    client := pb.NewGPUDeviceClient(conn)

    // Store streamID in context
    md := metadata.Pairs(
        "streamid", fmt.Sprintf("%d", streamID),
        "reduceop", reduceOp.String(),
    )
    ctx := metadata.NewOutgoingContext(context.Background(), md)

    // Check that client (destination) is ready
    const maxRetries = 10
    const retryDelay = 100 * time.Millisecond
    for attempt := 1; attempt <= maxRetries; attempt++ {
        statusResp, err := client.GetStreamStatus(ctx, &pb.GetStreamStatusRequest{
            StreamId: &pb.StreamId{Value: streamID},
        })
        if err == nil && statusResp.Status == pb.Status_IN_PROGRESS {
            break
        }
        if attempt == maxRetries {
            log.Printf("StreamSend failed: Stream %d not in progress on destination after %d attempts", streamID, maxRetries)
            s.mu.Lock()
            s.streams[streamID].status = pb.Status_FAILED
            s.mu.Unlock()
            return
        }
        time.Sleep(retryDelay)
    }

    stream, err := client.StreamSend(ctx)
    if err != nil {
        log.Printf("Failed to StreamSend on receiving end: %v", err)
        s.mu.Lock()
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    // Fetch data/chunk from memory
    s.mu.Lock()
    memAddr := streamInfo.sendBuffAddr - s.minMemAddr
    if memAddr+streamInfo.numBytes > uint64(len(s.memory)) {
        log.Printf("Memory read out of bounds")
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return
    }
    data := s.memory[memAddr:memAddr+streamInfo.numBytes]
    s.mu.Unlock()

    chunk := &pb.DataChunk{
        Data: data,
    }

    if err := stream.Send(chunk); err != nil {
        log.Printf("Failed to send data chunk: %v", err)
        s.mu.Lock()
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    // Close the stream and receive the response
    resp, err := stream.CloseAndRecv()
    if err != nil || !resp.Success {
        log.Printf("StreamSend failed: %v", err)
        s.mu.Lock()
        s.streams[streamID].status = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    s.mu.Lock()
    s.streams[streamID].status = pb.Status_SUCCESS
    log.Printf("StreamSend succeeded (%d --> %d)", s.deviceId, streamInfo.dstRank)
    s.mu.Unlock()
}
