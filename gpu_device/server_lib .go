package main

import (
    "context"
    pb "../proto"
    "sync"
    "sync/atomic"
    "metadata"
    "strconv"
    "time"
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
    peers               map[uint32]*peerInfo
    nextStreamIndex     uint64
    streams             map[uint64]*streamInfo
    isHealthy           bool
    opQueue             chan *operation

    // Lock
    mu sync.Mutex
}

type operation struct {
    opType      string // "send" or "recv"
    streamID    uint64
}

type peerInfo struct {
    ipAddress  string
    port       uint64
}

type streamInfo struct {
    status        pb.Status
    sendBuffAddr  uint64
    recvBuffAddr  uint64
    numBytes      uint64
    srcRank       uint64
    dstRank       uint64
}

func MakeGPUDeviceServer(deviceID uint64, peers map[uint32]*peerInfo) *gpuDeviceServer{
    server := gpuDeviceServer{
        deviceId:           deviceID,
        minMemAddr:         0,
        maxMemAddr:         1024 * 1024, // 1 MB memory
        memory:             make([]byte, 1024*1024),
        peers:              peers, // Read in from config files
        nextStreamIndex:    1,
        streams:            make(map[uint64]*streamInfo),
        isHealthy:          true,
        opQueue:            make(chan *operation, 100), // Queue up to 100 operations
    }

    // Start processing operations
    go server.processOperations()

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
    streamID := (s.deviceID << 32) | s.nextStreamIndex // Kinda rough
    s.nextStreamIndex++

    // Store & add streamInfo
    s.streams[streamId] = &streamInfo{
        status: pb.Status_IN_PROGRESS,
        sendBuffAddr: req.SendBuffAddr.Value,
        numBytes: req.NumBytes,
        dstRank: req.DstRank.Value,
    }

    // Queue the operation
    op := &operation{
        opType: "send",
        streamID: streamID,
    }
    s.opQueue <- op

    // Return BeginSendResponse
    return &pb.BeginSendResponse{
        Initiated: true,
        StreamId:  &pb.StreamId{Value: streamId},
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
        streamID: streamID,
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
    streamIDs := md.Get("streamid")
    if len(streamIDs) == 0 {
        return fmt.Errorf("Missing streamid from StreamSend metadata")
    }
    streamID, err := strconv.ParseUint(streamIDs[0], 10, 64)
    if err != nil {
        return fmt.Errorf("Invalid streamID from StreamSend metadata")
    }

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
        s.streams[streamID] = pb.Status_FAILED
        s.mu.Unlock()
        return fmt.Errorf("Failed to receive stream: %v", err)
    }

    data := message.Data
    dataLen := uint64(len(data))

    // Check size of received data
    if dataLen != streamInfo.numBytes {
        s.mu.Lock()
        s.streams[streamID] = pb.Status_FAILED
        s.mu.Unlock()
        return fmt.Errorf("Expected %d bytes, received %d bytes", streamInfo.numBytes, dataLen)
    }

    // Write data to memory and send response
    s.mu.Lock()
    defer s.mu.Unlock()
    memAddr := streamInfo.recvBuffAddr - s.minMemAddr
    if memAddr+dataLen > uint64(len(s.memory)) {
        s.streams[streamID] = pb.Status_FAILED
        return errors.New("Memory write out of bounds")
    }
    copy(s.memory[memAddr:memAddr+dataLen], data)
    s.streams[streamID] = pb.Status_SUCCESS

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
                        s.handleSendOperation(op.streamID)
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
func (s *gpuDeviceServer) handleSendOperation(streamID uint64) {
    s.mu.Lock()
    streamInfo, exists := s.streams[streamID]
    s.mu.Unlock()
    if !exists {
        log.Printf("Stream %d not found", streamID)
        return
    }

    // Get destination peerInfo
    dstPeer, exists := s.peers[streamInfo.dstRank]
    if !exists {
        log.Printf("Destination rank %d not found", streamInfo.dstRank)
        s.mu.Lock()
        s.streams[streamID] = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    // Get address of dstPeer
    dstAddress := fmt.Sprintf("%s:%d", dstPeer.ipAddress, dstPeer.port)

    // Create a gRPC client to dstAddress
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient(dstAddress, opts...)
    if err != nil {
        log.Printf("Failed to connect: %v", err)
        s.mu.Lock()
        s.streams[streamID] = pb.Status_FAILED
        s.mu.Unlock()
        return
    }
    defer conn.Close()

    client := pb.NewGPUDeviceClient(conn)

    // Store streamId in context
    md := metadata.Pairs("streamid", fmt.Sprintf("%d", streamID))
    ctx := metadata.NewOutgoingContext(context.Background(), md)
    stream, err := client.StreamSend(ctx)
    if err != nil {
        log.Printf("Failed to StreamSend on receiving end: %v", err)
        s.mu.Lock()
        s.streams[streamID] = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    // Fetch data/chunk from memory
    s.mu.Lock()
    memAddr := streamInfo.sendBuffAddr - s.minMemAddr
    if memAddr+streamInfo.numBytes > uint64(len(s.memory)) {
        log.Printf("Memory read out of bounds")
        s.streams[streamID] = pb.Status_FAILED
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
        s.streams[streamID] = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    // Close the stream and receive the response
    resp, err := stream.CloseAndRecv()
    if err != nil || !resp.Success {
        log.Printf("StreamSend failed: %v", err)
        s.mu.Lock()
        s.streams[streamID] = pb.Status_FAILED
        s.mu.Unlock()
        return
    }

    s.mu.Lock()
    s.streams[streamID] = pb.Status_SUCCESS
    s.mu.Unlock()
}
