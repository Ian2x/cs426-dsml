package main

import (
    "context"
    pb "github.com/Ian2x/cs426-dsml/proto"
    "sync"
    "errors"
    "fmt"
    "google.golang.org/grpc"
)

// gpuCoordinatorServer implements the GPUCoordinator service
type gpuCoordinatorServer struct {
    pb.UnimplementedGPUCoordinatorServer

    // Devices and Communicators
    devices        map[uint64]*DeviceConfig       // DeviceID to DeviceConfig
    rankToDeviceID map[uint32]uint64              // Rank to DeviceID
    communicators  map[uint64]*communicator       // CommID to Communicator

    // Other state
    nextCommID     uint64

    // Lock
    mu             sync.Mutex
}

type DeviceConfig struct {
    deviceID   uint64
    ipAddress  string
    port       uint64
    minMemAddr uint64
    maxMemAddr uint64
}

type communicator struct {
    commID       uint64
    devices      map[uint32]*DeviceConfig // Rank to DeviceConfig
    groupStarted bool
    opQueue      []operation
    status       pb.Status
}

type operation struct {
    opType string
    opReq    interface{}
}

func MakeGPUCoordinatorServer() *gpuCoordinatorServer{
    server := gpuCoordinatorServer{
        devices:        make(map[uint64]*DeviceConfig),
        rankToDeviceID: make(map[uint32]uint64),
        communicators:  make(map[uint64]*communicator),
        nextCommID:     1,
    }

    return &server
}

func (s *gpuCoordinatorServer) CommInit(ctx context.Context, req *pb.CommInitRequest) (*pb.CommInitResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if req.NumDevices == 0 {
        return nil, fmt.Errorf("Need > 0 devices in comunicator")
    }

    if uint32(len(s.devices)) < req.NumDevices {
        return nil, fmt.Errorf("Not enough devices for communicator")
    }

    // Get new commID
    commID := s.nextCommID
    s.nextCommID++

    // Select devices for communicator
    communicator := &communicator{
        commID:  commID,
        devices: make(map[uint32]*DeviceConfig),
        groupStarted: false,
        opQueue: make([]operation, req.NumDevices * 100),
        status: pb.Status_IN_PROGRESS,
    }

    var devicesMetadata []*pb.DeviceMetadata
    rank := uint32(0)
    for _, device := range s.devices {
        // Just need req.numDevices
        if rank >= req.numDevices {
            break
        }

        // Assign device to communicator
        communicator.devices[rank] = device

        // Add device to devicesMetadata (for response)
        devicesMetadata = append(devicesMetadata, &pb.DeviceMetadata{
            DeviceId:   &pb.DeviceId{Value: device.deviceID},
            MinMemAddr: &pb.MemAddr{Value: device.minMemAddr},
            MaxMemAddr: &pb.MemAddr{Value: device.maxMemAddr},
        })

        s.rankToDeviceID[rank] = device.DeviceID
        rank++
    }

    s.communicators[commID] = communicator

    return &pb.CommInitResponse{
        Success: true,
        CommId:  commID,
        Devices: devicesMetadata,
    }, nil
}

func (s *gpuCoordinatorServer) GetCommStatus(ctx context.Context, req *pb.GetCommStatusRequest) (*pb.GetCommStatusResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    communicator, exists := s.communicators[req.commID]
    if !exists {
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    return &pb.GetCommStatusResponse{
        Status: communicator.status,
    }, nil
}

func (s *gpuCoordinatorServer) GroupStart(ctx context.Context, req *pb.GroupStartRequest) (*pb.GroupStartResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    communicator, exists := s.communicators[req.CommID]
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

func (s *gpuCoordinatorServer) GroupEnd(ctx context.Context, req *pb.GroupEndRequest) (*pb.GroupEndResponse, error) {
    s.mu.Lock()

    communicator, exists := s.communicators[req.CommId]
    if !exists {
        s.mu.Unlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    if !communicator.groupStarted {
        s.mu.Unlock()
        return nil, fmt.Errorf("No group operation started")
    }

    // Copy queued ops
    queuedOps := communicator.opQueue

    // Clear opQueue and reset for next group
    communicator.opQueue = nil
    communicator.groupStarted = false
    s.mu.Unlock()

    // Execute operations asynchronously
    for _, op := range queuedOps {
        switch op.opType {
            case "AllReduceRing":
                allReduceRingRequest := op.opReq.(*pb.AllReduceRingRequest)
                if _, err := s.executeAllReduceRing(req); err != nil {
                    return nil, err
                }
            // Other operations
            default:
                return nil, fmt.Errorf("Unknown operation type: %v", op.OpType)
        }
    }

    return &pb.GroupEndResponse{
        Success: true,
    }, nil
}

func (s *gpuCoordinatorServer) AllReduceRing(ctx context.Context, req *pb.AllReduceRingRequest) (*pb.AllReduceRingResponse, error) {
    s.mu.Lock()

    communicator, exists := s.communicators[req.CommId]
    if !exists {
        s.mu.Unlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }
    
    if communicator.groupStarted { // If groupStarted, queue the operation.
        communicator.opQueue = append(communicator.opQueue, operation{
            opType: "AllReduceRing",
            opReq:  req,
        })
        s.mu.Unlock()
    } else { // Else, executed immediately
        s.mu.Unlock()
        if _, err := s.executeAllReduceRing(req); err != nil {
            return nil, err
        }
    }

    return &pb.AllReduceRingResponse{
        Success: true,
    }, nil
}

func (s *gpuCoordinatorServer) executeAllReduceRing(req *pb.AllReduceRingRequest) (*pb.AllReduceRingResponse, error) {
    // TODO (may need to change parameters and return types)
}

func (s *gpuCoordinatorServer) Memcpy(ctx context.Context, req *pb.MemcpyRequest) (*pb.MemcpyResponse, error) {
    switch op := req.Either.(type) {
        case *pb.MemcpyRequest_HostToDevice:
            return s.handleMemcpyHostToDevice(ctx, op.HostToDevice)
        case *pb.MemcpyRequest_DeviceToHost:
            return s.handleMemcpyDeviceToHost(ctx, op.DeviceToHost)
        default:
            return nil, fmt.Errorf("Invalid Memcpy operation")
    }
}

func (s *gpuCoordinatorServer) handleMemcpyHostToDevice(ctx context.Context, req *pb.MemcpyHostToDeviceRequest) (*pb.MemcpyResponse, error) {
    s.mu.Lock()
    deviceInfo, exists := s.devices[req.DstDeviceId.Value]
    if !exists {
        s.mu.Unlock()
        return nil, fmt.Errorf("Destination device not found")
    }
    s.mu.Unlock()

    // Connect to destination device
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", deviceInfo.ipAddress, deviceInfo.port), opts...)
    if err != nil {
        return nil, err
    }
    defer conn.Close()

    client := pb.NewGPUDeviceClient(conn)

    // Send data to device
    data := req.HostSrcData

    chunk := &pb.DataChunk{
        Data: data[i:end],
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

func (s *gpuCoordinatorServer) handleMemcpyDeviceToHost(ctx context.Context, req *pb.MemcpyDeviceToHostRequest) (*pb.MemcpyResponse, error) {
    s.mu.Lock()
    deviceInfo, exists := s.devices[req.SrcDeviceId.Value]
    if !exists {
        s.mu.Unlock()
        return nil, fmt.Errorf("Source device not found")
    }
    s.mu.Unlock()

    // Connect to source device
    var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", deviceInfo.ipAddress, deviceInfo.port), opts...)
    if err != nil {
        return nil, err
    }
    defer conn.Close()

    client := pb.NewGPUDeviceClient(conn)

    // Retrieve data from device
    stream, err := client.MemcpyDeviceToHost(context.Background(), &pb.MemcpyDeviceToHostRequest{
        SrcDeviceId: req.SrcDeviceId
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