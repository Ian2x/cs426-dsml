package server_lib

import (
    "context"
    pb "github.com/Ian2x/cs426-dsml/proto"
    "sync"
    "fmt"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"
)

// GpuCoordinatorServer implements the GPUCoordinator service
type GpuCoordinatorServer struct {
    pb.UnimplementedGPUCoordinatorServer

    // Devices and Communicators
    Devices        map[uint64]*DeviceConfig       // DeviceID to DeviceConfig
    RankToDeviceID map[uint32]uint64              // Rank to DeviceID
    Communicators  map[uint64]*communicator       // CommID to Communicator

    // Other state
    NextCommID     uint64

    // Lock
    Mu             sync.Mutex
}

type DeviceConfig struct {
    DeviceID   uint64
    IpAddress  string
    Port       uint64
    MinMemAddr uint64
    MaxMemAddr uint64
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

func MakeGPUCoordinatorServer() *GpuCoordinatorServer{
    server := GpuCoordinatorServer{
        Devices:        make(map[uint64]*DeviceConfig),
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
        devices: make(map[uint32]*DeviceConfig),
        groupStarted: false,
        opQueue: make([]operation, req.NumDevices * 100),
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
    for _, op := range queuedOps {
        switch op.opType {
            case "AllReduceRing":
                allReduceRingRequest := op.opReq.(*pb.AllReduceRingRequest)
                if _, err := s.executeAllReduceRing(allReduceRingRequest); err != nil {
                    return nil, err
                }
            // Other operations
            default:
                return nil, fmt.Errorf("Unknown operation type: %v", op.opType)
        }
    }

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
        if _, err := s.executeAllReduceRing(req); err != nil {
            return nil, err
        }
    }

    return &pb.AllReduceRingResponse{
        Success: true,
    }, nil
}

func (s *GpuCoordinatorServer) executeAllReduceRing(req *pb.AllReduceRingRequest) (*pb.AllReduceRingResponse, error) {
    // TODO (may need to change parameters and return types)
    return nil, nil
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
    conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", deviceInfo.IpAddress, deviceInfo.Port), opts...)
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
    conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", deviceInfo.IpAddress, deviceInfo.Port), opts...)
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