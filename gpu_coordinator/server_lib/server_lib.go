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
    "io"

    utl "github.com/Ian2x/cs426-dsml/util"

    "go.etcd.io/etcd/client/v3"
)

// GpuCoordinatorServer implements the GPUCoordinator service
type GpuCoordinatorServer struct {
    pb.UnimplementedGPUCoordinatorServer

    // Devices and Communicators
    Devices        map[uint64]*utl.DeviceConfig       // DeviceID to DeviceConfig
    Communicators  map[uint64]*communicator       // CommID to Communicator

    // Other state
    NextCommID     uint64

    // Lock
    Mu             sync.RWMutex     // NOTE: make RWMutex for faster reads?
    // ADDED: timeouts for each device
    DeviceTimeouts map[uint64]time.Time           // DeviceID to timeout
    AddDuration time.Duration
    // ADDED: device health
    DeviceHealth   map[uint64]bool                // DeviceID to health

    EtcdClient *clientv3.Client             // etcd
}

type communicator struct {
    commID       uint64
    rankToDeviceId map[uint32]uint64              // Rank to DeviceID
    groupStarted bool
    opQueue      []operation
    status       pb.Status
    opsCompleted uint64
    deviceMem    map[uint32][]byte          // ADDED: rank to device memory (from client)

    removed      uint32  // ADDED for testing
}

type operation struct {
    opType string
    opReq    interface{}
}

func (s *GpuCoordinatorServer) SaveCheckpoint(ctx context.Context, key string, data string) error {
	_, err := s.EtcdClient.Put(ctx, key, data)
	if err != nil {
		log.Printf("Failed to save checkpoint: %v", err)
		return err
	}
	log.Printf("Checkpoint saved with key: %s", key)
	return nil
}

func (s *GpuCoordinatorServer) GetCheckpoint(ctx context.Context, key string) (string, error) {
	resp, err := s.EtcdClient.Get(ctx, key)
	if err != nil {
		log.Printf("Failed to retrieve checkpoint: %v", err)
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil // Key not found
	}
	return string(resp.Kvs[0].Value), nil
}


func MakeGPUCoordinatorServer() *GpuCoordinatorServer{
    // etcd client
    cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://etcd:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil
	}

    server := GpuCoordinatorServer{
        Devices:        make(map[uint64]*utl.DeviceConfig),
        Communicators:  make(map[uint64]*communicator),
        NextCommID:     1,
        DeviceTimeouts: make(map[uint64]time.Time),
        AddDuration:    1000 * time.Millisecond,
        DeviceHealth:   make(map[uint64]bool),
        EtcdClient:     cli,
    }

    server.Mu.Lock()
    defer server.Mu.Unlock()

    t := time.Now()
    for _, d := range server.Devices {
        // set timeouts
        server.DeviceTimeouts[d.DeviceID] = t.Add(server.AddDuration)
        // set health to true
        server.DeviceHealth[d.DeviceID] = true
    }

    // go server.checkExpired()

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
        rankToDeviceId: make(map[uint32]uint64),
        groupStarted: false,
        opQueue: make([]operation, 0, req.NumDevices * 100),
        status: pb.Status_IN_PROGRESS,
        deviceMem: make(map[uint32][]byte),
        removed: uint32(0xffffffff),
    }

    var devicesMetadata []*pb.DeviceMetadata
    rank := uint32(0)
    for _, device := range s.Devices {
        // Just need req.NumDevices
        if rank >= req.NumDevices {
            break
        }

        // // Assign device to communicator
        // communicator.devices[rank] = device

        // Add device to devicesMetadata (for response)
        devicesMetadata = append(devicesMetadata, &pb.DeviceMetadata{
            DeviceId:   &pb.DeviceId{Value: device.DeviceID},
            MinMemAddr: &pb.MemAddr{Value: device.MinMemAddr},
            MaxMemAddr: &pb.MemAddr{Value: device.MaxMemAddr},
        })

        communicator.rankToDeviceId[rank] = device.DeviceID
        communicator.deviceMem[rank] = []byte{}
        log.Printf("Comm assigning device %d to rank %d", device.DeviceID, rank)
        rank++
    }

    s.Communicators[commID] = communicator

    return &pb.CommInitResponse{
        Success: true,
        CommId:  commID,
        Devices: devicesMetadata,
    }, nil
}

func (s *GpuCoordinatorServer) CommRemoveDevice(ctx context.Context, req *pb.CommRemoveDeviceRequest) (*pb.CommRemoveDeviceResponse, error) {
    s.Mu.Lock()
    defer s.Mu.Unlock()
    // req arguments
    commId := req.CommId
    rank := req.Rank.Value
    // check if comm exists
    comm, exists := s.Communicators[commId]
    if !exists {
        return nil, fmt.Errorf("Communicator %d not found", commId)
    }
    if len(comm.rankToDeviceId) <= 1 {
        return nil, fmt.Errorf("Communicator %d only has %d devices", commId, len(comm.rankToDeviceId))
    }
    comm.removed = rank
    // // remove device rank
    // delete(comm.rankToDeviceId, rank)
    // // get all deviceIds of communicator
    // ids := make([]uint64, 0)
    // for r, id := range comm.rankToDeviceId {
    //     if r != rank {
    //         ids = append(ids, id)
    //     }
    // }
    // // create new rank to id mapping
    // comm.rankToDeviceId = make(map[uint32]uint64)
    // for i := uint32(0); i < uint32(len(ids)); i++ {
    //     comm.rankToDeviceId[i] = ids[i]
    // }
    // // create devicesMetadata
    // var devicesMetadata []*pb.DeviceMetadata
    // for _, deviceId := range comm.rankToDeviceId {
    //     devicesMetadata = append(devicesMetadata, &pb.DeviceMetadata{
    //         DeviceId:   &pb.DeviceId{Value: deviceId},
    //         MinMemAddr: &pb.MemAddr{Value: s.Devices[deviceId].MinMemAddr},
    //         MaxMemAddr: &pb.MemAddr{Value: s.Devices[deviceId].MaxMemAddr},
    //     })
    // }
    // return
    return &pb.CommRemoveDeviceResponse{
        Success: true,
        // Devices: devicesMetadata,
    }, nil
}

func (s *GpuCoordinatorServer) GetCommStatus(ctx context.Context, req *pb.GetCommStatusRequest) (*pb.GetCommStatusResponse, error) {
    s.Mu.RLock()
    defer s.Mu.RUnlock()

    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    return &pb.GetCommStatusResponse{
        Status: communicator.status,
    }, nil
}

func (s *GpuCoordinatorServer) GroupStart(ctx context.Context, req *pb.GroupStartRequest) (*pb.GroupStartResponse, error) {
    s.Mu.RLock()
    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        s.Mu.RUnlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    if communicator.groupStarted {
        s.Mu.RUnlock()
        return nil, fmt.Errorf("Group already started")
    }
    s.Mu.RUnlock()

    s.Mu.Lock()
    defer s.Mu.Unlock()

    communicator.opQueue = nil
    communicator.groupStarted = true
    communicator.status = pb.Status_IN_PROGRESS

    return &pb.GroupStartResponse{
        Success: true,
    }, nil
}

func (s *GpuCoordinatorServer) GroupEnd(ctx context.Context, req *pb.GroupEndRequest) (*pb.GroupEndResponse, error) {
    s.Mu.RLock()
    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        s.Mu.RUnlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }

    if !communicator.groupStarted {
        s.Mu.RUnlock()
        return nil, fmt.Errorf("No group operation started")
    }
    s.Mu.RUnlock()

    s.Mu.Lock()
    defer s.Mu.Unlock()

    // Copy queued ops
    queuedOps := communicator.opQueue

    communicator.groupStarted = false

    // Execute operations in-order, asynchronously
    go s.executeOps(queuedOps, communicator)

    return &pb.GroupEndResponse{
        Success: true,
    }, nil
}

func (s *GpuCoordinatorServer) executeOps(queuedOps []operation, comm *communicator) {
    errCh := make(chan error, len(queuedOps))
    
    for _, op := range queuedOps {
        switch op.opType {
            case "AllReduceRing":
                allReduceRingRequest := op.opReq.(*pb.AllReduceRingRequest)
                if err := s.executeAllReduceRing(allReduceRingRequest); err != nil {
                    return
                    // errCh <- err
                }
            case "AllReduce":
                allReduceRequest := op.opReq.(*pb.AllReduceRequest)
                if err := s.executeAllReduce(allReduceRequest); err != nil {
                    return
                    // errCh <- err
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
            comm.status = pb.Status_FAILED
            s.Mu.Unlock()
            log.Printf("GroupEnd received error: %v", err)
        }
    }
    s.Mu.Lock()
    comm.status = pb.Status_SUCCESS
    s.Mu.Unlock()
}

func (s *GpuCoordinatorServer) AllReduceRing(ctx context.Context, req *pb.AllReduceRingRequest) (*pb.AllReduceRingResponse, error) {
    s.Mu.RLock()
    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        s.Mu.RUnlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }
    s.Mu.RUnlock()

    s.Mu.Lock()
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
    s.Mu.RLock()
    communicator, exists := s.Communicators[req.CommId]
    if !exists {
        s.Mu.RUnlock()
        return nil, fmt.Errorf("Communicator %d not found", req.CommId)
    }
    s.Mu.RUnlock()
    
    s.Mu.Lock()
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
    rankToDeviceId map[uint32]uint64,
    op pb.ReduceOp,
    errCh chan *utl.DeviceError,
    wg *sync.WaitGroup,
) {
    defer wg.Done()
    // create context
    ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
    defer cancel()

    s.Mu.RLock()
    srcClient := clients[srcRank]
    dstClient := clients[dstRank]
    s.Mu.RUnlock()
    // invoke beginSend
    beginSendResp, beginSendErr := srcClient.BeginSend(
        ctx,
        &pb.BeginSendRequest{
            SendBuffAddr: &pb.MemAddr{Value: srcBuffAddr},
            NumBytes: bytesPerReq,
            DstId: &pb.DeviceId{Value: rankToDeviceId[dstRank]},
            ReceiveOp: op,
        },
    )
    if beginSendErr != nil || !beginSendResp.Initiated {
        errCh <- utl.DeviceErrorf("BeginSend not initiated", srcRank)
        return
    }
    // invoke beginReceive
    log.Printf("Invoked send [%d (devid: %d, addr: %d) --> %d (devid: %d, addr: %d)] on streamID %d w/ %d bytes", srcRank, rankToDeviceId[srcRank], srcBuffAddr, dstRank, rankToDeviceId[dstRank], dstBuffAddr, beginSendResp.StreamId.Value, bytesPerReq)
    beginRecvResp, beginRecvErr := dstClient.BeginReceive(
        ctx,
        &pb.BeginReceiveRequest{
            StreamId: beginSendResp.StreamId,
            RecvBuffAddr: &pb.MemAddr{Value: dstBuffAddr},
            NumBytes: bytesPerReq,
            SrcId: &pb.DeviceId{Value: rankToDeviceId[srcRank]},
        },
    )
    if beginRecvErr != nil || !beginRecvResp.Initiated {
        errCh <- utl.DeviceErrorf("BeginReceive not initiated", dstRank)
        return
    }
    // check stream status
    for {
        getStatusResp, getStatusErr := srcClient.GetStreamStatus(
            ctx,
            &pb.GetStreamStatusRequest{
                StreamId: beginSendResp.StreamId,
            },
        )
        if getStatusErr != nil {
            // not sure how to handle error here. maybe just resend?
            errCh <- utl.DeviceErrorf("Stream status FAILED", srcRank)
            return
        }
        switch getStatusResp.Status {
        case pb.Status_SUCCESS:
            return
        case pb.Status_FAILED:
            // handle error
            errCh <- utl.DeviceErrorf("Stream status FAILED", srcRank)
            return
        case pb.Status_IN_PROGRESS:
            // sleep and retry
            time.Sleep(100 * time.Millisecond)
        }
    }
}

func (s *GpuCoordinatorServer) executeAllReduceRing(req *pb.AllReduceRingRequest) error {
    // Get the communicator and number of devices
    s.Mu.RLock()
    communicator := s.Communicators[req.CommId]
    if communicator == nil {
        s.Mu.RUnlock()
        return fmt.Errorf("Communicator %d not found", req.CommId)
    }
    if len(communicator.rankToDeviceId) <= 1 {
        s.Mu.RUnlock()
        return fmt.Errorf("Communicator has less than 2 devices")
    }
    s.Mu.RUnlock()

    s.Mu.Lock()
    deviceClients := make(map[uint32]pb.GPUDeviceClient)
    conns := make(map[uint32]*grpc.ClientConn)
    numDevices := uint32(len(communicator.rankToDeviceId))

    opts := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }

    // for rank, deviceConfig := range communicator.devices {
    for rank, deviceId := range communicator.rankToDeviceId {
        // target := fmt.Sprintf("%s:%d", deviceConfig.IPAddress, deviceConfig.Port)
        config := s.Devices[deviceId]
        target := fmt.Sprintf("%s:%d", config.IPAddress, config.Port)
        conn, err := grpc.NewClient(target, opts...)
        if err != nil {
            s.Mu.Unlock()
            return fmt.Errorf("could not create gRPC client for rank=%d device=%d: %v", rank, deviceId, err)
        }
        conns[rank] = conn
        deviceClients[rank] = pb.NewGPUDeviceClient(conn)
    }
    s.Mu.Unlock()

    // defer close connections
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

    s.Mu.RLock()
    for rank := range communicator.rankToDeviceId {
        // read in device memory and store it
        stream, err := deviceClients[rank].MemcpyDeviceToHost(
            context.Background(),
            &pb.MemcpyDeviceToHostRequest{
                SrcDeviceId: &pb.DeviceId{Value: communicator.rankToDeviceId[rank]},
                SrcMemAddr: memAddrs[rank],
                NumBytes: numBytes,
            },
        )
        if err != nil {
            fmt.Printf("Error starting MemcpyDeviceToHost: %v\n", err)
            return err
        }
        var result []byte
        for {
            chunk, err := stream.Recv()
            if err == io.EOF {
                break
            }
            if err != nil {
                fmt.Printf("Error receiving data chunk: %v\n", err)
                return err
            }
            result = append(result, chunk.Data...)
        }
        communicator.deviceMem[rank] = result
    }
    s.Mu.RUnlock()

    // Error channel for both phases
    errCh := make(chan *utl.DeviceError, 2 * (numDevices - 1))
    
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
                offset := bytesPerReq * uint64((srcRank + numDevices - i + phase) % numDevices) % numBytes
                srcBuffAddr := uint64(memAddrs[srcRank].Value + offset)
                dstBuffAddr := uint64(memAddrs[dstRank].Value + offset)
                
                // FOR TESTING
                if dstRank == communicator.removed {
                    log.Printf("Device %d removed from cluster (testing)", dstRank)
                    errCh <- utl.DeviceErrorf("Device removed from cluster (testing)", dstRank)
                } else {
                    // invoke beginShare
                    wg.Add(1)
                    go s.beginShare(
                        srcRank, srcBuffAddr,
                        dstRank, dstBuffAddr,
                        bytesPerReq, deviceClients,
                        communicator.rankToDeviceId,
                        reduceOp, errCh, &wg,
                    )
                }
            }
            wg.Wait()

            // empty error channel and read errors
            errs := make([]*utl.DeviceError, 0)
            DrainErrors:
            for {
                select {
                case err := <-errCh:
                    errs = append(errs, err)
                default:
                    break DrainErrors
                }
            }
            if len(errs) > 0 {
                close(errCh)
                s.handleErrors(errs, communicator)
                return fmt.Errorf("At least one device unresponsive... Restarting.")
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
    numDevices := uint32(len(communicator.rankToDeviceId))

    opts := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }

    for rank, deviceId := range communicator.rankToDeviceId {
        deviceConfig := s.Devices[deviceId]
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
    errCh := make(chan *utl.DeviceError, numDevices * (numDevices - 1))

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
                communicator.rankToDeviceId,
                op, errCh, &wg,
            )
        }
    }

    wg.Wait()

    // empty error channel and read errors
    errs := make([]*utl.DeviceError, 0)
    DrainErrors:
    for {
        select {
        case err := <-errCh:
            errs = append(errs, err)
        default:
            break DrainErrors
        }
    }
    s.handleErrors(errs, communicator)

    close(errCh)

    return nil
}

// NOTE: need to ensure that previous call to groupEnd (where the
// original goroutine with the loop over queuedOps) has terminated
func (s *GpuCoordinatorServer) handleErrors(errs []*utl.DeviceError, comm *communicator) {
    s.Mu.Lock()
    for _, err := range errs {
        id := comm.rankToDeviceId[err.Rank]
        // set unhealthy
        s.DeviceHealth[id] = false
        // remove from communicator
        log.Printf("Would remove unhealthy %d from communicator", id)
        delete(comm.rankToDeviceId, err.Rank)
    }
    // get all deviceIds of communicator
    ids := make([]uint64, 0)
    for _, id := range comm.rankToDeviceId {
        ids = append(ids, id)
    }
    // create new rank to id mapping
    comm.rankToDeviceId = make(map[uint32]uint64)
    for i := uint32(0); i < uint32(len(ids)); i++ {
        comm.rankToDeviceId[i] = ids[i]
    }
    // update opQueue based on ops completed
    comm.opQueue = comm.opQueue[comm.opsCompleted:]
    // restart execution
    queuedOps := comm.opQueue
    comm.removed = uint32(0xffffffff)
    go s.executeOps(queuedOps, comm)
    s.Mu.Unlock()
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
    s.DeviceTimeouts[id] = time.Now().Add(s.AddDuration)
    // return
    return &pb.HeartbeatResponse{Success: true}, nil
}

// goroutine to check if timeouts are expired
func (s *GpuCoordinatorServer) checkExpired() {
    for {
        // lock
        s.Mu.Lock()
        // list of expired devices
        expired := make([]uint64, 0)
        // check timeouts of devices
        t := time.Now()
        for _, d := range s.Devices {
            if s.DeviceTimeouts[d.DeviceID].After(t) {
                expired = append(expired, d.DeviceID)
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