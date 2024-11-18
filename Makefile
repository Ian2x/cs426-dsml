.PHONY: all clean run_device run_coordinator run_client

all: build_device build_coordinator build_client

build_device:
    docker build -t gpu_device ./gpu_device

build_coordinator:
    docker build -t gpu_coordinator ./gpu_coordinator

build_client:
    go build -o client_app/client_app ./client_app

run_device:
    docker run -p 8081:8081 gpu_device

run_coordinator:
    docker run -p 8082:8082 gpu_coordinator

run_client:
    ./client_app/client_app
