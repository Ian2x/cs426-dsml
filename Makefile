.PHONY: all clean build_device build_coordinator build_client run_device run_coordinator run_client create_network

all: build_device build_coordinator build_client

build_device:
	docker build -t gpu_device -f gpu_device/Dockerfile .

build_coordinator:
	docker build -t gpu_coordinator ./gpu_coordinator

build_client:
	go build -o client_app/client_app ./client_app

create_network:
	docker network create gpu_network || true  # Prevent error if it already exists

run_device: create_network
	docker run --network=gpu_network -p 8081:8081 --name gpu_device gpu_device

run_coordinator: create_network
	docker run --network=gpu_network -p 8082:8082 --name gpu_coordinator gpu_coordinator

run_client:
	./client_app/client_app
