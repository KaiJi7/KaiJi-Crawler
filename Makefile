.PHONY: build

image:
	docker-compose -f ./build/docker-compose.yaml build
