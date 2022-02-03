#! /bin/bash

docker stop replica3
docker rm replica3
docker stop replica2
docker rm replica2
docker stop replica1
docker rm replica1
docker system prune -f
docker network create --subnet=10.10.0.0/16 mynet
docker build -t assignment3-img .
docker run -d -p 8082:8085 --net=mynet --ip=10.10.0.2 --name="replica1" -e SOCKET_ADDRESS="10.10.0.2:8085" -e VIEW="10.10.0.2:8085,10.10.0.3:8085,10.10.0.4:8085" assignment3-img
sleep 2
docker run -d -p 8083:8085 --net=mynet --ip=10.10.0.3 --name="replica2" -e SOCKET_ADDRESS="10.10.0.3:8085" -e VIEW="10.10.0.2:8085,10.10.0.3:8085,10.10.0.4:8085" assignment3-img
sleep 2
docker run -d -p 8084:8085 --net=mynet --ip=10.10.0.4 --name="replica3" -e SOCKET_ADDRESS="10.10.0.4:8085" -e VIEW="10.10.0.2:8085,10.10.0.3:8085,10.10.0.4:8085" assignment3-img 
sleep 2