#!/bin/bash

sudo pip3 install -r requirements.txt
sudo python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto