# Modified Raft.py with Leader Lease using gRPC

This project implements a modified version of the Raft consensus algorithm with Leader Lease using gRPC in Python.

## Installation

### Prerequisites
- Python 3.6 or higher

### Installation
Clone the repository
```bash
git clone https://github.com/utkar22/DSCD-A2-G1.git
```

```bash
chmod +x setup.sh
```

```bash
./setup.sh
```

```bash
chmod +x clean_logs.sh
```

## Usage

To clean up any previous logs
```bash
./clean_logs.sh
```

To start a Raft node:
```bash
python3 RaftNode.py
```

Run the above command in 5 different terminals, and enter the following server id in each terminal:
- 50051
- 50052
- 50053
- 50054
- 50055


To start a Raft client:
```bash
python3 RaftClient.py
```

The client can send the following commands to the Raft cluster:
- `SET key value` to set a key-value pair
- `GET key` to get the value of a key