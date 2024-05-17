import grpc
import raft_pb2
import raft_pb2_grpc
import random
import time

class RaftClient:
    """
    Represents a client for the Raft consensus algorithm.

    Args:
        servers (dict): A dictionary containing the server IDs and their corresponding addresses.

    Attributes:
        servers (dict): A dictionary containing the server IDs and their corresponding addresses.
        leader_id (str): The ID of the current leader server.

    Methods:
        send_request(request): Sends a request to the Raft servers.
        run(): Executes the main logic for the Raft client.
    """

    def __init__(self, servers):
        self.servers = servers
        self.leader_id = random.choice(list(self.servers.keys()))

    def send_request(self, request):
        """
        Sends a request to the Raft servers.

        Args:
            request (str): The request to be sent.

        Returns:
            None
        """
        # Logic to send request to Raft servers

        if len(request.split()) not in (2,3):
            print("Invalid request!")
            return
        if request.split()[0] not in ('GET', 'SET'):
            print("Invalid request!")
            return
        if len(request.split()) == 3 and request.split()[0] == 'GET':
            print("Invalid request!")
            return
        if len(request.split()) == 2 and request.split()[0] == 'SET':
            print("Invalid request!")
            return

        while True:
            try:
                client_request = raft_pb2.ClientRequest(Request=request)
                channel = grpc.insecure_channel(self.servers[self.leader_id])
                stub = raft_pb2_grpc.RaftNodeStub(channel)

                try:
                    response = stub.ServeClient(client_request)
                except:
                    print(self.leader_id, " is down. Trying another server.")
                    self.leader_id = random.choice(list(self.servers.keys()))
                    continue

                if response.Success:
                    print("Request sent successfully!")
                    print("Response from server: ", response.Data)
                    if response.LeaderID:
                        self.leader_id = response.LeaderID
                    break
                else:
                    if response.Data == "Leader is down":
                        print("Leader is down. Trying another server.")
                        self.leader_id = random.choice(list(self.servers.keys()))
                        time.sleep(10)
                        continue
                    if response.Data == "No leader available":
                        self.leader_id = random.choice(list(self.servers.keys()))
                        time.sleep(10)
                    if response.Data == "Invalid request!":
                        print("Invalid request!")
                        break
                    if response.LeaderID:
                        self.leader_id = response.LeaderID
                        continue
                    else:
                        continue
            except Exception as e:
                print("Error. Please retry.")
                break

    def run(self):
        """
        Executes the main logic for the Raft client.

        Returns:
            None
        """
        while True:
            query = input("Enter your query: ")
            self.send_request(query)
        

if __name__ == '__main__':
    servers = {'50051':'localhost:50051',
               '50052': 'localhost:50052',
               '50053': 'localhost:50053',
               '50054': 'localhost:50054',
               '50055': 'localhost:50055',
               }
    client = RaftClient(servers)
    client.run()