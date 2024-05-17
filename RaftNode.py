import datetime
import json
import json
import grpc
import raft_pb2
import raft_pb2_grpc
import time
import random
import threading
import os
from concurrent import futures


LEASE_DURATION = 20     #Duration for which leader lease is acquired
ELECTION_DURATION = 10  #Duration for which election is held
HEARTBEAT_WAIT = 25     #Duration for which follower waits for heartbeat before starting election
WRITE_INTERVAL = 25     #Interval for writing metadata and log files
LEADER_SLEEP = 10       #Interval for leader routine
FOLLOWER_SLEEP = 10     #Interval for follower routine

def datetime_to_timestamp(dt):
    """
    Converts a datetime object to a Unix timestamp.

    Args:
        dt (datetime): The datetime object to convert.

    Returns:
        int: The Unix timestamp representing the given datetime object.
            Returns None if the input datetime is None.
    """
    if dt is None:
        return None
    return int(dt.timestamp())

def timestamp_to_datetime(timestamp):
    """
    Converts a Unix timestamp to a datetime object.
    
    Args:
        timestamp (float): The timestamp to convert.
        
    Returns:
        datetime.datetime: The datetime object representing the timestamp.
    """
    if timestamp is None:
        return None
    return datetime.datetime.fromtimestamp(timestamp)

class LogEntry:
    """
    Represents a log entry in the Raft consensus algorithm.

    Attributes:
        term (int): The term in which the log entry was created.
        command (str): The command associated with the log entry.
    """   
    def __init__(self, term, command): 
        self.term = term
        self.command = command

class RaftNode:
    '''
    Represents a Node in the Raft Consensus Algorithm

    Args:
        server_id (str): The unique identifier of the server.
        other_servers (dict): A dictionary of other servers in the network.
    '''
    def __init__(self, server_id, other_servers):
        n = len(other_servers)

        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.committed_log = []
        self.commit_index = 0
        self.commit_length = 0
        self.role = 'follower'
        self.leader = None
        self.votes_recieved = set()
        self.acked_length = {}
        self.sent_length = {}
        self.server_id = server_id
        self.database = {}
        self.is_leader = False


        self.is_token = False
        self.lease_end = None
        self.recovery()

        self.lease_lock = threading.Lock()

        self.other_servers = other_servers

        self.lock = threading.Lock()

        self.election_end = None
        
        self.last_heartbeat = None
        
        self.lease_ack = 0
        self.lease_ack_lock = threading.Lock()

        self.heartbeat_lock = threading.Lock()
        self.dump_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.database_lock = threading.Lock()

        self.election_timeout = False

        leader_routine_timer = threading.Timer(random.randint(4,10), self.leader_routine)
        leader_routine_timer.start()

        follower_routine_timer = threading.Timer(random.randint(30,40), self.follower_routine)
        follower_routine_timer.start()

        node_routine_timer = threading.Timer(random.randint(10,20), self.node_routine)
        node_routine_timer.start()

    def recovery(self):
        """
        Performs the recovery process for the Raft node.

        This method creates necessary folders and files for logging and metadata storage.
        If the log file, metadata file, and dump file already exist, it reads their contents.
        If they don't exist, it creates empty files.

        Returns:
            None
        """
        logs_folder = 'logs'
        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)

        node_logs_folder = 'logs/node_' + self.server_id
        if not os.path.exists(node_logs_folder):
            os.makedirs(node_logs_folder)

        self.log_file_path = node_logs_folder + '/log.txt'
        self.metadata_file_path = node_logs_folder + '/metadata.txt'
        self.dump_file_path = node_logs_folder + '/dump.txt'

        if not os.path.exists(self.log_file_path):
            self.create_empty_file(self.log_file_path)
        else:
            self.read_log_file()

        if not os.path.exists(self.metadata_file_path):
            self.create_empty_file(self.metadata_file_path)
        else:
            self.read_metadata_file()

        if not os.path.exists(self.dump_file_path):
            self.create_empty_file(self.dump_file_path)

    def create_empty_file(self, path):
        """
        Creates an empty file at the specified path.

        Args:
            path (str): The path to create the file.

        Returns:
            None
        """
        with open(path, 'w') as f:
            f.write('')

    def read_log_file(self):
        """
        Reads the log file and populates the log, committed_log, and database.

        This method reads the log file specified by `self.log_file_path` and populates
        the `log`, `committed_log`, and `database` attributes of the RaftNode object.

        Returns:
            None
        """
        try:
            with open(self.log_file_path, 'r') as f:
                lines = f.readlines()
                if len(lines) == 0:
                    return
                with self.log_lock:
                    for line in lines:
                        line_arr = line.split()
                        term = int(line_arr[-1])
                        command = ' '.join(line_arr[:-1])
                        log_entry = LogEntry(term, command)
                        self.log.append(log_entry)
                        self.committed_log.append(log_entry)
                        self.update_database(command)
        except FileNotFoundError:
            self.create_empty_file(self.log_file_path)

    def read_metadata_file(self):
        """
        Reads the metadata file and updates the state of the Raft node based on the contents of the file.

        Returns:
            None
        """
        try:
            with open(self.metadata_file_path, 'r') as f:
                if os.path.getsize(self.log_file_path) == 0:
                    return
                state = json.load(f)

            self.current_term = state['current_term']
            self.voted_for = state['voted_for']
            self.commit_index = state['commit_index']
            self.commit_length = state['commit_length']
            self.leader = state['leader']
            self.role = state['role']
            self.acked_length = state['acked_length']
            self.sent_length = state['sent_length']

            if self.role == 'leader':
                self.is_leader = True

            self.lease_end = timestamp_to_datetime(state['lease_end'])

            if (state['is_token'] and self.lease_end is not None and datetime.datetime.now() > self.lease_end):
                self.is_token = True
            else:
                self.is_token = False

            self.dump(f"Node {self.server_id} recovered state from metadata file")
        except FileNotFoundError:
            self.create_empty_file(self.metadata_file_path)
        

    def write_log_file(self):
        """
        Writes the log entries to a file.

        This method writes the log entries stored in the `log` attribute to the file specified by `log_file_path`.
        Each log entry is written as a line in the file, with the format: "<command> <term>".

        Returns:
            None
        """
        try:
            with self.log_lock:
                with open(self.log_file_path, 'w') as f:
                    for entry in self.log:
                        f.write(entry.command + ' ' + str(entry.term) + '\n')
        except FileNotFoundError:
            self.create_empty_file(self.log_file_path)

    def write_metadata_file(self):
        """
        Writes the current state of the RaftNode object to a metadata file.

        The state includes the current term, voted for candidate, commit index,
        commit length, leader, token status, lease end time, role, acked length,
        and sent length.

        The state is written to a JSON file specified by the `metadata_file_path`
        attribute of the RaftNode object.

        Returns:
            None
        """
        try:
            state = {
                'current_term': self.current_term,
                'voted_for': self.voted_for,
                'commit_index': self.commit_index,
                'commit_length': self.commit_length,
                'leader': self.leader,
                'is_token': self.is_token,
                'lease_end': datetime_to_timestamp(self.lease_end),
                'role': self.role,
                'acked_length': self.acked_length,
                'sent_length': self.sent_length
            }

            with open(self.metadata_file_path, 'w') as f:
                json.dump(state, f)
        except FileNotFoundError:
            self.create_empty_file(self.metadata_file_path)

    def dump(self, request):
        """
        Write the given request to the dump file.

        Parameters:
        - request (str): The request to be written to the dump file.

        Returns:
        None
        """
        try:
            with open(self.dump_file_path, 'a') as f:
                f.write(request + '\n')
        except FileNotFoundError:
            self.create_empty_file(self.dump_file_path)

    def leader_routine(self):
        """
        Executes the routine for a leader node.

        This method is responsible for performing the tasks of a leader node in the Raft consensus algorithm.
        It continuously checks if the current node is the leader and performs the following actions:
        - Sends heartbeat and renews the lease.
        - Replicates all logs.
        - Commits log entries.
        - Checks if the node is a token holder and if the lease has expired. If so, it steps down as a leader.

        This method runs in an infinite loop with a sleep interval of 10 seconds.

        Returns:
            None
        """
        while True:
            if self.role == 'leader':
                with self.lease_ack_lock:
                    self.lease_ack = 1
                self.dump(f"Leader {self.server_id} sending heartbeat & Renewing Lease")
                self.replicate_all_logs()
                self.commit_log_entries()
                if self.is_token and self.lease_end is not None and datetime.datetime.now() > self.lease_end:
                    self.dump(f"Leader {self.server_id} lease renewal failed. Stepping Down.")
                    thread = threading.Thread(target=self.step_down)
                    thread.start()
            time.sleep(LEADER_SLEEP)

    def follower_routine(self):
        """
        Executes the routine for a follower node in the Raft consensus algorithm.
        
        This method continuously checks if the node is in the 'follower' role and if it has received a heartbeat from the leader.
        If no heartbeat is received within a certain time period, the node starts an election by calling the 'start_election' method.
        The method sleeps for 10 seconds between each check.
        """
        while True:
            if (self.role == 'follower'):
                if (self.last_heartbeat == None or
                    datetime.datetime.now() - self.last_heartbeat > datetime.timedelta(seconds=HEARTBEAT_WAIT)):
                    print(f"{self.server_id} did not receive heartbeat from leader")
                    time.sleep(random.randint(2,7))
                    thread = threading.Thread(target=self.start_election)
                    thread.start()
            time.sleep(FOLLOWER_SLEEP)

    def node_routine(self):
        """
        Performs the routine tasks of a Raft node.
        
        This method is responsible for executing the routine tasks of a Raft node, such as writing the log file and metadata file.
        It also includes a sleep interval to control the frequency of these tasks.
        """
        while True:
            self.write_log_file()
            self.write_metadata_file()
            time.sleep(WRITE_INTERVAL)

    def step_down(self):
        """
        Steps down from the leader role and transitions to the follower role.
        Resets the leader, voted_for, votes_received, acked_length, and sent_length attributes.
        Starts a new election after stepping down.

        Returns:
            None
        """
        self.role = 'follower'
        self.leader = None
        self.voted_for = None
        self.votes_received = set()
        self.acked_length = {}
        self.sent_length = {}
        self.dump(f"Node {self.server_id} stepping down")
        self.start_election()

    def cancel_election_timer(self):
        """
        Cancels the election timer.
        
        This method is responsible for canceling the election timer by setting the `election_end` attribute to None.
        """
        self.election_end = None

    def election_handler(self):
        """
        Function to check if the election timer has timed out.

        This method continuously checks if the election timer has timed out,
        and if so, it starts a new election by calling the `start_election` method.

        This method needs to be called in a separate thread.

        Returns:
            None
        """
        # Logic to handle election
        self.election_timeout = False
        while True:
            if self.role != 'candidate':
                return

            if self.election_end is None or datetime.datetime.now() > self.election_end:
                self.dump(f"Node {self.server_id} election timer timed out, Starting election.")
                self.election_timeout = True
                self.start_election()
                return

            time.sleep(1)


    def start_election(self):
        """
        Starts the election process for the Raft node.

        This method is responsible for initiating the election process by setting the necessary variables,
        sending vote requests to other servers, and handling the election timeout.

        Returns:
            None
        """
        self.dump(f"Node {self.server_id} starting election")
        if (self.last_heartbeat is not None):
            time_since_last_heartbeat = (datetime.datetime.now() - self.last_heartbeat).total_seconds()
            if (time_since_last_heartbeat < HEARTBEAT_WAIT):
                return
            print(f"Server {self.server_id} starting election, last heartbeat: {time_since_last_heartbeat} seconds ago")
        else:
            print(f"Server {self.server_id} starting election, last heatbeat: None")
        self.current_term = self.current_term + 1
        self.role = 'candidate'
        self.voted_for = self.server_id
        self.votes_recieved = set()
        self.votes_recieved.add(self.server_id)

        if len(self.log) != 0:
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index].term
        else:
            last_log_index = 0
            last_log_term = 0

        msg = raft_pb2.VoteRequest(Term=self.current_term,
                                      NodeID=self.server_id,
                                      LastLogIndex=last_log_index,
                                      LastLogTerm=last_log_term)

        if len(self.other_servers) == 0:
            self.role = 'leader'
            self.is_leader = True
            self.is_token = True
            return
        
        self.election_end = datetime.datetime.now() + datetime.timedelta(seconds=ELECTION_DURATION)
        thread_election_handler = threading.Thread(target=self.election_handler)
        thread_election_handler.start()

        threads = []
        for server in self.other_servers:
            if server == self.server_id:
                continue
            thread = threading.Thread(target=self.send_vote_request, args=(server, msg))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def send_vote_request(self, server, msg):
        """
        Sends a vote request to the specified server.

        Args:
            server (str): The ID of the server to send the vote request to.
            msg (VoteRequest message): The vote request message to send.

        Returns:
            None
        """
        try:
            channel = grpc.insecure_channel(self.other_servers[server])
            stub = raft_pb2_grpc.RaftNodeStub(channel)

            print(f"Server {self.server_id} sending vote request to {server}")
            try:
                response = stub.ServeVoteRequest(msg)
            except:
                self.dump(f"Error in sending vote request to Node {server}")
                return

            if (self.election_end == None or datetime.datetime.now() > self.election_end):
                print(f"Server {self.server_id} recieves vote request from {server}, but election is over")
                return

            with self.lock:
                if (self.role == 'candidate' and
                    response.Term == self.current_term and
                    response.Vote):
                    self.votes_recieved.add(response.NodeID)

                    if response.LeaseEnd is not None:
                        if self.lease_end is None or response.LeaseEnd > datetime_to_timestamp(self.lease_end):
                            with self.lease_lock:
                                self.lease_end = timestamp_to_datetime(response.LeaseEnd)

                    if len(self.votes_recieved) > len(self.other_servers) / 2:
                        self.elected()
                        return
                elif response.Term > self.current_term:
                    self.current_term = response.Term
                    self.role = 'follower'
                    self.voted_for = None
                    return
        except Exception as e:
            print(e)
            return


    def elected(self):
        """
        Executes the logic when the server is elected as the leader.

        This method sets the server's role to 'leader', updates the leader information,
        acquires the token, and starts the leader routine. It also initializes the log
        with a NO-OP entry and replicates all logs to the followers.

        Returns:
            None
        """
        print(f"Server {self.server_id} elected as leader")
        self.dump(f"Node {self.server_id} became the leader for term {self.current_term}")
        
        self.role = 'leader'
        self.is_leader = True
        self.leader = self.server_id

        #The newly elected leader must acquire the token. However, if any other
        #server has already acquired the token, the new leader must wait for the
        #lease to expire before acquiring the token.
        while True:
            if (self.lease_end is None or datetime.datetime.now() > self.lease_end):
                self.is_token = True
                self.lease_end = datetime.datetime.now() + datetime.timedelta(seconds=LEASE_DURATION)
                self.dump(f"Leader {self.server_id} has acquired token")
                print(f"Server {self.server_id} has token")
                break
            else:
                self.dump("New Leader waiting for Old Leader Lease to timeout.")
                time_diff = (self.lease_end - datetime.datetime.now()).total_seconds()
                time.sleep(time_diff)
                continue

        for follower in self.other_servers:
            self.sent_length[follower] = len(self.log)
            self.acked_length[follower] = 0

        #Once the leader has acquired the token, it adds a NO-OP entry to the log
        print(f"Server {self.server_id} is leader, starting leader routine")
        log_entry = LogEntry(self.current_term, "NO-OP")
        with self.log_lock:
            self.log.append(log_entry)
        
        self.replicate_all_logs()

    def ServeVoteRequest(self, VoteRequest, context):
        """
        Serves a vote request from a candidate node.

        Args:
            VoteRequest: The vote request message received from the candidate node.
            context: The context of the vote request.

        Returns:
            A VoteResponse message indicating whether the vote is granted or denied.
        """
        try:
            print(f"Server {self.server_id} received vote request from {VoteRequest.NodeID}")
            if VoteRequest.Term < self.current_term:
                print(f"Server {self.server_id} rejected vote request from {VoteRequest.NodeID} because of lower term")
                self.dump(f"Vote denied for Node {VoteRequest.NodeID} in term {self.current_term}")
                return raft_pb2.VoteResponse(NodeID=self.server_id,
                                             Term=self.current_term,
                                             Vote=False,
                                             LeaseEnd=datetime_to_timestamp(self.lease_end)
                                             )

            if VoteRequest.Term > self.current_term:
                self.current_term = VoteRequest.Term
                self.role = 'follower'
                self.voted_for = None

            if len(self.log) != 0:
                last_log_index = len(self.log) - 1
                last_log_term = self.log[last_log_index].term
            else:
                last_log_index = 0
                last_log_term = 0

            logOK = ((VoteRequest.LastLogTerm > last_log_term) or
                     (VoteRequest.LastLogTerm == last_log_term and
                      VoteRequest.LastLogIndex >= last_log_index))

            if (VoteRequest.Term == self.current_term and
                    logOK and
                    (self.voted_for == None or self.voted_for == VoteRequest.NodeID)):
                self.voted_for = VoteRequest.NodeID
                print(f"Server {self.server_id} voted for {VoteRequest.NodeID}")
                self.dump(f"Vote granted for Node {VoteRequest.NodeID} in term {self.current_term}")
                return raft_pb2.VoteResponse(NodeID=self.server_id,
                                             Term=self.current_term,
                                             Vote=True,
                                             LeaseEnd=datetime_to_timestamp(self.lease_end)
                                             )
            else:
                print(f"Server {self.server_id} rejected vote request from {VoteRequest.NodeID} because of log mismatch or already voted for someone else")
                self.dump(f"Vote denied for Node {VoteRequest.NodeID} in term {self.current_term}")
                return raft_pb2.VoteResponse(NodeID=self.server_id,
                                             Term=self.current_term,
                                             Vote=False,
                                             LeaseEnd=datetime_to_timestamp(self.lease_end)
                                             )
        except:
            self.dump("Error in serving vote request")

    def replicate_all_logs(self):
        """
        Replicates all logs to the other servers in the cluster.

        This method iterates over the list of other servers in the cluster and starts a separate thread
        to replicate the logs to each server. It waits for all the threads to complete before returning.

        Returns:
        None
        """
        print(f"Server {self.server_id} replicating all logs")
        threads = []
        for follower in self.other_servers:
            if follower == self.server_id:
                continue
            thread = threading.Thread(target=self.replicate_log, args=(follower,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def replicate_log(self, follower):
        """
        Replicates the log entries to a follower node.

        Args:
            follower (int): The ID of the follower node.

        Returns:
            None
        """
        prefix_length = self.sent_length[follower]
        
        suffix = []
        for i in self.log[prefix_length:]:
            suffix.append(raft_pb2.LogEntry(Term=i.term, Command=i.command))

        if prefix_length > 0:
            prefix_term = self.log[prefix_length - 1].term
        else:
            prefix_term = 0

        channel = grpc.insecure_channel(self.other_servers[follower])
        stub = raft_pb2_grpc.RaftNodeStub(channel)

        print(f"Server {self.server_id} sending log request to {follower}")
        msg = raft_pb2.LogRequest(Term=self.current_term,
                                  LeaderID=self.server_id,
                                  PrefixLength=prefix_length ,
                                  PrefixTerm=prefix_term,
                                  CommitLength = len(self.log), #see this part
                                  LogEntries=suffix,
                                  LeaseEnd = datetime_to_timestamp(self.lease_end)
                                )
        
        try:
            log_response = stub.ServeLogRequest(msg)
        except:
            print(f"Follower {follower} unresponsive")
            self.dump(f"Follower {follower} unresponsive")
            return

        if (log_response.Term == self.current_term and self.role == 'leader'):
            if (log_response.Response and log_response.Ack >= self.acked_length[follower]):
                self.sent_length[follower] = log_response.Ack
                self.acked_length[follower] = log_response.Ack
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] -= 1
                self.replicate_log(follower)

            with self.lease_ack_lock:
                self.lease_ack += 1

            if (self.lease_ack >= len(self.other_servers) / 2):
                self.is_token = True
                self.lease_end = datetime.datetime.now() + datetime.timedelta(seconds=LEASE_DURATION)
        elif log_response.Term > self.current_term:
            self.current_term = log_response.Term
            self.role = 'follower'
            self.voted_for = None
            self.leader = None
            self.cancel_election_timer()

    def acks(self, length):
        """
        Counts the number of servers that have acknowledged a given length.

        Parameters:
        length (int): The length to compare against the acknowledged lengths.

        Returns:
        int: The number of servers that have acknowledged the given length.
        """
        n = 0
        for server in self.other_servers:
            if self.acked_length[server] >= length:
                n += 1
        return n

    def commit_log_entries(self):
        """
        Commits log entries to the state machine.

        This method checks for log entries that have received enough acknowledgments
        from other servers and commits them to the state machine. It updates the
        commit length and appends the committed log entries to the committed_log list.

        Returns:
            None
        """
        min_acks = (len(self.other_servers) + 1) / 2

        ready = []
        for i in range(len(self.log)):
            if self.acks(i) >= min_acks:
                ready.append(i)

        if (len(ready) != 0 and
            max(ready) >= self.commit_length and
            self.log[max(ready)].term == self.current_term):
            for i in range(self.commit_length, max(ready) + 1):
                self.update_database(self.log[i].command)
                self.committed_log.append(self.log[i])
                self.dump(f"Node {self.server_id} (Leader) committed log entry {self.log[i].command} to the state machine")
            self.commit_length = max(ready) + 1

            print(f"Server {self.server_id} committed log entries of length {len(ready)}")



    def ServeLogRequest(self, LogRequest, context):
        """
        Serves a log request from the leader.

        Args:
            LogRequest: The log request object containing information about the leader, term, log entries, etc.
            context: The context object for the gRPC request.

        Returns:
            A LogResponse object indicating whether the log request was accepted or denied.
        """
        try:
            if LogRequest.Term > self.current_term:
                self.current_term = LogRequest.Term
                self.role = 'follower'
                self.voted_for = None
                self.leader = LogRequest.LeaderID
                self.cancel_election_timer()

            if LogRequest.Term == self.current_term:
                self.leader = LogRequest.LeaderID
                self.role = 'follower'

            with self.lease_lock:
                if (LogRequest.LeaseEnd is not None):
                    if (self.lease_end is None or LogRequest.LeaseEnd > datetime_to_timestamp(self.lease_end)):
                        self.lease_end = timestamp_to_datetime(LogRequest.LeaseEnd)

            with self.heartbeat_lock:
                self.last_heartbeat = datetime.datetime.now()

            logOk = ((len(self.log) >= LogRequest.PrefixLength) and
                    (LogRequest.PrefixLength == 0 or
                    self.log[LogRequest.PrefixLength - 1].term == LogRequest.PrefixTerm))

            if self.current_term == LogRequest.Term and logOk:
                self.append_entries(LogRequest.PrefixLength, LogRequest.CommitLength, LogRequest.LogEntries)
                ack = LogRequest.PrefixLength + len(LogRequest.LogEntries)
                self.dump(f"Node {self.server_id} accepted AppendEntries RPC from {LogRequest.LeaderID}")
                return raft_pb2.LogResponse(NodeID=self.server_id,
                                            Term=self.current_term,
                                            Ack=ack,
                                            Response=True)
            else:
                self.dump(f"Node {self.server_id} denied AppendEntries RPC from {LogRequest.LeaderID}")
                return raft_pb2.LogResponse(NodeID=self.server_id,
                                            Term=self.current_term,
                                            Ack=0,
                                            Response=False)
        except:
            self.dump("Error in serving log request")
        
    def append_entries(self, prefix_length, leader_commit, suffix):
        """
        Appends entries to the log and updates the commit length.

        Args:
            prefix_length (int): The length of the prefix in the log.
            leader_commit (int): The index of the leader's commit.
            suffix (list): The list of log entries to be appended.

        Returns:
            None
        """
        if (len(suffix) > 0 and len(self.log) > prefix_length):
            index = min(len(self.log), prefix_length + len(suffix)) - 1
            if (self.log[index] != suffix[index - prefix_length]):
                with self.log_lock:
                    self.log = self.log[:index]

        if (prefix_length + len(suffix) > len(self.log)):
            for i in range(len(self.log) - prefix_length, len(suffix)):
                log_entry = LogEntry(suffix[i].Term, suffix[i].Command)
                with self.log_lock:
                    self.log.append(log_entry)
                    self.committed_log.append(log_entry)

        if (leader_commit > self.commit_length):
            for i in range(self.commit_length, leader_commit):
                try:
                    self.update_database(self.log[i].command)
                    self.dump(f"Node {self.server_id} (Follower) committed log entry {self.log[i].command} to the state machine")
                except Exception as e:
                    print(e)

            self.commit_length = leader_commit

        print(f"Server {self.server_id} appended entries")


    def update_database(self, command):
        """
        Updates the database based on the given command.

        Parameters:
        command (str): The command to be executed on the database.

        Returns:
        None
        """
        
        if command.startswith("SET"):
            key = command.split(" ")[1]
            value = command.split(" ")[2]
            with self.database_lock:
                self.database[key] = value
    

    def db_get(self, key):
        """
        Retrieves the value associated with the given key from the database.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            str: The value associated with the given key, or an empty string if the key is not found.
        """
        return self.database.get(key, "")

    def execute_log_entry(self, request):
        """
        Executes a log entry by appending it to the log and replicating all logs.

        Args:
            request: The log entry request to be executed.

        Returns:
            None
        """
        with self.log_lock:
            self.log.append(LogEntry(self.current_term, request.Request))
        self.replicate_all_logs()
           

    def ServeClient(self, request, context):
        """
        Handles client requests and forwards them to the leader if necessary.

        Args:
            request: The client request.
            context: The context of the request.

        Returns:
            A response indicating the result of the request.
        """
        try:
            print(f"Server {self.server_id} received client request: {request}")
            if (self.role != 'leader'):
                if (self.leader == None):
                    response = raft_pb2.ClientResponse(Data="No leader available",
                                                    LeaderID=None,
                                                    Success=False)
                    return response

                # Forward the request to the leader
                print(f"Server {self.server_id} forwarding request to leader {self.leader}")
                
                try:
                    channel = grpc.insecure_channel(self.other_servers[self.leader])
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    response = stub.ServeClient(request)
                    return response
                except:
                    thread = threading.Thread(target=self.start_election)
                    thread.start()
                    response = raft_pb2.ClientResponse(Data="Leader is down",
                                                    LeaderID=None,
                                                    Success=False)
                    return response
            
            if (self.role == 'leader'):
                if (request.Request.startswith("GET")):
                    print(f"Server {self.server_id} processing GET request")
                    self.dump(f"Node {self.server_id} (leader) received GET request")
                    if (self.is_token == False):
                        print(f"Server {self.server_id} does not have token")
                        response = raft_pb2.ClientResponse(Data="Do not have token yet",
                                                        LeaderID=self.server_id,
                                                        Success=False)
                        return response
                    else:
                        print(f"Server {self.server_id} has token")
                        key = request.Request.split(" ")[1]
                        val = self.db_get(key)
                        response = raft_pb2.ClientResponse(Data=val,
                                                        LeaderID=self.server_id,
                                                        Success=True)
                        return response
                    
                elif (request.Request.startswith("SET")):
                    print(f"Server {self.server_id} processing SET request")
                    self.dump(f"Node {self.server_id} (leader) received SET request")
                    try:
                        # Spawn a new thread to execute the log entry
                        thread = threading.Thread(target=self.execute_log_entry, args=(request,))
                        thread.start()
                        response = raft_pb2.ClientResponse(Data="Request received successfully!",
                                                            LeaderID=self.server_id,
                                                            Success=True)
                        return response
                    except Exception as e:
                        response = raft_pb2.ClientResponse(Data="Request failed!",
                                                        LeaderID=self.server_id,
                                                        Success=False)
                        return response

            response = raft_pb2.ClientResponse(Data="Error!",
                                            LeaderID=self.leader,
                                            Success=False)
        except:
            try:
                response = raft_pb2.ClientResponse(Data="Error!",
                                            LeaderID=self.leader,
                                            Success=False)
                return response
            except:
                self.dump("Error in serving client request")
        

def run_server(server_id, server_address, servers):
    """
    Runs the Raft server with the given server_id, server_address, and list of servers.

    Args:
        server_id (int): The ID of the server.
        server_address (str): The address at which the server will run.
        servers (list): A list of server addresses in the Raft cluster.

    Returns:
        None
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftNodeServicer_to_server(RaftNode(server_id, servers), server)
    server.add_insecure_port(server_address)
    server.start()
    print(f"Server {server_id} started at {server_address}")
    server.wait_for_termination()

'''
def serve():
    servers = {'50051':'localhost:50051',
               '50052': 'localhost:50052',
               '50053': 'localhost:50053',
               '50054': 'localhost:50054',
               '50055': 'localhost:50055',
               '50056': 'localhost:50056',
               '50057': 'localhost:50057'}

    threads = []
    for server_id, server_address in servers.items():
        thread = threading.Thread(target=run_server, args=(server_id, server_address, servers))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
'''

def serve():
    """
    Starts the server and prompts the user to enter a server id.
    Then, it runs the server with the provided server id and the corresponding server address from the servers dictionary.
    
    Parameters:
    None
    
    Returns:
    None
    """
    servers = {'50051':'localhost:50051',
               '50052': 'localhost:50052',
               '50053': 'localhost:50053',
               '50054': 'localhost:50054',
               '50055': 'localhost:50055',
            }
    
    server_id = input("Enter server id: ")
    run_server(server_id, servers[server_id], servers)

if __name__ == '__main__':
    serve()