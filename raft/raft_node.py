# raft/raft_node.py
import threading
import time
import grpc
import traceback

from server import inventory_pb2, inventory_pb2_grpc

# ===============================
# Raft Timing Constants
# ===============================
ELECTION_TIMEOUT_MIN = 10.0
ELECTION_TIMEOUT_MAX = 12.0
HEARTBEAT_INTERVAL = 1.0


# Helper: normalize gRPC target (force IPv4)
def normalize_target(addr: str) -> str:
    if not addr:
        return addr
    if addr.startswith("ipv4:") or addr.startswith("ipv6:"):
        return addr
    host_port = addr.split(":", 1)
    host = host_port[0] if host_port else ""
    port = host_port[1] if len(host_port) > 1 else ""
    if host in ("localhost", "0.0.0.0", ""):
        host = "127.0.0.1"
    return f"ipv4:{host}:{port}" if port else f"ipv4:{host}"


class RaftState:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers

        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.commit_index = -1
        self.last_applied = -1

        self.state = "follower"
        self.leader_id = None

        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
        self.stubs = {}
        self.refresh_stubs()

    def refresh_stubs(self):
        for pid, addr in self.peers.items():
            if pid == self.node_id:
                self.stubs[pid] = None
                continue
            try:
                target = normalize_target(addr)
                ch = grpc.insecure_channel(target)
                self.stubs[pid] = inventory_pb2_grpc.RaftServiceStub(ch)
            except Exception:
                self.stubs[pid] = None


class RaftRPC(inventory_pb2_grpc.RaftServiceServicer):
    def __init__(self, state: RaftState):
        self.state = state

    def RequestVote(self, request, context):
        with self.state.lock:
            print(f"[{time.strftime('%H:%M:%S')}] [Node {self.state.node_id}] RequestVote from {request.candidateId} term={request.term}")
            try:
                if request.term < self.state.current_term:
                    return inventory_pb2.VoteReply(term=self.state.current_term, voteGranted=False)

                if request.term > self.state.current_term:
                    self.state.current_term = request.term
                    self.state.voted_for = None
                    self.state.state = "follower"

                last_index = len(self.state.log) - 1
                last_term = self.state.log[last_index][0] if last_index >= 0 else 0

                up_to_date = (
                    request.lastLogTerm > last_term or
                    (request.lastLogTerm == last_term and request.lastLogIndex >= last_index)
                )

                if (self.state.voted_for is None or self.state.voted_for == request.candidateId) and up_to_date:
                    self.state.voted_for = request.candidateId
                    print(f"[{time.strftime('%H:%M:%S')}] [Node {self.state.node_id}] VOTED for {request.candidateId} term={self.state.current_term}")
                    return inventory_pb2.VoteReply(term=self.state.current_term, voteGranted=True)

                return inventory_pb2.VoteReply(term=self.state.current_term, voteGranted=False)
            except Exception as e:
                print("[RequestVote ERROR]", e)
                traceback.print_exc()
                return inventory_pb2.VoteReply(term=self.state.current_term, voteGranted=False)

    def AppendEntries(self, request, context):
        with self.state.lock:
            print(f"[{time.strftime('%H:%M:%S')}] [Node {self.state.node_id}] AppendEntries from leader {request.leaderId} term={request.term} entries={len(request.entries)}")
            try:
                if request.term < self.state.current_term:
                    return inventory_pb2.AppendReply(term=self.state.current_term, success=False)

                if request.term > self.state.current_term:
                    self.state.current_term = request.term
                    self.state.voted_for = None
                    self.state.state = "follower"

                # accept leader and update heartbeat time
                self.state.leader_id = request.leaderId
                self.state.last_heartbeat = time.time()

                prev = request.prevLogIndex
                if prev >= 0:
                    if prev >= len(self.state.log):
                        return inventory_pb2.AppendReply(term=self.state.current_term, success=False)
                    if self.state.log[prev][0] != request.prevLogTerm:
                        return inventory_pb2.AppendReply(term=self.state.current_term, success=False)

                if request.entries:
                    self.state.log = self.state.log[:prev + 1]
                    for e in request.entries:
                        self.state.log.append((request.term, e))

                if request.commitIndex > self.state.commit_index:
                    self.state.commit_index = min(request.commitIndex, len(self.state.log) - 1)

                return inventory_pb2.AppendReply(term=self.state.current_term, success=True)
            except Exception as e:
                print("[AppendEntries ERROR]", e)
                traceback.print_exc()
                return inventory_pb2.AppendReply(term=self.state.current_term, success=False)


def apply_committed_entries(state: RaftState, apply_fn):
    try:
        with state.lock:
            while state.last_applied < state.commit_index:
                state.last_applied += 1
                term, cmd = state.log[state.last_applied]
                parts = cmd.split("|")
                ctype = parts[0].upper() if len(parts) > 0 else ""
                sku = parts[1].upper() if len(parts) > 1 else ""
                qty = int(parts[2]) if len(parts) > 2 and parts[2] != "" else 0
                try:
                    apply_fn(ctype, sku, qty, None)
                except TypeError:
                    try:
                        apply_fn(ctype, sku, qty)
                    except Exception as e:
                        print("[APPLY ERROR] apply_fn failed:", e)
                        traceback.print_exc()
                except Exception as e:
                    print("[APPLY ERROR] during apply_fn:", e)
                    traceback.print_exc()
    except Exception as e_outer:
        print("[apply_committed_entries ERROR]", e_outer)
        traceback.print_exc()
