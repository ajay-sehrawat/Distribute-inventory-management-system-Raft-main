# raft/run_node.py
import argparse
import grpc
import threading
import time
import random
import sys
import os
import signal

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from concurrent import futures

from server import app_server
from server import inventory_pb2, inventory_pb2_grpc
from raft.raft_node import (
    RaftState,
    RaftRPC,
    ELECTION_TIMEOUT_MIN,
    ELECTION_TIMEOUT_MAX,
    HEARTBEAT_INTERVAL,
    apply_committed_entries,
)


def start_grpc_server(state, app_servicer, raft_servicer, port, bind_address="127.0.0.1"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=30))
    inventory_pb2_grpc.add_RaftServiceServicer_to_server(raft_servicer, server)
    app_server.register_services(server, app_servicer)
    address = f"{bind_address}:{port}"
    bound_port = server.add_insecure_port(address)
    if bound_port == 0:
        print(f"[Node {state.node_id}] ERROR: Failed to bind to address {address}.")
        raise RuntimeError(f"Failed to bind to address {address}")
    server.start()
    print(f"[Node {state.node_id}] Running on {address} - Role: {state.state}")
    return server


def election_loop(state: RaftState):
    timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    deadline = state.last_heartbeat + timeout
    print(f"[Node {state.node_id}] Election loop started (timeouts {ELECTION_TIMEOUT_MIN}-{ELECTION_TIMEOUT_MAX}s)")
    while True:
        time.sleep(0.05)
        with state.lock:
            if state.state == "leader":
                try:
                    state.refresh_stubs()
                except Exception:
                    pass
                deadline = state.last_heartbeat + random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
                continue

            now = time.time()
            if now < deadline:
                continue

            state.state = "candidate"
            state.current_term += 1
            state.voted_for = state.node_id

            votes = 1
            last_index = len(state.log) - 1
            last_term = state.log[last_index][0] if last_index >= 0 else 0

            try:
                state.refresh_stubs()
            except Exception:
                pass

            for pid, stub in state.stubs.items():
                if pid == state.node_id:
                    continue
                if stub is None:
                    continue
                try:
                    req = inventory_pb2.VoteRequest(
                        term=state.current_term,
                        candidateId=state.node_id,
                        lastLogIndex=last_index,
                        lastLogTerm=last_term,
                    )
                    rep = stub.RequestVote(req, timeout=1)
                    if getattr(rep, "voteGranted", False):
                        votes += 1
                except Exception:
                    pass

            majority = (len(state.peers) // 2)
            if votes > majority:
                state.state = "leader"
                state.leader_id = state.node_id
                state.last_heartbeat = time.time()
                print(f"\n🔥 Node {state.node_id} became LEADER (Term {state.current_term})\n")
                # send immediate heartbeat so followers learn quickly
                try:
                    state.refresh_stubs()
                    last_index = len(state.log) - 1
                    last_term = state.log[last_index][0] if last_index >= 0 else 0
                    for pid, stub in state.stubs.items():
                        if pid == state.node_id or stub is None:
                            continue
                        areq = inventory_pb2.AppendRequest(
                            term=state.current_term,
                            leaderId=state.node_id,
                            prevLogIndex=last_index,
                            prevLogTerm=last_term,
                            commitIndex=state.commit_index,
                            entries=[],
                        )
                        try:
                            stub.AppendEntries(areq, timeout=2)
                        except Exception:
                            pass
                except Exception:
                    pass
            else:
                state.state = "follower"

            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            deadline = state.last_heartbeat + timeout


def heartbeat_loop(state: RaftState):
    print(f"[Node {state.node_id}] Heartbeat loop started (interval {HEARTBEAT_INTERVAL}s)")
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with state.lock:
            if state.state != "leader":
                continue
            last_index = len(state.log) - 1
            last_term = state.log[last_index][0] if last_index >= 0 else 0
            try:
                state.refresh_stubs()
            except Exception:
                pass
            for pid, stub in state.stubs.items():
                if pid == state.node_id:
                    continue
                if stub is None:
                    continue
                try:
                    req = inventory_pb2.AppendRequest(
                        term=state.current_term,
                        leaderId=state.node_id,
                        prevLogIndex=last_index,
                        prevLogTerm=last_term,
                        commitIndex=state.commit_index,
                        entries=[],
                    )
                    stub.AppendEntries(req, timeout=2)
                except Exception:
                    pass


def apply_loop(state: RaftState, app_servicer):
    print(f"[Node {state.node_id}] Apply loop started")
    while True:
        try:
            apply_committed_entries(state, app_servicer.inventory.apply_business_logic)
        except Exception as e:
            print("[APPLY ERROR]", e)
        time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", nargs="*", default=[])
    parser.add_argument("--bind", type=str, default="127.0.0.1")
    args = parser.parse_args()

    peers = {}
    for p in args.peers:
        if ":" not in p:
            print(f"[WARN] skipping invalid peer entry: {p}")
            continue
        pid_str, addr = p.split(":", 1)
        try:
            pid = int(pid_str)
        except ValueError:
            print(f"[WARN] invalid peer id in entry: {p}")
            continue
        peers[pid] = addr

    self_addr = f"{args.bind}:{args.port}"
    if args.id in peers:
        print(f"[WARN] peers contained this node id ({args.id}). Overriding address with {self_addr}.")
    peers[args.id] = self_addr

    state = RaftState(args.id, peers)
    app_servicer = app_server.AppServerWithRaft(state)
    raft_servicer = RaftRPC(state)

    try:
        grpc_server = start_grpc_server(state, app_servicer, raft_servicer, args.port, bind_address=args.bind)
    except Exception as e:
        print(f"[Node {args.id}] Failed to start gRPC server: {e}")
        sys.exit(1)

    t_election = threading.Thread(target=election_loop, args=(state,), daemon=True)
    t_heartbeat = threading.Thread(target=heartbeat_loop, args=(state,), daemon=True)
    t_apply = threading.Thread(target=apply_loop, args=(state, app_servicer), daemon=True)

    t_election.start()
    t_heartbeat.start()
    t_apply.start()

    def shutdown(signum, frame):
        print(f"\n[Node {state.node_id}] Shutting down...")
        try:
            grpc_server.stop(0)
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()
