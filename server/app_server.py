# server/app_server.py
import os
import sys
import time
import uuid
import traceback
import threading
from concurrent import futures
import grpc
import hashlib

from server import auth_pb2, auth_pb2_grpc, inventory_pb2, inventory_pb2_grpc
from llm_server import llm_pb2, llm_pb2_grpc

# GLOBAL logical state (each node has own physical copy)
USERS = {
    "Alankrit": {"password": "Alankrit", "role": "customer"},
    "Diti": {"password": "Diti", "role": "customer"},
    "Ajay": {"password": "Ajay", "role": "manager"},
}

INVENTORY = {
    "SKU-MOBILE PHONE": {"name": "Mobile Phone", "stock": 10},
    "SKU-POWERBANK": {"name": "Powerbank", "stock": 5},
    "SKU-LAPTOP": {"name": "Laptop", "stock": 8},
}


# ------------------------
# Token helpers (stateless)
# ------------------------
def _make_token_for(username: str, password: str) -> str:
    """
    Deterministic token derived from username+password.
    Any node can recompute and validate tokens (no SESSIONS required).
    Good for demo / local cluster.
    """
    msg = f"{username}:{password}"
    h = hashlib.sha256(msg.encode("utf-8")).hexdigest()
    return h


def require_auth(token: str):
    """
    Validate token by scanning USERS and recomputing expected token.
    Returns (ok, username, role)
    """
    if not token:
        return False, None, None
    for username, info in USERS.items():
        expected = _make_token_for(username, info.get("password", ""))
        if token == expected:
            return True, username, info.get("role", "customer")
    return False, None, None


# ------------------------
# Auth service (stateless token)
# ------------------------
class AuthService(auth_pb2_grpc.AuthServiceServicer):
    def Login(self, request, context):
        username = (request.username or "").strip()
        password = request.password or ""
        user = USERS.get(username)
        if user and user.get("password") == password:
            token = _make_token_for(username, password)
            return auth_pb2.LoginResponse(
                status="OK",
                token=token,
                message=f"Welcome {username}! Logged in as {user.get('role', 'customer')}."
            )
        return auth_pb2.LoginResponse(status="ERROR", token="", message="Invalid username or password")

    def Logout(self, request, context):
        # Stateless: simply acknowledge (no server-side session removal)
        ok, username, role = require_auth(request.token)
        if ok:
            return auth_pb2.StatusReply(status="OK", message="Logged out.")
        return auth_pb2.StatusReply(status="ERROR", message="Invalid token.")


# ------------------------
# Inventory / Business logic
# ------------------------
class InventoryService(inventory_pb2_grpc.InventoryServiceServicer):
    def __init__(self, llm_channel_target="127.0.0.1:50052"):
        self.llm_channel_target = llm_channel_target

    def apply_business_logic(self, ctype, sku, qty, context=None):
        """
        Apply a command to local INVENTORY. Called from apply loop (context=None)
        or directly for ASK_LLM (context present).
        Returns auth_pb2.StatusReply.
        """
        try:
            if not sku and ctype != "ASK_LLM":
                return auth_pb2.StatusReply(status="ERROR", message="Missing SKU")
            sku = (sku or "").upper()
            if ctype != "ASK_LLM" and sku not in INVENTORY:
                return auth_pb2.StatusReply(status="ERROR", message=f"Unknown SKU {sku}")

            if ctype == "ORDER":
                if INVENTORY[sku]["stock"] < qty:
                    return auth_pb2.StatusReply(status="ERROR", message="Insufficient stock")
                INVENTORY[sku]["stock"] -= qty
                return auth_pb2.StatusReply(status="OK", message=f"Order placed for {qty} of {INVENTORY[sku]['name']}")

            if ctype == "ADD_STOCK":
                INVENTORY[sku]["stock"] += qty
                return auth_pb2.StatusReply(status="OK", message=f"Added {qty} units to {INVENTORY[sku]['name']}")

            if ctype == "ASK_LLM":
                # Read-only; handled locally
                try:
                    ch_target = f"ipv4:{self.llm_channel_target}"
                    with grpc.insecure_channel(ch_target) as ch:
                        stub = llm_pb2_grpc.LLMServiceStub(ch)
                        req_id = str(uuid.uuid4())
                        resp = stub.GetLLMAnswer(
                            llm_pb2.AskRequest(
                                request_id=req_id,
                                query=f"Should we reorder {INVENTORY.get(sku, {'name': sku})['name']}? Current stock={INVENTORY.get(sku,{'stock': 'N/A'})['stock']}",
                                context="inventory"
                            ),
                            timeout=7
                        )
                        return auth_pb2.StatusReply(status="OK", message=resp.answer)
                except Exception as e:
                    return auth_pb2.StatusReply(status="ERROR", message=f"LLM error: {e}")

            return auth_pb2.StatusReply(status="ERROR", message="Unknown type; use ORDER, ADD_STOCK, ASK_LLM")
        except Exception as e:
            traceback.print_exc()
            return auth_pb2.StatusReply(status="ERROR", message=f"Internal error: {e}")

    def Get(self, request, context):
        ok, user, role = require_auth(request.token)
        if not ok:
            return inventory_pb2.GetResponse(status="ERROR", items=[], message="Unauthorized")

        typ = (request.type or "ALL").upper().strip()
        if typ == "ALL":
            items = [inventory_pb2.Item(sku=sku, name=data["name"], stock=data["stock"]) for sku, data in INVENTORY.items()]
            return inventory_pb2.GetResponse(status="OK", items=items, message=f"{len(items)} items")
        elif typ == "ONE":
            sku = (request.sku or "").strip().upper()
            if sku not in INVENTORY:
                return inventory_pb2.GetResponse(status="ERROR", items=[], message=f"Unknown SKU {sku}")
            d = INVENTORY[sku]
            return inventory_pb2.GetResponse(status="OK", items=[inventory_pb2.Item(sku=sku, name=d["name"], stock=d["stock"])], message="1 item")
        else:
            return inventory_pb2.GetResponse(status="ERROR", items=[], message="Unknown type; use ALL or ONE")


# ------------------------
# Raft-aware wrapper (Inventory + Auth)
# ------------------------
class AppServerWithRaft(inventory_pb2_grpc.InventoryServiceServicer):
    def __init__(self, raft_state, llm_channel_target="127.0.0.1:50052", wait_apply_timeout=2.0):
        """
        wait_apply_timeout: leader waits this long for local apply loop to apply committed entry.
        """
        self.raft = raft_state
        self.inventory = InventoryService(llm_channel_target=llm_channel_target)
        self.auth = AuthService()
        self.wait_apply_timeout = float(wait_apply_timeout)

    def Post(self, request, context):
        """
        Raft-aware POST:
          - For ASK_LLM (read-only) validate locally and respond.
          - For modifying commands (ORDER/ADD_STOCK): if not leader -> forward to leader.
            Forward occurs without local token validation (leader will validate).
        """
        try:
            print(f"[{time.strftime('%H:%M:%S')}] [Node {self.raft.node_id}] Post called: type={request.type} sku={request.sku} qty={request.qty} role={self.raft.state}")
            sys.stdout.flush()

            typ = (request.type or "").upper().strip()

            # READ-ONLY: ASK_LLM -> needs local auth
            if typ == "ASK_LLM":
                ok, user, role = require_auth(request.token)
                if not ok:
                    return auth_pb2.StatusReply(status="ERROR", message="Unauthorized")
                # inventory.apply_business_logic handles LLM call
                return self.inventory.apply_business_logic(typ, request.sku or "", int(request.qty or 0), context)

            # Only ORDER and ADD_STOCK are valid modifying commands
            if typ not in ("ORDER", "ADD_STOCK"):
                return auth_pb2.StatusReply(status="ERROR", message="Unknown type; use ORDER, ADD_STOCK, ASK_LLM")

            # If not leader -> forward to leader (don't validate token locally; leader will)
            # Also avoid forwarding to ourselves (if leader_id points to self)
            if self.raft.state != "leader":
                leader_id = self.raft.leader_id
                if leader_id is None:
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details("Leader not known yet")
                    return auth_pb2.StatusReply(status="ERROR", message="Leader unknown")

                leader_addr = self.raft.peers.get(leader_id)
                # If leader address equals this node's address, treat as local leader (avoid forwarding to self)
                my_addr = self.raft.peers.get(self.raft.node_id)
                if leader_addr is None:
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details("Leader address missing")
                    return auth_pb2.StatusReply(status="ERROR", message="Leader address missing")

                if leader_id == self.raft.node_id or (my_addr and leader_addr == my_addr):
                    # leader_id claims this node is leader, fall through to leader path
                    pass
                else:
                    ch_target = f"ipv4:{leader_addr}"
                    print(f"[Node {self.raft.node_id}] FORWARD → leader @ {ch_target}")
                    sys.stdout.flush()
                    try:
                        with grpc.insecure_channel(ch_target) as ch:
                            stub = inventory_pb2_grpc.InventoryServiceStub(ch)
                            resp = stub.Post(request, timeout=12)
                            return resp
                    except Exception as e:
                        print(f"[Node {self.raft.node_id}] Forward to leader failed:", e)
                        traceback.print_exc()
                        context.set_code(grpc.StatusCode.UNAVAILABLE)
                        context.set_details(f"Forward failed: {e}")
                        return auth_pb2.StatusReply(status="ERROR", message=f"Forward failed: {e}")

            # LEADER path: validate token (leader must know token)
            ok, username, role = require_auth(request.token)
            if not ok:
                return auth_pb2.StatusReply(status="ERROR", message="Unauthorized")

            # Append command to leader's log (do not apply here)
            cmd = f"{typ}|{request.sku}|{request.qty}"
            with self.raft.lock:
                self.raft.log.append((self.raft.current_term, cmd))
                index = len(self.raft.log) - 1

            print(f"[Leader {self.raft.node_id}] Appended log idx={index} cmd={cmd}")
            sys.stdout.flush()

            # replicate to followers
            self.raft.refresh_stubs()
            success_count = 1
            total_peers = len(self.raft.peers)

            for pid, stub in self.raft.stubs.items():
                if pid == self.raft.node_id:
                    continue
                if stub is None:
                    print(f"[Leader {self.raft.node_id}] No stub for peer {pid}, skipping")
                    sys.stdout.flush()
                    continue
                try:
                    prev = index - 1
                    prev_term = self.raft.log[prev][0] if prev >= 0 else 0
                    areq = inventory_pb2.AppendRequest(
                        term=self.raft.current_term,
                        leaderId=self.raft.node_id,
                        prevLogIndex=prev,
                        prevLogTerm=prev_term,
                        commitIndex=self.raft.commit_index,
                        entries=[cmd]
                    )
                    print(f"[Leader {self.raft.node_id}] Sending AppendEntries to {pid} addr={self.raft.peers.get(pid)} entries={len(areq.entries)}")
                    sys.stdout.flush()
                    rep = stub.AppendEntries(areq, timeout=7)
                    print(f"[Leader {self.raft.node_id}] AppendEntries reply from {pid}: success={getattr(rep,'success',None)} term={getattr(rep,'term',None)}")
                    sys.stdout.flush()
                    if getattr(rep, "success", False):
                        success_count += 1
                    if getattr(rep, "term", 0) > self.raft.current_term:
                        print(f"[Leader {self.raft.node_id}] Follower {pid} has higher term={rep.term}; stepping down")
                        with self.raft.lock:
                            self.raft.current_term = rep.term
                            self.raft.state = "follower"
                            self.raft.leader_id = None
                        return auth_pb2.StatusReply(status="ERROR", message="Leader lost leadership; retry")
                except Exception as e:
                    print(f"[Leader {self.raft.node_id}] Replication to {pid} failed: {e}")
                    traceback.print_exc()
                    sys.stdout.flush()
                    # continue to other peers

            # majority check (success_count > floor(N/2) is majority)
            if success_count > (total_peers // 2):
                with self.raft.lock:
                    self.raft.commit_index = index
                print(f"[Leader {self.raft.node_id}] Majority achieved ({success_count}/{total_peers}). Commit index={index}")
                sys.stdout.flush()

                # optionally wait for local apply
                start = time.time()
                applied = False
                while time.time() - start < self.wait_apply_timeout:
                    with self.raft.lock:
                        if getattr(self.raft, "last_applied", -1) >= index:
                            applied = True
                            break
                    time.sleep(0.03)

                if applied:
                    return auth_pb2.StatusReply(status="OK", message="OK: Applied")
                else:
                    return auth_pb2.StatusReply(status="OK", message="OK: Committed")

            print(f"[Leader {self.raft.node_id}] Replication failed ({success_count}/{total_peers})")
            sys.stdout.flush()
            return auth_pb2.StatusReply(status="ERROR", message="Replication failed")
        except Exception as e:
            print(f"[Node {self.raft.node_id}] Unexpected error in Post(): {e}")
            traceback.print_exc()
            sys.stdout.flush()
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(e))
            return auth_pb2.StatusReply(status="ERROR", message=f"Server error: {e}")

    def Get(self, request, context):
        return self.inventory.Get(request, context)


def register_services(server, app_servicer):
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(app_servicer, server)
    auth_pb2_grpc.add_AuthServiceServicer_to_server(app_servicer.auth, server)


# local dev server helper
if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    class DummyRaft:
        def __init__(self):
            self.node_id = 1
            self.peers = {1: f"127.0.0.1:50051"}
            self.lock = threading.Lock()
            self.current_term = 0
            self.log = []
            self.commit_index = -1
            self.last_applied = -1
            self.state = "leader"
            self.leader_id = 1
            self.stubs = {}
        def refresh_stubs(self):
            pass
    raft = DummyRaft()
    app = AppServerWithRaft(raft)
    register_services(server, app)
    server.add_insecure_port("127.0.0.1:50051")
    print("App Server listening on 127.0.0.1:50051")
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
