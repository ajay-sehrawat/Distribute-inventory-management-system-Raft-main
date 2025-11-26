# client/client.py
import grpc
import time
import getpass

from server import auth_pb2, auth_pb2_grpc, inventory_pb2, inventory_pb2_grpc

# Raft cluster nodes
SERVERS = [
    "ipv4:127.0.0.1:51001",
    "ipv4:127.0.0.1:51002",
    "ipv4:127.0.0.1:51003",
]


# ================================================================
# Helper: Probe server without requiring auth (safe method)
# ================================================================
def probe_server(address, timeout=0.5):
    try:
        ch = grpc.insecure_channel(address)
        stub = auth_pb2_grpc.AuthServiceStub(ch)
        stub.Login(auth_pb2.LoginRequest(username="", password=""), timeout=timeout)
        # This always returns error but proves server is reachable
        return ch
    except Exception:
        try:
            ch.close()
        except:
            pass
        return None


# ================================================================
# Connect to first reachable node
# ================================================================
def connect_any_node():
    for addr in SERVERS:
        ch = probe_server(addr)
        if ch:
            return ch
    raise RuntimeError("❌ No Raft node reachable. Please start servers first.")


# ================================================================
# Display inventory
# ================================================================
def show_inventory(inv_stub, token):
    resp = inv_stub.Get(inventory_pb2.GetRequest(token=token, type="ALL"))
    if resp.status != "OK":
        print(f"Error fetching inventory: {resp.message}")
        return []

    print("\n=== Current Inventory ===")
    for idx, item in enumerate(resp.items, start=1):
        print(f"{idx}. {item.name} ({item.sku}) - Stock: {item.stock}")

    return resp.items


# ================================================================
# Wait until inventory reflects the committed Raft update
# ================================================================
def wait_until_applied(inv_stub, token, sku, expected_stock, timeout=2.0):
    start = time.time()
    while time.time() - start < timeout:
        items = inv_stub.Get(inventory_pb2.GetRequest(token=token, type="ALL")).items
        for it in items:
            if it.sku == sku and it.stock == expected_stock:
                return True
        time.sleep(0.05)
    return False


# ================================================================
# Handle follower → leader redirect automatically
# ================================================================
def check_redirect_and_reconnect(resp, channel, token):
    """If follower forwarded incorrectly, reconnect to correct leader."""
    if "Forward" in resp.message or "leader" in resp.message.lower():
        try:
            # probe all nodes again for a leader
            return connect_any_node()
        except Exception:
            return channel
    return channel


# ================================================================
# Main Interactive Client
# ================================================================
session_actions = []


def interactive_client():
    channel = connect_any_node()
    auth = auth_pb2_grpc.AuthServiceStub(channel)
    inv = inventory_pb2_grpc.InventoryServiceStub(channel)

    # --------------------------
    # LOGIN
    # --------------------------
    print("=== Login ===")
    username = input("Enter username (e.g., Ajay): ").strip()
    password = getpass.getpass("Enter password: ").strip()

    login = auth.Login(auth_pb2.LoginRequest(username=username, password=password))
    if login.status != "OK":
        print("Login failed:", login.message)
        return

    token = login.token
    role = "manager" if "manager" in login.message.lower() else "customer"
    print(f"\nWelcome, {username}! Logged in as {role}\n")

    show_inventory(inv, token)

    # --------------------------
    # MENU LOOP
    # --------------------------
    while True:
        print("\nOptions:")

        if role == "manager":
            print("1. Add stock")
            print("2. View inventory")
            print("3. Logout")
            choice = input("Choose (1–3): ").strip()

            if choice == "1":
                sku = input("Enter SKU: ").strip().upper()
                qty_str = input("Enter quantity: ").strip()

                if not qty_str.isdigit() or int(qty_str) <= 0:
                    print("Invalid quantity.")
                    continue
                qty = int(qty_str)

                # get old stock
                old_inv = inv.Get(inventory_pb2.GetRequest(token=token, type="ONE", sku=sku))
                if old_inv.status != "OK":
                    print(old_inv.message)
                    continue
                old_stock = old_inv.items[0].stock

                # POST request
                resp = inv.Post(inventory_pb2.PostRequest(
                    token=token, type="ADD_STOCK", sku=sku, qty=qty
                ))

                print(f"{resp.status}: {resp.message}")

                # wait for updated inventory
                expected = old_stock + qty
                wait_until_applied(inv, token, sku, expected)

                show_inventory(inv, token)

                if resp.status == "OK":
                    session_actions.append(f"Added {qty} → {sku}")

            elif choice == "2":
                show_inventory(inv, token)

            elif choice == "3":
                logout = auth.Logout(auth_pb2.LogoutRequest(token=token))
                print(logout.message)
                if session_actions:
                    print("\n=== Session Summary ===")
                    for i, act in enumerate(session_actions, 1):
                        print(f"{i}. {act}")
                return

        # --------------------------
        # CUSTOMER MENU
        # --------------------------
        else:
            print("1. Buy an item")
            print("2. Ask LLM about item")
            print("3. Logout")
            choice = input("Choose (1–3): ").strip()

            if choice == "1":
                sku = input("Enter SKU: ").strip().upper()
                qty_str = input("Enter qty: ").strip()

                if not qty_str.isdigit() or int(qty_str) <= 0:
                    print("Invalid qty.")
                    continue

                qty = int(qty_str)

                # get old stock
                old_inv = inv.Get(inventory_pb2.GetRequest(token=token, type="ONE", sku=sku))
                if old_inv.status != "OK":
                    print(old_inv.message)
                    continue
                old_stock = old_inv.items[0].stock

                resp = inv.Post(inventory_pb2.PostRequest(
                    token=token, type="ORDER", sku=sku, qty=qty
                ))

                print(f"{resp.status}: {resp.message}")

                # wait for Raft apply
                expected = old_stock - qty
                wait_until_applied(inv, token, sku, expected)

                show_inventory(inv, token)

                if resp.status == "OK":
                    session_actions.append(f"Bought {qty} → {sku}")

            elif choice == "2":
                sku = input("Enter SKU: ").strip().upper()
                resp = inv.Post(inventory_pb2.PostRequest(
                    token=token, type="ASK_LLM", sku=sku, qty=0
                ))
                print("LLM says:", resp.message)

            elif choice == "3":
                logout = auth.Logout(auth_pb2.LogoutRequest(token=token))
                print(logout.message)
                if session_actions:
                    print("\n=== Session Summary ===")
                    for i, act in enumerate(session_actions, 1):
                        print(f"{i}. {act}")
                return


# ================================================================
def main():
    print("Connecting to cluster...")
    try:
        interactive_client()
    except Exception as e:
        print("Client error:", e)


if __name__ == "__main__":
    main()
