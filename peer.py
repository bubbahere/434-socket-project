#!/usr/bin/env python3
"""
CSE 434 - Socket Programming Project
DHT Peer (Milestone: register, setup-dht, dht-complete)
Group 81 - Port range: 40500 ~ 40999

Usage:
    python3 peer.py <manager-ip> <manager-port>

Then enter commands from stdin, for example:
    register Alice 127.0.0.1 40501 40502
    setup-dht Alice 3 1950
    dht-complete Alice
"""

import socket
import sys
import csv
import os

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
BUFFER_SIZE = 65536   # Larger buffer; setup-dht reply can be long


# ──────────────────────────────────────────────
# Utility functions
# ──────────────────────────────────────────────

def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5)+1, 2):
        if n % i == 0:
            return False
    return True


def next_prime(n):
    """Return the first prime greater than n"""
    candidate = n + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate


def load_csv(year):
    """
    Read details-YYYY.csv and return a list of records.
    Each record is a dict (14 fields).
    """
    filename = f"details-{year}.csv"
    if not os.path.exists(filename):
        print(f"[Peer] ERROR: {filename} not found in current directory!")
        return None

    records = []
    with open(filename, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    return records


# ──────────────────────────────────────────────
# DHT local storage
# ──────────────────────────────────────────────
# local_hash_table[pos] = record  (pos = event_id mod s)
local_hash_table = {}
my_id      = None   # this node's id in the ring
ring_size  = None   # ring size n
right_neighbour = None  # (ip, p_port)  right neighbour
peer_tuples = []    # list of all peers' 3-tuples: [(name, ip, p_port), ...]


# ──────────────────────────────────────────────
# Send a message to the Manager and wait for reply
# ──────────────────────────────────────────────

def send_to_manager(sock, manager_addr, message):
    sock.sendto(message.encode(), manager_addr)
    print(f"[Peer] --> Manager: {message}")
    data, _ = sock.recvfrom(BUFFER_SIZE)
    response = data.decode().strip()
    print(f"[Peer] <-- Manager: {response}")
    return response


# ──────────────────────────────────────────────
# Send a message to another Peer (UDP, don't wait for reply)
# ──────────────────────────────────────────────

def send_to_peer(sock, ip, port, message):
    sock.sendto(message.encode(), (ip, port))
    print(f"[Peer] --> Peer({ip}:{port}): {message[:120]}")


# ──────────────────────────────────────────────
# Send store command (forward record along the ring)
# ──────────────────────────────────────────────

def forward_store(sock, target_id, pos, record):
    """
    Send a store command to the right neighbour so it forwards along the ring until target_id.
    Message format:
        store <target_id> <pos> <field1>|<field2>|...|<field14>
    """
    fields = list(record.values())
    record_str = "|".join(str(f) for f in fields)
    msg = f"store {target_id} {pos} {record_str}"
    ip, port = right_neighbour
    send_to_peer(sock, ip, port, msg)


# ──────────────────────────────────────────────
# Handle messages received from Peers (during setup phase)
# ──────────────────────────────────────────────

def handle_peer_message(sock, data):
    """
    Handle messages received from other peers.
    Return True to continue, False to stop waiting.
    """
    global my_id, ring_size, right_neighbour, peer_tuples, local_hash_table

    message = data.decode().strip()
    print(f"[Peer] <-- Peer: {message[:120]}")
    parts = message.split()
    if not parts:
        return True

    command = parts[0].lower()

    # ── set-id command ──────────────────────────────
    # Format: set-id <id> <n> peer0 ip0 p0 peer1 ip1 p1 ...
    if command == "set-id":
        my_id     = int(parts[1])
        ring_size = int(parts[2])
        # parse all peer 3-tuples
        peer_tuples = []
        idx = 3
        while idx + 2 < len(parts):
            peer_tuples.append((parts[idx], parts[idx+1], int(parts[idx+2])))
            idx += 3

        # right neighbour is id = (my_id + 1) mod ring_size
        right_id = (my_id + 1) % ring_size
        right_name, right_ip, right_port = peer_tuples[right_id]
        right_neighbour = (right_ip, right_port)
        print(f"[Peer] Set id={my_id}, ring_size={ring_size}, right_neighbour={right_neighbour}")
        return False   # after receiving set-id we don't need to keep waiting

    # ── store command ──────────────────────────────
    # Format: store <target_id> <pos> <record_str>
    elif command == "store":
        target_id = int(parts[1])
        pos       = int(parts[2])
        record_str = parts[3] if len(parts) > 3 else ""

        if target_id == my_id:
            # store locally
            local_hash_table[pos] = record_str
        else:
            # forward to right neighbour
            ip, port = right_neighbour
            send_to_peer(sock, ip, port, message)
        return True   # continue listening

    # ── rebuild-dht command (reserved; not needed for milestone) ──
    elif command == "rebuild-dht":
        print(f"[Peer] rebuild-dht received (not implemented in milestone)")
        return True

    else:
        print(f"[Peer] Unknown peer command: {command}")
        return True


# ──────────────────────────────────────────────
# Leader performs post setup-dht steps
# ──────────────────────────────────────────────

def leader_setup_dht(sock, m_port, p_port, manager_addr, n, year, tuples):
    """
    After the leader receives setup-dht SUCCESS it performs:
    1. Send set-id to all other peers
    2. Read the CSV and distribute store commands
    3. Print how many records each node stored
    4. Send dht-complete to manager
    """
    global my_id, ring_size, right_neighbour, peer_tuples, local_hash_table

    my_id     = 0
    ring_size = n
    peer_tuples = tuples  # list of (name, ip, p_port)

    right_id = (my_id + 1) % ring_size
    right_name, right_ip, right_port = peer_tuples[right_id]
    right_neighbour = (right_ip, right_port)

    # 1. Send set-id to peers 1 .. n-1
    tuples_str = " ".join(f"{name} {ip} {pp}" for name, ip, pp in peer_tuples)
    for i in range(1, n):
        name, ip, pp = peer_tuples[i]
        msg = f"set-id {i} {n} {tuples_str}"
        send_to_peer(sock, ip, pp, msg)

    # 2. Read CSV
    print(f"[Peer/Leader] Loading details-{year}.csv ...")
    records = load_csv(year)
    if records is None:
        print("[Peer/Leader] ABORT: CSV file not found.")
        return

    l = len(records)
    s = next_prime(2 * l)
    print(f"[Peer/Leader] Total records: {l}, hash table size s={s}")

    # counters for number of records per node (for printing)
    count = [0] * n
    local_hash_table = {}

    for record in records:
        try:
            event_id = int(record.get("EVENT_ID") or record.get("event_id") or 0)
        except (ValueError, TypeError):
            continue

        pos = event_id % s
        target_id = pos % n
        count[target_id] += 1

        if target_id == my_id:
            local_hash_table[pos] = record
        else:
            # forward along the ring
            fields = list(record.values())
            record_str = "|".join(str(f) for f in fields)
            msg = f"store {target_id} {pos} {record_str}"
            rip, rport = right_neighbour
            send_to_peer(sock, rip, rport, msg)

    # 3. Print stats
    print("\n[Peer/Leader] ── DHT Record Distribution ──")
    for i, name_ip_port in enumerate(peer_tuples):
        print(f"  Node {i} ({name_ip_port[0]}): {count[i]} records")
    print()

    # 4. Send dht-complete
    my_name = peer_tuples[0][0]
    response = send_to_manager(sock, manager_addr, f"dht-complete {my_name}")
    if response.startswith("SUCCESS"):
        print("[Peer/Leader] DHT setup complete!")
    else:
        print(f"[Peer/Leader] dht-complete FAILED: {response}")


# ──────────────────────────────────────────────
# Non-leader peer waits for set-id
# ──────────────────────────────────────────────

def peer_wait_for_set_id(sock):
    """Non-leader peer registers and waits until it receives set-id"""
    print("[Peer] Waiting for set-id from leader...")
    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        keep_going = handle_peer_message(sock, data)
        if not keep_going:
            # set-id handled, continue listening for store commands
            print("[Peer] Now listening for store commands...")
            peer_listen_for_stores(sock)
            return


def peer_listen_for_stores(sock):
    """
    Non-leader peer listens for store commands.
    Because Milestone has no explicit finish signal,
    we set a short timeout and consider setup complete
    when no messages arrive for a while.
    """
    sock.settimeout(3.0)   # consider setup complete if no messages for 3 seconds
    while True:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            handle_peer_message(sock, data)
        except socket.timeout:
            print(f"[Peer] Store phase complete. Stored {len(local_hash_table)} records locally.")
            sock.settimeout(None)
            return


# ──────────────────────────────────────────────
# Main program
# ──────────────────────────────────────────────

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <manager-ip> <manager-port>")
        sys.exit(1)

    manager_ip   = sys.argv[1]
    manager_port = int(sys.argv[2])
    manager_addr = (manager_ip, manager_port)

    # Peer sockets (m_port and p_port are read from the register command)
    # We bind one socket to m_port (manager communication) and another socket to p_port (peer-to-peer communication)
    # For simplicity, in the Milestone we will use the same socket first

    registered   = False
    my_name      = None
    my_m_port    = None
    my_p_port    = None
    sock_m       = None   # manager socket
    sock_p       = None   # peer socket

    print("[Peer] Ready. Type commands (register / setup-dht / dht-complete):")

    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            continue

        parts = line.split()
        command = parts[0].lower()

        # ── register ────────────────────────────
        if command == "register":
            # register <peer-name> <IPv4> <m-port> <p-port>
            if len(parts) != 5:
                print("Usage: register <peer-name> <IPv4> <m-port> <p-port>")
                continue

            _, peer_name, ip, m_port_s, p_port_s = parts
            m_port = int(m_port_s)
            p_port = int(p_port_s)

            # create manager socket
            sock_m = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock_m.bind(("", m_port))

            # create peer socket
            sock_p = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock_p.bind(("", p_port))

            response = send_to_manager(sock_m, manager_addr,
                                       f"register {peer_name} {ip} {m_port} {p_port}")
            if response.startswith("SUCCESS"):
                registered = True
                my_name    = peer_name
                my_m_port  = m_port
                my_p_port  = p_port
                print(f"[Peer] Registered as '{my_name}'")
            else:
                sock_m.close()
                sock_p.close()
                sock_m = sock_p = None

        # ── setup-dht ───────────────────────────
        elif command == "setup-dht":
            # setup-dht <peer-name> <n> <YYYY>
            if not registered:
                print("[Peer] ERROR: must register first")
                continue
            if len(parts) != 4:
                print("Usage: setup-dht <peer-name> <n> <YYYY>")
                continue

            _, peer_name, n_s, yyyy = parts
            n = int(n_s)

            response = send_to_manager(sock_m, manager_addr,
                                       f"setup-dht {peer_name} {n} {yyyy}")

            if not response.startswith("SUCCESS"):
                print(f"[Peer] setup-dht failed: {response}")
                continue

            # parse reply: SUCCESS <n> <yyyy> name0 ip0 p0 name1 ip1 p1 ...
            resp_parts = response.split()
            # resp_parts[0] = SUCCESS
            # resp_parts[1] = n
            # resp_parts[2] = yyyy
            # resp_parts[3..] = tuples
            tuples = []
            idx = 3
            while idx + 2 < len(resp_parts):
                tuples.append((resp_parts[idx], resp_parts[idx+1], int(resp_parts[idx+2])))
                idx += 3

            print(f"[Peer] setup-dht SUCCESS, I am the leader. Starting DHT construction...")
            leader_setup_dht(sock_p, my_m_port, my_p_port, manager_addr,
                             n, yyyy, tuples)

        # ── dht-complete (manual) ──────────────────
        elif command == "dht-complete":
            if not registered:
                print("[Peer] ERROR: must register first")
                continue
            if len(parts) != 2:
                print("Usage: dht-complete <peer-name>")
                continue
            _, peer_name = parts
            response = send_to_manager(sock_m, manager_addr, f"dht-complete {peer_name}")

        # ── wait (let non-leader peer enter waiting state) ────
        elif command == "wait":
            if sock_p is None:
                print("[Peer] ERROR: must register first")
                continue
            peer_wait_for_set_id(sock_p)

        # ── exit ────────────────────────────────
        elif command == "exit":
            print("[Peer] Exiting.")
            break

        else:
            print(f"[Peer] Unknown command: {command}")

    if sock_m:
        sock_m.close()
    if sock_p:
        sock_p.close()


if __name__ == "__main__":
    main()
