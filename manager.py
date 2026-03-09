#!/usr/bin/env python3
"""
CSE 434 - Socket Programming Project
DHT Manager (Milestone: register, setup-dht, dht-complete)
Group 81 - Port range: 40500 ~ 40999
"""

import socket
import sys
import random

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
BUFFER_SIZE = 4096

# Peer states
STATE_FREE   = "Free"
STATE_LEADER = "Leader"
STATE_INDHT  = "InDHT"

# ──────────────────────────────────────────────
# Global state
# ──────────────────────────────────────────────
peers = {}
# peers[peer_name] = {
#   "ip": str,
#   "m_port": int,
#   "p_port": int,
#   "state": str,
#   "addr": (ip, m_port)   ← used for sending replies
# }

dht_exists    = False   # Whether a DHT has been fully established
dht_building  = False   # Whether a DHT is currently being built (waiting for dht-complete)
dht_leader    = None    # Current leader peer_name


def find_free_peers(exclude=None):
    """Return a list of peer names in FREE state (excluding 'exclude')"""
    return [name for name, info in peers.items()
            if info["state"] == STATE_FREE and name != exclude]


def handle_register(parts, addr, sock):
    """register <peer-name> <IPv4> <m-port> <p-port>"""
    if len(parts) != 5:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name, ip, m_port_s, p_port_s = parts
    try:
        m_port = int(m_port_s)
        p_port = int(p_port_s)
    except ValueError:
        send_response(sock, addr, "FAILURE bad-port")
        return

    # Check duplicate peer name
    if peer_name in peers:
        print(f"[Manager] REGISTER FAILURE: {peer_name} already registered")
        send_response(sock, addr, "FAILURE duplicate-name")
        return

    # Check port uniqueness
    for name, info in peers.items():
        if info["m_port"] == m_port or info["p_port"] == p_port \
                or info["m_port"] == p_port or info["p_port"] == m_port:
            print(f"[Manager] REGISTER FAILURE: port conflict with {name}")
            send_response(sock, addr, "FAILURE duplicate-port")
            return

    peers[peer_name] = {
        "ip":     ip,
        "m_port": m_port,
        "p_port": p_port,
        "state":  STATE_FREE,
        "addr":   addr
    }
    print(f"[Manager] Registered peer: {peer_name} @ {ip}  m-port={m_port}  p-port={p_port}")
    send_response(sock, addr, "SUCCESS")


def handle_setup_dht(parts, addr, sock):
    """setup-dht <peer-name> <n> <YYYY>"""
    global dht_exists, dht_building, dht_leader

    if len(parts) != 4:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name, n_s, yyyy = parts
    try:
        n = int(n_s)
    except ValueError:
        send_response(sock, addr, "FAILURE bad-n")
        return

    # Various failure conditions
    if peer_name not in peers:
        print(f"[Manager] SETUP-DHT FAILURE: {peer_name} not registered")
        send_response(sock, addr, "FAILURE not-registered")
        return
    if n < 3:
        print(f"[Manager] SETUP-DHT FAILURE: n={n} < 3")
        send_response(sock, addr, "FAILURE n-too-small")
        return
    if dht_exists or dht_building:
        print(f"[Manager] SETUP-DHT FAILURE: DHT already exists")
        send_response(sock, addr, "FAILURE dht-exists")
        return

    free_peers = find_free_peers(exclude=peer_name)
    if len(free_peers) < n - 1:
        print(f"[Manager] SETUP-DHT FAILURE: not enough free peers")
        send_response(sock, addr, "FAILURE not-enough-peers")
        return

    # Select n-1 random FREE peers
    chosen = random.sample(free_peers, n - 1)

    # Update states
    peers[peer_name]["state"] = STATE_LEADER
    for name in chosen:
        peers[name]["state"] = STATE_INDHT

    dht_building = True
    dht_leader   = peer_name

    # Build response message:
    # SUCCESS <n> <YYYY> peer0 ip0 p-port0 peer1 ip1 p-port1 ...
    # Leader's 3-tuple appears first
    tuples = []
    tuples.append(f"{peer_name} {peers[peer_name]['ip']} {peers[peer_name]['p_port']}")
    for name in chosen:
        tuples.append(f"{name} {peers[name]['ip']} {peers[name]['p_port']}")

    response = "SUCCESS " + f"{n} {yyyy} " + " ".join(tuples)
    print(f"[Manager] SETUP-DHT SUCCESS: leader={peer_name}, peers={[peer_name]+chosen}, year={yyyy}")
    send_response(sock, addr, response)


def handle_dht_complete(parts, addr, sock):
    """dht-complete <peer-name>"""
    global dht_exists, dht_building

    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if peer_name != dht_leader:
        print(f"[Manager] DHT-COMPLETE FAILURE: {peer_name} is not the leader")
        send_response(sock, addr, "FAILURE not-leader")
        return

    dht_building = False
    dht_exists   = True
    print(f"[Manager] DHT-COMPLETE: DHT is now active, leader={peer_name}")
    send_response(sock, addr, "SUCCESS")


def send_response(sock, addr, message):
    """Send a UDP response"""
    sock.sendto(message.encode(), addr)
    print(f"[Manager] --> {addr}: {message}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", port))
    print(f"[Manager] Listening on port {port} ...")

    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        message = data.decode().strip()
        print(f"[Manager] <-- {addr}: {message}")

        parts = message.split()
        if not parts:
            continue

        command = parts[0].lower()

        # If DHT is being built, only accept dht-complete
        if dht_building and command != "dht-complete":
            print(f"[Manager] FAILURE: DHT is being built, only dht-complete accepted")
            send_response(sock, addr, "FAILURE dht-building")
            continue

        if command == "register":
            handle_register(parts, addr, sock)
        elif command == "setup-dht":
            handle_setup_dht(parts, addr, sock)
        elif command == "dht-complete":
            handle_dht_complete(parts, addr, sock)
        else:
            print(f"[Manager] Unknown command: {command}")
            send_response(sock, addr, "FAILURE unknown-command")


if __name__ == "__main__":
    main()
