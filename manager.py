#!/usr/bin/env python3
"""
CSE 434 — DHT Manager (UDP server, §1.1)

Tracks registered peers (Free / Leader / InDHT), one DHT at a time, and
serialized phases: build (setup-dht → dht-complete), churn (leave/join →
dht-rebuilt), teardown (teardown-dht → teardown-complete). Responds to each
datagram with SUCCESS/FAILURE and any required payload.

Usage: python3 manager.py <port>
"""

import socket
import sys
import random

BUFFER_SIZE = 8192

STATE_FREE = "Free"
STATE_LEADER = "Leader"
STATE_INDHT = "InDHT"

peers = {}

dht_exists = False
dht_building = False
dht_leader = None
dht_size = 0

waiting_rebuilt = False
rebuilt_initiator = None
rebuilt_kind = None

waiting_teardown = False


def find_free_peers(exclude=None):
    """Names of peers in state Free, optionally excluding one name."""
    return [name for name, info in peers.items()
            if info["state"] == STATE_FREE and name != exclude]


def dht_member_names():
    """Names of peers currently Leader or InDHT (ring members)."""
    return [name for name, info in peers.items()
            if info["state"] in (STATE_LEADER, STATE_INDHT)]


def peer_in_dht(name):
    """True if name is registered as Leader or InDHT."""
    if name not in peers:
        return False
    return peers[name]["state"] in (STATE_LEADER, STATE_INDHT)


def manager_busy_except(allowed_command):
    """True when a phase lock is active and only allowed_command may proceed."""
    if dht_building:
        return allowed_command != "dht-complete"
    if waiting_rebuilt:
        return allowed_command != "dht-rebuilt"
    if waiting_teardown:
        return allowed_command != "teardown-complete"
    return False


def send_response(sock, addr, message):
    """Send one UDP reply and log it."""
    sock.sendto(message.encode(), addr)
    print(f"[Manager] --> {addr}: {message}")


def handle_register(parts, addr, sock):
    """register: add peer with unique name and unique ports; state Free."""
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

    if peer_name in peers:
        print(f"[Manager] REGISTER FAILURE: {peer_name} already registered")
        send_response(sock, addr, "FAILURE duplicate-name")
        return

    for name, info in peers.items():
        if info["m_port"] == m_port or info["p_port"] == p_port \
                or info["m_port"] == p_port or info["p_port"] == m_port:
            print(f"[Manager] REGISTER FAILURE: port conflict with {name}")
            send_response(sock, addr, "FAILURE duplicate-port")
            return

    peers[peer_name] = {
        "ip": ip,
        "m_port": m_port,
        "p_port": p_port,
        "state": STATE_FREE,
        "addr": addr,
    }
    print(f"[Manager] Registered: {peer_name} @ {ip}  m-port={m_port}  p-port={p_port}")
    send_response(sock, addr, "SUCCESS")


def handle_setup_dht(parts, addr, sock):
    """setup-dht: pick n peers, mark building, return n tuples + year (leader first)."""
    global dht_exists, dht_building, dht_leader

    if len(parts) != 4:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name, n_s, _yyyy = parts
    try:
        n = int(n_s)
    except ValueError:
        send_response(sock, addr, "FAILURE bad-n")
        return

    if peer_name not in peers:
        send_response(sock, addr, "FAILURE not-registered")
        return
    if n < 3:
        send_response(sock, addr, "FAILURE n-too-small")
        return
    if dht_exists or dht_building or waiting_rebuilt or waiting_teardown:
        send_response(sock, addr, "FAILURE dht-exists")
        return

    free_peers = find_free_peers(exclude=peer_name)
    if len(free_peers) < n - 1:
        send_response(sock, addr, "FAILURE not-enough-peers")
        return

    chosen = random.sample(free_peers, n - 1)

    peers[peer_name]["state"] = STATE_LEADER
    for name in chosen:
        peers[name]["state"] = STATE_INDHT

    dht_building = True
    dht_leader = peer_name

    tuples = [f"{peer_name} {peers[peer_name]['ip']} {peers[peer_name]['p_port']}"]
    for name in chosen:
        tuples.append(f"{name} {peers[name]['ip']} {peers[name]['p_port']}")

    response = f"SUCCESS {n} {_yyyy} " + " ".join(tuples)
    print(f"[Manager] SETUP-DHT SUCCESS: leader={peer_name}, peers={[peer_name]+chosen}")
    send_response(sock, addr, response)


def handle_dht_complete(parts, addr, sock):
    """dht-complete: leader confirms ring loaded; DHT becomes queryable."""
    global dht_exists, dht_building, dht_size

    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if peer_name != dht_leader:
        print(f"[Manager] DHT-COMPLETE FAILURE: {peer_name} is not the leader")
        send_response(sock, addr, "FAILURE not-leader")
        return

    dht_building = False
    dht_exists = True
    dht_size = len(dht_member_names())
    print(f"[Manager] DHT is now active. Leader={peer_name}, size={dht_size}")
    send_response(sock, addr, "SUCCESS")


def handle_query_dht(parts, addr, sock):
    """query-dht: Free peer gets random entry node + ring size n for hashing."""
    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if not dht_exists or dht_building or waiting_rebuilt or waiting_teardown:
        send_response(sock, addr, "FAILURE dht-unavailable")
        return
    if peer_name not in peers:
        send_response(sock, addr, "FAILURE not-registered")
        return
    if peers[peer_name]["state"] != STATE_FREE:
        send_response(sock, addr, "FAILURE not-free")
        return

    members = dht_member_names()
    if not members:
        send_response(sock, addr, "FAILURE no-members")
        return

    pick = random.choice(members)
    info = peers[pick]
    response = f"SUCCESS {pick} {info['ip']} {info['p_port']} {dht_size}"
    print(f"[Manager] QUERY-DHT -> entry {pick} for requester {peer_name} (n={dht_size})")
    send_response(sock, addr, response)


def handle_leave_dht(parts, addr, sock):
    """leave-dht: member starts leave; block until matching dht-rebuilt."""
    global waiting_rebuilt, rebuilt_initiator, rebuilt_kind

    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if not dht_exists or waiting_rebuilt or waiting_teardown or dht_building:
        send_response(sock, addr, "FAILURE dht-unavailable")
        return
    if not peer_in_dht(peer_name):
        send_response(sock, addr, "FAILURE not-in-dht")
        return

    waiting_rebuilt = True
    rebuilt_initiator = peer_name
    rebuilt_kind = "leave"
    print(f"[Manager] LEAVE-DHT by {peer_name}; awaiting dht-rebuilt")
    send_response(sock, addr, "SUCCESS")


def handle_join_dht(parts, addr, sock):
    """join-dht: Free peer joins; return leader contact; block until dht-rebuilt."""
    global waiting_rebuilt, rebuilt_initiator, rebuilt_kind

    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if not dht_exists or waiting_rebuilt or waiting_teardown or dht_building:
        send_response(sock, addr, "FAILURE dht-unavailable")
        return
    if peer_name not in peers or peers[peer_name]["state"] != STATE_FREE:
        send_response(sock, addr, "FAILURE not-free")
        return
    if dht_leader is None or dht_leader not in peers:
        send_response(sock, addr, "FAILURE no-leader")
        return

    waiting_rebuilt = True
    rebuilt_initiator = peer_name
    rebuilt_kind = "join"

    li = peers[dht_leader]
    response = f"SUCCESS {dht_leader} {li['ip']} {li['p_port']}"
    print(f"[Manager] JOIN-DHT by {peer_name}; leader contact {dht_leader}; awaiting dht-rebuilt")
    send_response(sock, addr, response)


def handle_dht_rebuilt(parts, addr, sock):
    """dht-rebuilt: end churn phase; update leader/InDHT/Free per leave or join."""
    global waiting_rebuilt, rebuilt_initiator, rebuilt_kind, dht_leader, dht_size

    if len(parts) != 3:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name, new_leader = parts

    if not waiting_rebuilt or rebuilt_initiator is None or rebuilt_kind is None:
        send_response(sock, addr, "FAILURE not-waiting")
        return
    if peer_name != rebuilt_initiator:
        send_response(sock, addr, "FAILURE wrong-initiator")
        return
    if new_leader not in peers:
        send_response(sock, addr, "FAILURE bad-leader")
        return

    kind = rebuilt_kind
    old_members = set(dht_member_names())

    if kind == "leave":
        if peer_name not in old_members:
            send_response(sock, addr, "FAILURE not-in-dht")
            return
        remaining = old_members - {peer_name}
        if new_leader not in remaining:
            send_response(sock, addr, "FAILURE bad-leader")
            return
        waiting_rebuilt = False
        rebuilt_initiator = None
        rebuilt_kind = None
        peers[peer_name]["state"] = STATE_FREE
        for name in remaining:
            peers[name]["state"] = STATE_LEADER if name == new_leader else STATE_INDHT
        dht_leader = new_leader
        dht_size = len(remaining)
        print(f"[Manager] DHT rebuilt after LEAVE. New leader={new_leader}, size={dht_size}")
    else:
        if peer_name not in peers or peers[peer_name]["state"] != STATE_FREE:
            send_response(sock, addr, "FAILURE not-free")
            return
        new_members = old_members | {peer_name}
        if new_leader not in new_members:
            send_response(sock, addr, "FAILURE bad-leader")
            return
        waiting_rebuilt = False
        rebuilt_initiator = None
        rebuilt_kind = None
        for name in new_members:
            peers[name]["state"] = STATE_LEADER if name == new_leader else STATE_INDHT
        dht_leader = new_leader
        dht_size = len(new_members)
        print(f"[Manager] DHT rebuilt after JOIN. Leader={new_leader}, size={dht_size}")

    send_response(sock, addr, "SUCCESS")


def handle_deregister(parts, addr, sock):
    """deregister: remove a Free peer from the registry."""
    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if peer_name not in peers:
        send_response(sock, addr, "FAILURE not-registered")
        return
    st = peers[peer_name]["state"]
    if st == STATE_INDHT or st == STATE_LEADER:
        send_response(sock, addr, "FAILURE in-dht")
        return

    del peers[peer_name]
    print(f"[Manager] DEREGISTERED {peer_name}")
    send_response(sock, addr, "SUCCESS")


def handle_teardown_dht(parts, addr, sock):
    """teardown-dht: leader starts delete; block until teardown-complete."""
    global waiting_teardown

    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if not dht_exists or waiting_teardown or dht_building or waiting_rebuilt:
        send_response(sock, addr, "FAILURE dht-unavailable")
        return
    if peer_name != dht_leader:
        send_response(sock, addr, "FAILURE not-leader")
        return

    waiting_teardown = True
    print(f"[Manager] TEARDOWN-DHT by leader {peer_name}; awaiting teardown-complete")
    send_response(sock, addr, "SUCCESS")


def handle_teardown_complete(parts, addr, sock):
    """teardown-complete: all former DHT peers back to Free; no DHT."""
    global dht_exists, dht_leader, dht_size, waiting_teardown

    if len(parts) != 2:
        send_response(sock, addr, "FAILURE bad-args")
        return

    _, peer_name = parts

    if not waiting_teardown:
        send_response(sock, addr, "FAILURE not-waiting")
        return
    if peer_name != dht_leader:
        send_response(sock, addr, "FAILURE not-leader")
        return

    for name, info in list(peers.items()):
        if info["state"] in (STATE_LEADER, STATE_INDHT):
            peers[name]["state"] = STATE_FREE

    waiting_teardown = False
    dht_exists = False
    dht_leader = None
    dht_size = 0
    print("[Manager] Teardown complete; all former DHT peers are Free")
    send_response(sock, addr, "SUCCESS")


def dispatch(command, parts, addr, sock):
    """Route one parsed command string to its handler."""
    if command == "register":
        handle_register(parts, addr, sock)
    elif command == "setup-dht":
        handle_setup_dht(parts, addr, sock)
    elif command == "dht-complete":
        handle_dht_complete(parts, addr, sock)
    elif command == "query-dht":
        handle_query_dht(parts, addr, sock)
    elif command == "leave-dht":
        handle_leave_dht(parts, addr, sock)
    elif command == "join-dht":
        handle_join_dht(parts, addr, sock)
    elif command == "dht-rebuilt":
        handle_dht_rebuilt(parts, addr, sock)
    elif command == "deregister":
        handle_deregister(parts, addr, sock)
    elif command == "teardown-dht":
        handle_teardown_dht(parts, addr, sock)
    elif command == "teardown-complete":
        handle_teardown_complete(parts, addr, sock)
    else:
        print(f"[Manager] Unknown command: {command}")
        send_response(sock, addr, "FAILURE unknown-command")


def main():
    """Bind UDP port and process incoming manager messages forever."""
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

        if manager_busy_except(command):
            if dht_building and command != "dht-complete":
                send_response(sock, addr, "FAILURE dht-building")
            elif waiting_rebuilt and command != "dht-rebuilt":
                send_response(sock, addr, "FAILURE awaiting-dht-rebuilt")
            elif waiting_teardown and command != "teardown-complete":
                send_response(sock, addr, "FAILURE awaiting-teardown")
            continue

        dispatch(command, parts, addr, sock)


if __name__ == "__main__":
    main()