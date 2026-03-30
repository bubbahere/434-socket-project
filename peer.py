#!/usr/bin/env python3
"""
CSE 434 - DHT Peer (UDP: manager port + peer port)
Group 81 - Port range: 40500-40999

Usage: python3 peer.py <manager-ip> <manager-port>
"""

import base64
import csv
import json
import os
import random
import socket
import sys
import threading
import time

BUFFER_SIZE = 65535

# Registration / sockets
my_name = None
my_ip = None
my_m_port = None
my_p_port = None
manager_addr = None
sock_m = None
sock_p = None

# Ring + data
local_hash_table = {}
my_id = None
ring_size = None
right_neighbour = None
peer_tuples = []
dht_year = None

# Synchronization
p2p_lock = threading.Lock()
listener_thread = None
listener_stop = threading.Event()

# Coordination events
query_result = None
query_event = threading.Event()
leave_teardown_done = threading.Event()
leave_reset_done = threading.Event()
leave_rebuild_done = threading.Event()
join_rebuild_done = threading.Event()


def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5) + 1, 2):
        if n % i == 0:
            return False
    return True


def next_prime(n):
    candidate = n + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate


def event_id_from_record(rec):
    v = rec.get("EVENT_ID") or rec.get("event_id")
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def encode_record(rec):
    return base64.b64encode(json.dumps(rec, sort_keys=True).encode()).decode("ascii")


def decode_record_blob(blob):
    try:
        raw = base64.b64decode(blob.encode())
        return json.loads(raw.decode())
    except Exception:
        return None


def load_csv(year):
    filename = f"details-{year}.csv"
    if not os.path.exists(filename):
        print(f"[Peer] ERROR: {filename} not found.")
        return None
    records = []
    with open(filename, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    return records


def send_to_manager(message):
    sock_m.sendto(message.encode(), manager_addr)
    print(f"[Peer] --> Manager: {message}")
    data, _ = sock_m.recvfrom(BUFFER_SIZE)
    response = data.decode().strip()
    print(f"[Peer] <-- Manager: {response}")
    return response


def send_to_peer(ip, port, message):
    sock_p.sendto(message.encode(), (ip, int(port)))
    preview = message if len(message) <= 140 else message[:137] + "..."
    print(f"[Peer] --> Peer({ip}:{port}): {preview}")


def tuples_to_json_list(tuples):
    return [[t[0], t[1], int(t[2])] for t in tuples]


def tuples_from_json_list(lst):
    return [(x[0], x[1], int(x[2])) for x in lst]


def clear_dht_storage():
    global local_hash_table, my_id, ring_size, right_neighbour, peer_tuples, dht_year
    local_hash_table = {}
    my_id = None
    ring_size = None
    right_neighbour = None
    peer_tuples = []
    dht_year = None


def handle_store(message):
    global local_hash_table
    parts = message.split(None, 3)
    if len(parts) < 4:
        return
    _, target_s, pos_s, blob = parts
    target_id = int(target_s)
    pos = int(pos_s)
    rec = decode_record_blob(blob)
    if rec is None:
        rec = {"_legacy": blob.split("|")}
    with p2p_lock:
        if target_id == my_id:
            local_hash_table[pos] = rec
        else:
            ip, port = right_neighbour
            send_to_peer(ip, port, message)


def handle_set_id(message):
    """
    Format: set-id <id> <n> <year> <name0> <ip0> <p0> ...
    Year is included so all peers know dht_year.
    """
    global my_id, ring_size, right_neighbour, peer_tuples, local_hash_table, dht_year
    parts = message.split()
    if len(parts) < 5:
        return False
    new_id = int(parts[1])
    n = int(parts[2])
    year = parts[3]
    tuples = []
    idx = 4
    while idx + 2 < len(parts):
        tuples.append((parts[idx], parts[idx + 1], int(parts[idx + 2])))
        idx += 3
    with p2p_lock:
        my_id = new_id
        ring_size = n
        peer_tuples = tuples
        dht_year = year
        rid = (my_id + 1) % ring_size
        _, rip, rpp = peer_tuples[rid]
        right_neighbour = (rip, rpp)
        local_hash_table = {}
    print(f"[Peer] set-id: id={my_id}, ring={ring_size}, year={year}, right={right_neighbour}")
    return False


def handle_teardown_ring(message):
    global local_hash_table
    parts = message.split()
    if len(parts) != 3:
        return
    _, origin, rem_s = parts
    remaining = int(rem_s)
    with p2p_lock:
        local_hash_table = {}
        rip, rpp = right_neighbour
        myn = my_name
    print(f"[Peer] teardown-ring: cleared table (origin={origin}, rem={remaining})")
    if remaining > 0:
        send_to_peer(rip, rpp, f"DHT_TD {origin} {remaining - 1}")
    elif myn == origin:
        leave_teardown_done.set()


def handle_reset_chain(message):
    global my_id, ring_size, right_neighbour, peer_tuples, local_hash_table
    if not message.startswith("DHT_RST "):
        return
    payload = message[8:].strip()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        print("[Peer] DHT_RST bad json")
        return
    n = int(data["n"])
    tuples = tuples_from_json_list(data["tuples"])
    ret_ip = data.get("ret_ip")
    ret_port = int(data.get("ret_port", 0))

    with p2p_lock:
        peer_tuples = tuples
        ring_size = n
        my_id = None
        for i, t in enumerate(peer_tuples):
            if t[0] == my_name:
                my_id = i
                break
        if my_id is None:
            print("[Peer] DHT_RST: name not in tuple list")
            return
        rid = (my_id + 1) % ring_size
        _, rip, rpp = peer_tuples[rid]
        right_neighbour = (rip, rpp)
        local_hash_table = {}

    print(f"[Peer] DHT_RST: id={my_id}, ring={ring_size}, right={right_neighbour}")

    if my_id < ring_size - 1:
        nxt = json.dumps(data, separators=(",", ":"))
        send_to_peer(rip, rpp, f"DHT_RST {nxt}")
    elif ret_ip and ret_port:
        send_to_peer(ret_ip, ret_port, "DHT_RST_DONE")


def handle_rebuild_cmd(message):
    global dht_year
    parts = message.split()
    if len(parts) < 4:
        return
    year, nip, nport = parts[1], parts[2], parts[3]
    notify = (nip, int(nport))
    with p2p_lock:
        if my_id != 0:
            return
    dht_year = year
    print(f"[Peer] Rebuilding DHT from details-{year}.csv ...")
    leader_redistribute_csv(year)
    send_to_peer(notify[0], notify[1], "DHT_DONE")


def leader_redistribute_csv(year):
    global local_hash_table
    records = load_csv(year)
    if records is None:
        return
    l = len(records)
    s = next_prime(2 * l)
    with p2p_lock:
        n = ring_size
        rip, rpp = right_neighbour
        local_hash_table = {}
    counts = [0] * n
    for record in records:
        eid = event_id_from_record(record)
        if eid is None:
            continue
        pos = eid % s
        tid = pos % n
        counts[tid] += 1
        blob = encode_record(record)
        if tid == my_id:
            with p2p_lock:
                local_hash_table[pos] = record
        else:
            send_to_peer(rip, rpp, f"store {tid} {pos} {blob}")
    print("[Peer] Redistribution counts:", counts)


def build_find_payload(eid, s, n, tgt, sender_name, sender_ip, sender_port, id_seq, remain):
    return {
        "eid": int(eid),
        "s": int(s),
        "n": int(n),
        "tgt": int(tgt),
        "sender": [sender_name, sender_ip, int(sender_port)],
        "id_seq": list(id_seq),
        "remain": list(remain),
    }


def send_find_reply(sender_ip, sender_port, ok, eid, id_seq, record):
    body = {"ok": ok, "eid": eid, "id_seq": id_seq}
    if ok and record is not None:
        body["record"] = record
    msg = "DHT_FINDR " + json.dumps(body, separators=(",", ":"))
    send_to_peer(sender_ip, sender_port, msg)


def handle_find_json(message):
    if not message.startswith("DHT_FIND "):
        return
    try:
        payload = json.loads(message[9:].strip())
    except json.JSONDecodeError:
        return
    eid = int(payload["eid"])
    s = int(payload["s"])
    n = int(payload["n"])
    tgt = int(payload["tgt"])
    sn, sip, spp = payload["sender"]
    id_seq = list(payload.get("id_seq", []))
    remain = list(payload.get("remain", []))

    pos = eid % s
    tgt_check = pos % n
    if tgt_check != tgt:
        tgt = tgt_check

    with p2p_lock:
        mid = my_id
        tuples = list(peer_tuples)

    if mid is None or not tuples:
        send_find_reply(sip, spp, False, eid, id_seq, None)
        return

    def addr_for_id(i):
        _, ip, pp = tuples[i]
        return ip, pp

    if mid == tgt:
        with p2p_lock:
            rec = local_hash_table.get(pos)
        if rec and not rec.get("_legacy"):
            er = event_id_from_record(rec)
            if er == eid:
                if not id_seq or id_seq[-1] != tgt:
                    id_seq = id_seq + [tgt]
                send_find_reply(sip, spp, True, eid, id_seq, rec)
                return
        send_find_reply(sip, spp, False, eid, id_seq, None)
        return

    if not id_seq:
        id_seq = [tgt]

    visited = set(id_seq)
    candidates = [i for i in range(n) if i != tgt and i not in visited]
    if candidates:
        nxt = random.choice(candidates)
        id_seq = id_seq + [nxt]
        new_remain = [i for i in range(n) if i != tgt and i not in set(id_seq)]
        payload_out = build_find_payload(eid, s, n, tgt, sn, sip, spp, id_seq, new_remain)
        ip, pp = addr_for_id(nxt)
        send_to_peer(ip, pp, "DHT_FIND " + json.dumps(payload_out, separators=(",", ":")))
        return

    ip, pp = addr_for_id(tgt)
    if not id_seq or id_seq[-1] != tgt:
        id_seq = id_seq + [tgt]
    payload_out = build_find_payload(eid, s, n, tgt, sn, sip, spp, id_seq, [])
    send_to_peer(ip, pp, "DHT_FIND " + json.dumps(payload_out, separators=(",", ":")))


def handle_find_reply(message):
    global query_result
    if not message.startswith("DHT_FINDR "):
        return
    try:
        body = json.loads(message[10:].strip())
    except json.JSONDecodeError:
        return
    query_result = body
    query_event.set()


def handle_join_start(message):
    parts = message.split()
    if len(parts) != 4:
        return
    _, jname, jip, jpp = parts
    threading.Thread(
        target=run_join_as_leader,
        args=(jname, jip, int(jpp)),
        daemon=True,
    ).start()


def run_join_as_leader(jname, jip, jpp):
    global peer_tuples, my_id, ring_size, right_neighbour, local_hash_table, dht_year
    leave_teardown_done.clear()
    with p2p_lock:
        if my_id != 0 or ring_size is None:
            print("[Peer] join-start ignored (not leader)")
            return
        tuples = list(peer_tuples)
        n = ring_size
        y = dht_year
    if y is None:
        print("[Peer] join-start: no dht_year")
        return

    print(f"[Peer] Join: integrating {jname} into ring of {n} ...")
    rip, rpp = right_neighbour
    send_to_peer(rip, rpp, f"DHT_TD {my_name} {n - 1}")

    deadline = time.time() + 120.0
    while time.time() < deadline:
        if leave_teardown_done.wait(timeout=0.5):
            break
    leave_teardown_done.clear()

    new_tuples = tuples + [(jname, jip, int(jpp))]
    n_new = n + 1
    # Include year in set-id
    tuples_str = " ".join(f"{a} {b} {c}" for a, b, c in new_tuples)
    for i in range(1, n_new):
        name, ip, pp = new_tuples[i]
        send_to_peer(ip, pp, f"set-id {i} {n_new} {y} {tuples_str}")

    with p2p_lock:
        my_id = 0
        ring_size = n_new
        peer_tuples = new_tuples
        _, rr_ip, rr_pp = peer_tuples[1 % n_new]
        right_neighbour = (rr_ip, rr_pp)
        local_hash_table = {}

    time.sleep(1.0)
    leader_redistribute_csv(y)
    send_to_peer(jip, int(jpp), "DHT_JOIN_DONE")
    print("[Peer] Join integration complete.")


def handle_join_done(_message):
    join_rebuild_done.set()


def handle_rst_done(_message):
    leave_reset_done.set()


def handle_done(_message):
    leave_rebuild_done.set()


def dispatch_p2p(text):
    if text.startswith("store "):
        handle_store(text)
    elif text.startswith("set-id "):
        handle_set_id(text)
    elif text.startswith("DHT_TD "):
        handle_teardown_ring(text)
    elif text.startswith("DHT_RST "):
        handle_reset_chain(text)
    elif text.startswith("DHT_REBUILD "):
        handle_rebuild_cmd(text)
    elif text.startswith("DHT_FIND "):
        handle_find_json(text)
    elif text.startswith("DHT_FINDR "):
        handle_find_reply(text)
    elif text.startswith("DHT_JOIN "):
        handle_join_start(text)
    elif text == "DHT_JOIN_DONE":
        handle_join_done(text)
    elif text == "DHT_RST_DONE":
        handle_rst_done(text)
    elif text == "DHT_DONE":
        handle_done(text)
    else:
        print(f"[Peer] Unknown P2P: {text[:80]}")


def p2p_listener():
    while not listener_stop.is_set():
        try:
            data, _addr = sock_p.recvfrom(BUFFER_SIZE)
            text = data.decode().strip()
            print(f"[Peer] <-- P2P: {text[:120]}{'...' if len(text) > 120 else ''}")
            dispatch_p2p(text)
        except OSError:
            break
        except Exception as ex:
            print(f"[Peer] listener error: {ex}")


def start_listener():
    global listener_thread
    if listener_thread is not None and listener_thread.is_alive():
        return
    listener_stop.clear()
    listener_thread = threading.Thread(target=p2p_listener, daemon=True)
    listener_thread.start()


def ensure_listener():
    if sock_p is None:
        return
    start_listener()


def leader_setup_dht(n, year, tuples):
    global my_id, ring_size, right_neighbour, peer_tuples, local_hash_table, dht_year
    dht_year = year
    my_id = 0
    ring_size = n
    peer_tuples = list(tuples)
    _, rip, rpp = peer_tuples[(my_id + 1) % ring_size]
    right_neighbour = (rip, rpp)

    # Include year in set-id so all peers know it
    tuples_str = " ".join(f"{name} {ip} {pp}" for name, ip, pp in peer_tuples)
    for i in range(1, n):
        name, ip, pp = peer_tuples[i]
        send_to_peer(ip, pp, f"set-id {i} {n} {year} {tuples_str}")

    records = load_csv(year)
    if records is None:
        print("[Peer/Leader] ABORT: CSV missing")
        return
    l = len(records)
    s = next_prime(2 * l)
    counts = [0] * n
    local_hash_table = {}
    for record in records:
        eid = event_id_from_record(record)
        if eid is None:
            continue
        pos = eid % s
        tid = pos % n
        counts[tid] += 1
        blob = encode_record(record)
        if tid == my_id:
            local_hash_table[pos] = record
        else:
            send_to_peer(rip, rpp, f"store {tid} {pos} {blob}")

    print("\n[Peer/Leader] DHT Record Distribution:")
    for i, (name, _, _) in enumerate(peer_tuples):
        print(f"  Node {i} ({name}): {counts[i]} records")
    print()

    resp = send_to_manager(f"dht-complete {peer_tuples[0][0]}")
    if resp.startswith("SUCCESS"):
        print("[Peer/Leader] DHT setup complete!")
        start_listener()
    else:
        print(f"[Peer/Leader] dht-complete FAILED: {resp}")


def peer_wait_for_set_id():
    print("[Peer] Waiting for set-id from leader...")
    while True:
        data, _ = sock_p.recvfrom(BUFFER_SIZE)
        text = data.decode().strip()
        if text.startswith("set-id "):
            handle_set_id(text)
            break
        dispatch_p2p(text)
    print("[Peer] Now receiving store phase ...")
    sock_p.settimeout(45.0)
    try:
        while True:
            data, _ = sock_p.recvfrom(BUFFER_SIZE)
            dispatch_p2p(data.decode().strip())
    except socket.timeout:
        pass
    finally:
        sock_p.settimeout(None)
    with p2p_lock:
        nloc = len(local_hash_table)
    print(f"[Peer] Store phase complete. Local records: {nloc}")
    start_listener()


def cmd_query_dht(event_id_s, year_s=None):
    global query_result
    ensure_listener()
    eid = int(event_id_s)
    resp = send_to_manager(f"query-dht {my_name}")
    if not resp.startswith("SUCCESS"):
        print(f"[Peer] query-dht failed: {resp}")
        return
    rp = resp.split()
    if len(rp) < 5:
        print("[Peer] query: bad manager response")
        return
    pip, ppp, ns = rp[2], rp[3], rp[4]
    n = int(ns)
    y = year_s or dht_year
    if y is None:
        print("[Peer] query-dht: pass year as second argument: query-dht <id> <YYYY>")
        return
    records = load_csv(str(y))
    if not records:
        print(f"[Peer] Missing details-{y}.csv for query hashing")
        return
    s = next_prime(2 * len(records))
    pos = eid % s
    tgt = pos % n
    payload = build_find_payload(eid, s, n, tgt, my_name, my_ip, my_p_port, [], [i for i in range(n) if i != tgt])
    query_result = None
    query_event.clear()
    send_to_peer(pip, int(ppp), "DHT_FIND " + json.dumps(payload, separators=(",", ":")))
    if query_event.wait(timeout=60.0):
        body = query_result or {}
        if body.get("ok") and body.get("record"):
            rec = body.get("record") or {}
            labels = [
                ("event id",          "EVENT_ID"),
                ("state",             "STATE"),
                ("year",              "YEAR"),
                ("month name",        "MONTH_NAME"),
                ("event type",        "EVENT_TYPE"),
                ("cz type",           "CZ_TYPE"),
                ("cz name",           "CZ_NAME"),
                ("injuries direct",   "INJURIES_DIRECT"),
                ("injuries indirect", "INJURIES_INDIRECT"),
                ("deaths direct",     "DEATHS_DIRECT"),
                ("deaths indirect",   "DEATHS_INDIRECT"),
                ("damage property",   "DAMAGE_PROPERTY"),
                ("damage crops",      "DAMAGE_CROPS"),
                ("tor f scale",       "TOR_F_SCALE"),
            ]
            for label, key in labels:
                val = rec.get(key) or rec.get(key.lower()) or ""
                print(f"{label}: {val}")
            print(f"id-seq: {','.join(str(x) for x in body.get('id_seq', []))}")
        else:
            print(f"Storm event {eid} not found in the DHT.")
    else:
        print("[Peer] query timeout")


def cmd_leave_dht():
    ensure_listener()
    resp = send_to_manager(f"leave-dht {my_name}")
    if not resp.startswith("SUCCESS"):
        print(f"[Peer] leave-dht failed: {resp}")
        return

    leave_teardown_done.clear()
    leave_reset_done.clear()
    leave_rebuild_done.clear()

    with p2p_lock:
        n = ring_size
        rip, rpp = right_neighbour
        order = []
        idx = (my_id + 1) % n
        for _ in range(n - 1):
            order.append(peer_tuples[idx])
            idx = (idx + 1) % n
        year = dht_year  # Save year before clearing

    send_to_peer(rip, rpp, f"DHT_TD {my_name} {n - 1}")

    if not leave_teardown_done.wait(timeout=120.0):
        print("[Peer] leave: teardown timeout")
        return
    leave_teardown_done.clear()

    tuples_json = {
        "n": n - 1,
        "tuples": tuples_to_json_list(order),
        "ret_ip": my_ip,
        "ret_port": my_p_port,
    }
    nl_ip, nl_pp = order[0][1], order[0][2]
    send_to_peer(nl_ip, int(nl_pp), "DHT_RST " + json.dumps(tuples_json, separators=(",", ":")))

    if not leave_reset_done.wait(timeout=120.0):
        print("[Peer] leave: reset timeout")
        return
    leave_reset_done.clear()

    new_leader = order[0][0]
    send_to_peer(nl_ip, int(nl_pp), f"DHT_REBUILD {year} {my_ip} {my_p_port}")

    if not leave_rebuild_done.wait(timeout=180.0):
        print("[Peer] leave: rebuild timeout")
        return
    leave_rebuild_done.clear()

    r2 = send_to_manager(f"dht-rebuilt {my_name} {new_leader}")
    print(f"[Peer] leave complete: {r2}")
    clear_dht_storage()


def cmd_join_dht():
    ensure_listener()
    resp = send_to_manager(f"join-dht {my_name}")
    if not resp.startswith("SUCCESS"):
        print(f"[Peer] join-dht failed: {resp}")
        return
    parts = resp.split()
    if len(parts) < 4:
        print("[Peer] join: bad manager response")
        return
    lname, lip, lpp = parts[1], parts[2], parts[3]

    join_rebuild_done.clear()
    send_to_peer(lip, int(lpp), f"DHT_JOIN {my_name} {my_ip} {my_p_port}")

    if not join_rebuild_done.wait(timeout=300.0):
        print("[Peer] join: timeout waiting for ring")
        return

    r2 = send_to_manager(f"dht-rebuilt {my_name} {lname}")
    print(f"[Peer] join complete: {r2}")


def cmd_teardown_dht():
    ensure_listener()
    with p2p_lock:
        if my_id != 0:
            print("[Peer] Only leader may teardown-dht")
            return
        n = ring_size
        rip, rpp = right_neighbour
        lname = my_name
    resp = send_to_manager(f"teardown-dht {my_name}")
    if not resp.startswith("SUCCESS"):
        print(f"[Peer] teardown-dht failed: {resp}")
        return
    leave_teardown_done.clear()
    send_to_peer(rip, rpp, f"DHT_TD {lname} {n - 1}")
    if not leave_teardown_done.wait(timeout=120.0):
        print("[Peer] teardown: timeout")
        return
    leave_teardown_done.clear()
    r2 = send_to_manager(f"teardown-complete {lname}")
    print(f"[Peer] teardown-complete: {r2}")
    clear_dht_storage()


def cmd_deregister():
    resp = send_to_manager(f"deregister {my_name}")
    if resp.startswith("SUCCESS"):
        print("[Peer] Deregistered. Exiting.")
        listener_stop.set()
        sys.exit(0)
    print(f"[Peer] deregister failed: {resp}")


def main():
    global my_name, my_ip, my_m_port, my_p_port, manager_addr, sock_m, sock_p

    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <manager-ip> <manager-port>")
        sys.exit(1)

    manager_addr = (sys.argv[1], int(sys.argv[2]))

    print("[Peer] Commands: register | setup-dht | wait | query-dht | leave-dht | join-dht | teardown-dht | deregister | exit")

    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split()
        cmd = parts[0].lower()

        if cmd == "register":
            if len(parts) != 5:
                print("Usage: register <n> <IPv4> <m-port> <p-port>")
                continue
            _, name, ip, mps, pps = parts
            my_name, my_ip = name, ip
            my_m_port, my_p_port = int(mps), int(pps)
            sock_m = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock_m.bind(("", my_m_port))
            sock_p = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock_p.bind(("", my_p_port))
            r = send_to_manager(f"register {my_name} {my_ip} {my_m_port} {my_p_port}")
            if r.startswith("SUCCESS"):
                print(f"[Peer] Registered as '{my_name}'")
            else:
                sock_m.close()
                sock_p.close()
                sock_m = sock_p = None

        elif cmd == "setup-dht":
            if sock_m is None:
                print("[Peer] register first")
                continue
            if len(parts) != 4:
                print("Usage: setup-dht <n> <n> <YYYY>")
                continue
            _, pname, ns, yyyy = parts
            n = int(ns)
            r = send_to_manager(f"setup-dht {pname} {n} {yyyy}")
            if not r.startswith("SUCCESS"):
                print(f"[Peer] setup-dht failed: {r}")
                continue
            rp = r.split()
            tuples = []
            i = 3
            while i + 2 < len(rp):
                tuples.append((rp[i], rp[i + 1], int(rp[i + 2])))
                i += 3
            if pname == my_name:
                leader_setup_dht(n, yyyy, tuples)
            else:
                print("[Peer] Not the setup leader; use 'wait' on other peers.")

        elif cmd == "wait":
            if sock_p is None:
                print("[Peer] register first")
                continue
            peer_wait_for_set_id()

        elif cmd == "query-dht":
            if sock_m is None or len(parts) < 2:
                print("Usage: query-dht <event-id> [YYYY]")
                continue
            yr = parts[2] if len(parts) >= 3 else None
            cmd_query_dht(parts[1], yr)

        elif cmd == "leave-dht":
            if sock_m is None:
                print("[Peer] register first")
                continue
            cmd_leave_dht()

        elif cmd == "join-dht":
            if sock_m is None:
                print("[Peer] register first")
                continue
            cmd_join_dht()

        elif cmd == "teardown-dht":
            if sock_m is None:
                print("[Peer] register first")
                continue
            cmd_teardown_dht()

        elif cmd == "deregister":
            if sock_m is None:
                print("[Peer] register first")
                continue
            cmd_deregister()

        elif cmd == "exit":
            listener_stop.set()
            break
        else:
            print(f"[Peer] Unknown: {cmd}")

    if sock_m:
        sock_m.close()
    if sock_p:
        sock_p.close()


if __name__ == "__main__":
    main()