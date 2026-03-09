#!/usr/bin/env python3
"""
CSE 434 - Socket Programming Project
DHT Peer (Milestone: register, setup-dht, dht-complete)
Group 81 - Port range: 40500 ~ 40999

用法:
    python3 peer.py <manager-ip> <manager-port>

然后从 stdin 输入命令，例如:
    register Alice 127.0.0.1 40501 40502
    setup-dht Alice 3 1950
    dht-complete Alice
"""

import socket
import sys
import csv
import os

# ──────────────────────────────────────────────
# 常量
# ──────────────────────────────────────────────
BUFFER_SIZE = 65536   # 大一些，setup-dht 回复可能很长


# ──────────────────────────────────────────────
# 工具函数
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
    """返回大于 n 的第一个素数"""
    candidate = n + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate


def load_csv(year):
    """
    读取 details-YYYY.csv，返回记录列表。
    每条记录是一个 dict（14个字段）。
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
# DHT 本地存储
# ──────────────────────────────────────────────
# local_hash_table[pos] = record  (pos = event_id mod s)
local_hash_table = {}
my_id      = None   # 本节点在 ring 中的 id
ring_size  = None   # ring 大小 n
right_neighbour = None  # (ip, p_port)  右邻居
peer_tuples = []    # 所有 peer 的 3-tuple 列表: [(name, ip, p_port), ...]


# ──────────────────────────────────────────────
# 向 Manager 发消息并等待回复
# ──────────────────────────────────────────────

def send_to_manager(sock, manager_addr, message):
    sock.sendto(message.encode(), manager_addr)
    print(f"[Peer] --> Manager: {message}")
    data, _ = sock.recvfrom(BUFFER_SIZE)
    response = data.decode().strip()
    print(f"[Peer] <-- Manager: {response}")
    return response


# ──────────────────────────────────────────────
# 向另一个 Peer 发消息（UDP，不等回复）
# ──────────────────────────────────────────────

def send_to_peer(sock, ip, port, message):
    sock.sendto(message.encode(), (ip, port))
    print(f"[Peer] --> Peer({ip}:{port}): {message[:120]}")


# ──────────────────────────────────────────────
# 发送 store 命令（沿 ring 转发记录）
# ──────────────────────────────────────────────

def forward_store(sock, target_id, pos, record):
    """
    把 store 命令发给右邻居，让它沿 ring 转发直到 target_id。
    消息格式:
        store <target_id> <pos> <field1>|<field2>|...|<field14>
    """
    fields = list(record.values())
    record_str = "|".join(str(f) for f in fields)
    msg = f"store {target_id} {pos} {record_str}"
    ip, port = right_neighbour
    send_to_peer(sock, ip, port, msg)


# ──────────────────────────────────────────────
# 处理收到的 Peer 消息（在 setup 阶段）
# ──────────────────────────────────────────────

def handle_peer_message(sock, data):
    """
    处理从其他 peer 收到的消息。
    返回 True 表示继续，False 表示结束等待。
    """
    global my_id, ring_size, right_neighbour, peer_tuples, local_hash_table

    message = data.decode().strip()
    print(f"[Peer] <-- Peer: {message[:120]}")
    parts = message.split()
    if not parts:
        return True

    command = parts[0].lower()

    # ── set-id 命令 ──────────────────────────────
    # 格式: set-id <id> <n> peer0 ip0 p0 peer1 ip1 p1 ...
    if command == "set-id":
        my_id     = int(parts[1])
        ring_size = int(parts[2])
        # 解析所有 peer 3-tuples
        peer_tuples = []
        idx = 3
        while idx + 2 < len(parts):
            peer_tuples.append((parts[idx], parts[idx+1], int(parts[idx+2])))
            idx += 3

        # 右邻居是 id = (my_id + 1) mod ring_size
        right_id = (my_id + 1) % ring_size
        right_name, right_ip, right_port = peer_tuples[right_id]
        right_neighbour = (right_ip, right_port)
        print(f"[Peer] Set id={my_id}, ring_size={ring_size}, right_neighbour={right_neighbour}")
        return False   # set-id 收到后不需要继续等待

    # ── store 命令 ──────────────────────────────
    # 格式: store <target_id> <pos> <record_str>
    elif command == "store":
        target_id = int(parts[1])
        pos       = int(parts[2])
        record_str = parts[3] if len(parts) > 3 else ""

        if target_id == my_id:
            # 存入本地
            local_hash_table[pos] = record_str
        else:
            # 继续转发给右邻居
            ip, port = right_neighbour
            send_to_peer(sock, ip, port, message)
        return True   # 继续监听

    # ── rebuild-dht 命令（Milestone 不需要，预留）──
    elif command == "rebuild-dht":
        print(f"[Peer] rebuild-dht received (not implemented in milestone)")
        return True

    else:
        print(f"[Peer] Unknown peer command: {command}")
        return True


# ──────────────────────────────────────────────
# Leader 执行 setup-dht 后续步骤
# ──────────────────────────────────────────────

def leader_setup_dht(sock, m_port, p_port, manager_addr, n, year, tuples):
    """
    Leader 在收到 setup-dht SUCCESS 后执行:
    1. 发送 set-id 给其他所有 peer
    2. 读取 CSV，分发 store 命令
    3. 打印每个节点存了多少条记录
    4. 发送 dht-complete 给 manager
    """
    global my_id, ring_size, right_neighbour, peer_tuples, local_hash_table

    my_id     = 0
    ring_size = n
    peer_tuples = tuples  # list of (name, ip, p_port)

    right_id = (my_id + 1) % ring_size
    right_name, right_ip, right_port = peer_tuples[right_id]
    right_neighbour = (right_ip, right_port)

    # 1. 发送 set-id 给 peer 1 .. n-1
    tuples_str = " ".join(f"{name} {ip} {pp}" for name, ip, pp in peer_tuples)
    for i in range(1, n):
        name, ip, pp = peer_tuples[i]
        msg = f"set-id {i} {n} {tuples_str}"
        send_to_peer(sock, ip, pp, msg)

    # 2. 读取 CSV
    print(f"[Peer/Leader] Loading details-{year}.csv ...")
    records = load_csv(year)
    if records is None:
        print("[Peer/Leader] ABORT: CSV file not found.")
        return

    l = len(records)
    s = next_prime(2 * l)
    print(f"[Peer/Leader] Total records: {l}, hash table size s={s}")

    # 每个节点的记录计数（用于打印）
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
            # 沿 ring 转发
            fields = list(record.values())
            record_str = "|".join(str(f) for f in fields)
            msg = f"store {target_id} {pos} {record_str}"
            rip, rport = right_neighbour
            send_to_peer(sock, rip, rport, msg)

    # 3. 打印统计
    print("\n[Peer/Leader] ── DHT Record Distribution ──")
    for i, name_ip_port in enumerate(peer_tuples):
        print(f"  Node {i} ({name_ip_port[0]}): {count[i]} records")
    print()

    # 4. 发送 dht-complete
    my_name = peer_tuples[0][0]
    response = send_to_manager(sock, manager_addr, f"dht-complete {my_name}")
    if response.startswith("SUCCESS"):
        print("[Peer/Leader] DHT setup complete!")
    else:
        print(f"[Peer/Leader] dht-complete FAILED: {response}")


# ──────────────────────────────────────────────
# 非 Leader peer 等待 set-id
# ──────────────────────────────────────────────

def peer_wait_for_set_id(sock):
    """非 leader peer 注册后进入等待，直到收到 set-id"""
    print("[Peer] Waiting for set-id from leader...")
    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        keep_going = handle_peer_message(sock, data)
        if not keep_going:
            # set-id 已处理，继续监听 store 命令
            print("[Peer] Now listening for store commands...")
            peer_listen_for_stores(sock)
            return


def peer_listen_for_stores(sock):
    """
    非 leader peer 监听 store 命令。
    因为 Milestone 没有明确的结束信号，
    我们设置一个短 timeout，
    当一段时间没有消息时认为 setup 完成。
    """
    sock.settimeout(3.0)   # 3 秒没收到消息则认为完成
    while True:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            handle_peer_message(sock, data)
        except socket.timeout:
            print(f"[Peer] Store phase complete. Stored {len(local_hash_table)} records locally.")
            sock.settimeout(None)
            return


# ──────────────────────────────────────────────
# 主程序
# ──────────────────────────────────────────────

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <manager-ip> <manager-port>")
        sys.exit(1)

    manager_ip   = sys.argv[1]
    manager_port = int(sys.argv[2])
    manager_addr = (manager_ip, manager_port)

    # peer 自己的 socket（m_port 和 p_port 从 register 命令里读取）
    # 我们用同一个 socket 绑定 m_port（manager通信），
    # 另一个 socket 绑定 p_port（peer间通信）
    # 为简单起见，Milestone 中我们先用同一个 socket

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

            # 创建 manager socket
            sock_m = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock_m.bind(("", m_port))

            # 创建 peer socket
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

            # 解析回复：SUCCESS <n> <yyyy> name0 ip0 p0 name1 ip1 p1 ...
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

        # ── dht-complete（手动）─────────────────
        elif command == "dht-complete":
            if not registered:
                print("[Peer] ERROR: must register first")
                continue
            if len(parts) != 2:
                print("Usage: dht-complete <peer-name>")
                continue
            _, peer_name = parts
            response = send_to_manager(sock_m, manager_addr, f"dht-complete {peer_name}")

        # ── wait（让非 leader peer 进入等待）──────
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
