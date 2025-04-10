import socket
import threading
import time
import uuid
import traceback
from core.rdb_loader import load_rdb
from core.rdb_saver import save_rdb
from config import HOST, PORT, RDB_FILE, IS_REPLICA

# Core data structures
data_store = {}
expiry_store = {}
streams = {}

# Replication identifiers
run_id = str(uuid.uuid4())
replication_offset = 0
replica_clients = []


def propagate_to_replicas(command_str):
    """Send command to all connected replicas."""
    for replica in replica_clients:
        try:
            replica.sendall(command_str.encode())
        except Exception as e:
            print(f"[REPLICA ERROR] Failed to propagate to replica: {e}")


def handle_client(conn, addr):
    """Handles individual client connection."""
    global replication_offset
    print(f"[NEW CONNECTION] {addr}")
    is_replica = False

    try:
        while True:
            data = conn.recv(1024).decode().strip()
            if not data:
                break

            print(f"[RECEIVED] From {addr}: {data}")
            parts = data.split()
            if not parts:
                continue

            command = parts[0].upper()

            if command == "PING":
                conn.sendall(b"+PONG\r\n")

            elif command == "ECHO" and len(parts) > 1:
                message = ' '.join(parts[1:])
                conn.sendall(f"{message}\r\n".encode())

            elif command == "SET":
                if len(parts) < 3:
                    conn.sendall(b"-ERR wrong number of arguments for 'SET'\r\n")
                    continue
                key, value = parts[1], parts[2]
                data_store[key] = value
                replication_offset += 1

                # Handle EX expiration
                if len(parts) > 4 and parts[3].upper() == "EX":
                    try:
                        seconds = int(parts[4])
                        expiry_store[key] = time.time() + seconds
                    except ValueError:
                        conn.sendall(b"-ERR Invalid expiry time\r\n")
                        continue
                else:
                    expiry_store.pop(key, None)

                conn.sendall(b"+OK\r\n")
                if not is_replica:
                    propagate_to_replicas(data + "\r\n")

            elif command == "GET":
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'GET'\r\n")
                    continue
                key = parts[1]

                if key in expiry_store and time.time() > expiry_store[key]:
                    data_store.pop(key, None)
                    expiry_store.pop(key, None)

                if key in data_store:
                    value = data_store[key]
                    conn.sendall(f"${len(value)}\r\n{value}\r\n".encode())
                else:
                    conn.sendall(b"$-1\r\n")

            elif command == "TYPE":
                key = parts[1] if len(parts) == 2 else None
                if not key:
                    conn.sendall(b"-ERR wrong number of arguments for 'TYPE'\r\n")
                elif key not in data_store and key not in streams:
                    conn.sendall(b"+none\r\n")
                elif isinstance(data_store.get(key), str):
                    conn.sendall(b"+string\r\n")
                elif isinstance(data_store.get(key), list):
                    conn.sendall(b"+list\r\n")
                elif isinstance(data_store.get(key), dict):
                    conn.sendall(b"+hash\r\n")
                elif key in streams:
                    conn.sendall(b"+stream\r\n")
                else:
                    conn.sendall(b"+unknown\r\n")

            elif command == "DEL":
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'DEL'\r\n")
                    continue
                key = parts[1]
                existed = key in data_store or key in streams
                data_store.pop(key, None)
                streams.pop(key, None)
                expiry_store.pop(key, None)

                if existed:
                    replication_offset += 1
                    conn.sendall(b"+OK\r\n")
                    if not is_replica:
                        propagate_to_replicas(data + "\r\n")
                else:
                    conn.sendall(b"$-1\r\n")

            elif command == "EXISTS":
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'EXISTS'\r\n")
                    continue
                key = parts[1]
                exists = int(key in data_store or key in streams)
                conn.sendall(f":{exists}\r\n".encode())

            elif command == "TTL":
                key = parts[1]
                if key not in data_store:
                    conn.sendall(b":-2\r\n")
                elif key in expiry_store:
                    ttl = int(expiry_store[key] - time.time())
                    if ttl < 0:
                        data_store.pop(key, None)
                        expiry_store.pop(key, None)
                        conn.sendall(b":-2\r\n")
                    else:
                        conn.sendall(f":{ttl}\r\n".encode())
                else:
                    conn.sendall(b":-1\r\n")

            elif command == "EXPIRE":
                try:
                    key, seconds = parts[1], int(parts[2])
                    if key in data_store:
                        expiry_store[key] = time.time() + seconds
                        replication_offset += 1
                        conn.sendall(b":1\r\n")
                        if not is_replica:
                            propagate_to_replicas(data + "\r\n")
                    else:
                        conn.sendall(b":0\r\n")
                except (IndexError, ValueError):
                    conn.sendall(b"-ERR invalid arguments\r\n")

            elif command == "PERSIST":
                key = parts[1]
                if key in data_store and key in expiry_store:
                    expiry_store.pop(key)
                    replication_offset += 1
                    conn.sendall(b":1\r\n")
                    if not is_replica:
                        propagate_to_replicas(data + "\r\n")
                else:
                    conn.sendall(b":0\r\n")

            elif command == "INCR":
                key = parts[1]
                if key in expiry_store and time.time() > expiry_store[key]:
                    data_store.pop(key, None)
                    expiry_store.pop(key, None)

                if key not in data_store:
                    data_store[key] = "0"
                try:
                    current = int(data_store[key]) + 1
                    data_store[key] = str(current)
                    replication_offset += 1
                    conn.sendall(f":{current}\r\n".encode())
                    if not is_replica:
                        propagate_to_replicas(data + "\r\n")
                except ValueError:
                    conn.sendall(b"-ERR value is not an integer\r\n")

            elif command == "XADD":
                if len(parts) < 5:
                    conn.sendall(b"-ERR wrong number of arguments for 'XADD'\r\n")
                    continue
                stream_name, entry_id = parts[1], parts[2]
                entry_id = f"{int(time.time() * 1000)}-0" if entry_id == "*" else entry_id
                field_value_parts = parts[3:]

                if len(field_value_parts) % 2 != 0:
                    conn.sendall(b"-ERR wrong number of field-value arguments\r\n")
                    continue

                fields = {field_value_parts[i]: field_value_parts[i + 1] for i in range(0, len(field_value_parts), 2)}
                streams.setdefault(stream_name, []).append({"id": entry_id, "fields": fields})

                replication_offset += 1
                conn.sendall(f"${len(entry_id)}\r\n{entry_id}\r\n".encode())
                if not is_replica:
                    propagate_to_replicas(data + "\r\n")

            elif command == "XRANGE":
                if len(parts) != 4:
                    conn.sendall(b"-ERR wrong number of arguments for 'XRANGE'\r\n")
                    continue

                stream_name, start, end = parts[1], parts[2], parts[3]
                stream = streams.get(stream_name, [])

                matching = [entry for entry in stream if (start == "-" or entry["id"] >= start) and (end == "+" or entry["id"] <= end)]
                response = f"*{len(matching)}\r\n"
                for entry in matching:
                    entry_id = entry["id"]
                    fields = entry["fields"]
                    response += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n"
                    response += f"*{len(fields) * 2}\r\n"
                    for k, v in fields.items():
                        response += f"${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n"
                conn.sendall(response.encode())

            elif command == "SAVE":
                save_rdb(RDB_FILE, data_store)
                conn.sendall(b"+OK\r\n")

            elif command == "RUN_ID":
                conn.sendall(f"*2\r\n$7\r\nrun_id\r\n${len(run_id)}\r\n{run_id}\r\n".encode())

            elif command == "OFFSET":
                conn.sendall(f":{replication_offset}\r\n".encode())

            elif command == "INFO":
                role = "replica" if IS_REPLICA else "master"
                info = (
                    "# Server\r\n"
                    "redis_version:0.1\r\n"
                    f"connected_clients:{threading.active_count() - 1}\r\n"
                    f"role:{role}\r\n"
                )
                conn.sendall(f"${len(info)}\r\n{info}".encode())

            elif command == "REPLCONF":
                if len(parts) >= 3 and parts[1].lower() == "listening-port":
                    is_replica = True
                    replica_clients.append(conn)
                    print(f"[REPLCONF] Replica connected on port {parts[2]}")
                    conn.sendall(b"+OK\r\n")
                elif len(parts) >= 3 and parts[1].lower() == "capa" and parts[2].lower() == "psync2":
                    print("[REPLCONF] Replica supports PSYNC2")
                    conn.sendall(b"+OK\r\n")
                else:
                    conn.sendall(b"-ERR unknown REPLCONF subcommand\r\n")

            elif command == "PSYNC":
                if len(parts) == 3 and parts[1] == "?" and parts[2] == "-1":
                    conn.sendall(f"+FULLRESYNC {run_id} {replication_offset}\r\n".encode())
                    rdb_data = save_rdb(RDB_FILE, data_store, return_bytes=True)
                    conn.sendall(f"${len(rdb_data)}\r\n".encode())
                    conn.sendall(rdb_data)
                else:
                    conn.sendall(b"-ERR PSYNC not supported for partial resync\r\n")

            else:
                conn.sendall(b"-ERR unknown command\r\n")

    except Exception as e:
        print(f"[ERROR] {e}")
        traceback.print_exc()

    finally:
        if is_replica and conn in replica_clients:
            replica_clients.remove(conn)
        conn.close()
        print(f"[DISCONNECTED] {addr}")


def start_server():
    """Initializes server socket and accepts client connections."""
    global data_store
    data_store = load_rdb(RDB_FILE)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    print(f"[SERVER] Running on {HOST}:{PORT}...")

    while True:
        client_socket, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket, addr)).start()


if __name__ == "__main__":
    start_server()