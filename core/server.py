import socket
import threading
import time
import uuid
from core.rdb_loader import load_rdb
from core.rdb_saver import save_rdb
from config import HOST, PORT, RDB_FILE, IS_REPLICA

data_store = {}
expiry_store = {}
streams = {}

run_id = str(uuid.uuid4())
replication_offset = 0
replica_clients = []

# Load RDB if it exists
data_store.update(load_rdb(RDB_FILE))


def propagate_to_replicas(command_str):
    for replica in replica_clients:
        try:
            replica.sendall(command_str.encode())
        except Exception as e:
            print(f"Failed to propagate to replica: {e}")


def handle_client(conn, addr):
    global replication_offset
    print(f"New connection from {addr}")
    is_replica = False

    while True:
        try:
            data = conn.recv(1024).decode().strip()
            if not data:
                break
            print(f"Received from {addr}: {data}")

            parts = data.split()
            if not parts:
                continue

            command = parts[0].upper()

            if command == "PING":
                conn.sendall(b"+PONG\r\n")

            elif command == "ECHO" and len(parts) > 1:
                message = ' '.join(parts[1:])
                conn.sendall(f"${len(message)}\r\n{message}\r\n".encode())

            elif command == "SET":
                if len(parts) < 3:
                    conn.sendall(b"-ERR wrong number of arguments for 'SET' command\r\n")
                    continue
                key = parts[1]
                value = parts[2]
                data_store[key] = value
                replication_offset += 1
                if len(parts) > 4 and parts[3].upper() == "EX":
                    try:
                        seconds = int(parts[4])
                        expiry_store[key] = time.time() + seconds
                    except ValueError:
                        conn.sendall(b"-ERR Invalid expiry time\r\n")
                        continue
                elif key in expiry_store:
                    expiry_store.pop(key)
                conn.sendall(b"+OK\r\n")
                if not is_replica:
                    propagate_to_replicas(data + "\r\n")

            elif command == "GET":
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'GET' command\r\n")
                    continue
                key = parts[1]
                if key in expiry_store and time.time() > expiry_store[key]:
                    data_store.pop(key, None)
                    expiry_store.pop(key, None)
                    conn.sendall(b"$-1\r\n")
                elif key in data_store:
                    value = data_store[key]
                    conn.sendall(f"${len(value)}\r\n{value}\r\n".encode())
                else:
                    conn.sendall(b"$-1\r\n")

            elif command == "TYPE":
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'TYPE' command\r\n")
                else:
                    key = parts[1]
                    if key not in data_store and key not in streams:
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
                    conn.sendall(b"-ERR wrong number of arguments for 'DEL' command\r\n")
                    continue
                key = parts[1]
                existed = key in data_store or key in streams
                data_store.pop(key, None)
                streams.pop(key, None)
                expiry_store.pop(key, None)
                if existed:
                    replication_offset += 1
                    conn.sendall(b":1\r\n")
                    if not is_replica:
                        propagate_to_replicas(data + "\r\n")
                else:
                    conn.sendall(b":0\r\n")

            elif command == "EXISTS":
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'EXISTS' command\r\n")
                    continue
                key = parts[1]
                conn.sendall(b":1\r\n" if key in data_store or key in streams else b":0\r\n")

            elif command == "TTL":
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'TTL' command\r\n")
                    continue
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
                    key = parts[1]
                    seconds = int(parts[2])
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
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'PERSIST' command\r\n")
                    continue
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
                if len(parts) != 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'INCR' command\r\n")
                    continue
                key = parts[1]
                if key in expiry_store and time.time() > expiry_store[key]:
                    data_store.pop(key, None)
                    expiry_store.pop(key, None)
                if key not in data_store:
                    data_store[key] = "0"
                try:
                    current = int(data_store[key])
                    current += 1
                    data_store[key] = str(current)
                    replication_offset += 1
                    conn.sendall(f":{current}\r\n".encode())
                    if not is_replica:
                        propagate_to_replicas(data + "\r\n")
                except ValueError:
                    conn.sendall(b"-ERR value is not an integer\r\n")

            elif command == "XADD":
                if len(parts) < 5:
                    conn.sendall(b"-ERR wrong number of arguments for 'XADD' command\r\n")
                    continue

                stream_name = parts[1]
                entry_id = parts[2]
                if entry_id == "*":
                    entry_id = f"{int(time.time() * 1000)}-0"

                field_value_parts = parts[3:]
                if len(field_value_parts) % 2 != 0:
                    conn.sendall(b"-ERR wrong number of field-value arguments\r\n")
                    continue

                fields = {}
                for i in range(0, len(field_value_parts), 2):
                    fields[field_value_parts[i]] = field_value_parts[i + 1]

                if stream_name not in streams:
                    streams[stream_name] = []

                streams[stream_name].append({
                    "id": entry_id,
                    "fields": fields
                })

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

            elif command == "XREAD":
                try:
                    block = None
                    count = None
                    streams_index = None

                    # Parse BLOCK and COUNT
                    i = 1
                    while i < len(parts):
                        if parts[i].upper() == "BLOCK":
                            block = int(parts[i + 1])
                            i += 2
                        elif parts[i].upper() == "COUNT":
                            count = int(parts[i + 1])
                            i += 2
                        elif parts[i].upper() == "STREAMS":
                            streams_index = i
                            break
                        else:
                            i += 1

                    if streams_index is None:
                        conn.sendall(b"-ERR missing STREAMS keyword\r\n")
                        continue

                    stream_names = parts[streams_index + 1 : streams_index + 1 + (len(parts) - streams_index - 1) // 2]
                    start_ids = parts[streams_index + 1 + len(stream_names):]

                    if len(stream_names) != len(start_ids):
                        conn.sendall(b"-ERR stream names and IDs count mismatch\r\n")
                        continue

                    start_time = time.time()

                    while True:
                        result = []
                        for stream_name, start_id in zip(stream_names, start_ids):
                            stream = streams.get(stream_name, [])
                            matching = [entry for entry in stream if entry["id"] > start_id]
                            if count is not None:
                                matching = matching[:count]

                            if matching:
                                result.append(f"${len(stream_name)}\r\n{stream_name}\r\n")
                                result.append(f"*{len(matching)}\r\n")
                                for entry in matching:
                                    entry_id = entry["id"]
                                    fields = entry["fields"]
                                    result.append(f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n")
                                    result.append(f"*{len(fields) * 2}\r\n")
                                    for k, v in fields.items():
                                        result.append(f"${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n")

                        if result:
                            conn.sendall(f"*{len(result)}\r\n".encode() + "".join(result).encode())
                            break

                        if block is None:
                            # No blocking and no result
                            conn.sendall(b"$-1\r\n")
                            break

                        if block >= 0 and (time.time() - start_time) * 1000 >= block:
                            # Timeout reached
                            conn.sendall(b"$-1\r\n")
                            break

                        time.sleep(0.1)  # Prevents tight loop

                except Exception as e:
                    conn.sendall(f"-ERR XREAD error: {str(e)}\r\n".encode())


            elif command == "SAVE":
                save_rdb(RDB_FILE, data_store)
                conn.sendall(b"+OK\r\n")

            elif command == "RUN_ID":
                response = f"*2\r\n$7\r\nrun_id\r\n${len(run_id)}\r\n{run_id}\r\n"
                conn.sendall(response.encode())

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
                # Basic REPLCONF support
                conn.sendall(b"+OK\r\n")
                is_replica = True
                replica_clients.append(conn)

            else:
                conn.sendall(b"-ERR unknown command\r\n")

        except Exception as e:
            print(f"Exception handling client {addr}: {e}")
            break

    conn.close()
    print(f"Connection closed: {addr}")


def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print(f"Server listening on {HOST}:{PORT}")

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    start_server()