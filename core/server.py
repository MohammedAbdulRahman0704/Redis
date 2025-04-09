import socket
import threading
import time
import uuid
from core.rdb_loader import load_rdb
from core.rdb_saver import save_rdb
from config import HOST, PORT, RDB_FILE, IS_REPLICA

data_store = {}
expiry_store = {}


run_id = str(uuid.uuid4())
replication_offset = 0

def handle_client(conn, addr):
    global replication_offset
    print(f"New connection from {addr}")
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
                conn.sendall(f"{message}\r\n".encode())

            elif command == "SET":
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

            elif command == "GET":
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

            elif command == "DEL":
                key = parts[1]
                existed = key in data_store
                data_store.pop(key, None)
                expiry_store.pop(key, None)
                if existed:
                    replication_offset += 1  
                    conn.sendall(b"+OK\r\n")
                else:
                    conn.sendall(b"$-1\r\n")

            elif command == "EXISTS":
                key = parts[1]
                conn.sendall(b":1\r\n" if key in data_store else b":0\r\n")

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
                key = parts[1]
                try:
                    seconds = int(parts[2])
                    if key in data_store:
                        expiry_store[key] = time.time() + seconds
                        replication_offset += 1 
                        conn.sendall(b":1\r\n")
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
                else:
                    conn.sendall(b":0\r\n")

            elif command == "INFO":
                role = "replica" if IS_REPLICA else "master"
                info = (
                    "# Server\r\n"
                    "redis_version:0.1\r\n"
                    f"connected_clients:{threading.active_count() - 1}\r\n"
                    f"role:{role}\r\n"
                )
                conn.sendall(f"${len(info)}\r\n{info}".encode())

            elif parts[0].upper() == "REPLCONF":
                if len(parts) >= 3 and parts[1].lower() == "listening-port":
                    port = parts[2]
                    print(f"Replica handshake received. Replica is listening on port {port}")
                    conn.sendall(b"+OK\r\n")
                else:
                    conn.sendall(b"-ERR unknown REPLCONF subcommand\r\n")

            elif command == "INCR":
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
                except ValueError:
                    conn.sendall(b"-ERR value is not an integer\r\n")

            elif command == "SAVE":
                save_rdb(RDB_FILE, data_store)
                conn.sendall(b"+OK\r\n")

            elif command == "RUN_ID":
                response = f"*2\r\n$7\r\nrun_id\r\n${len(run_id)}\r\n{run_id}\r\n"
                conn.sendall(response.encode())

            elif command == "OFFSET":
                conn.sendall(f":{replication_offset}\r\n".encode())

            else:
                conn.sendall(b"-ERR unknown command\r\n")

        except Exception as e:
            print(f"Error: {e}")
            break
    conn.close()

def start_server():
    global data_store
    data_store = load_rdb(RDB_FILE)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen()

    print(f"Redis-like server is running on {HOST}:{PORT}...")

    while True:
        client_socket, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket, addr))
        client_thread.start()

if __name__ == "__main__":
    start_server()