# core/server.py

import socket
import threading
import time
from core.rdb_loader import load_rdb
from core.rdb_saver import save_rdb
from config import HOST, PORT, RDB_FILE

data_store = {}
expiry_store = {}

def handle_client(conn, addr):
    print(f"New connection from {addr}")
    while True:
        try:
            data = conn.recv(1024).decode().strip()
            if not data:
                break
            print(f"Received from {addr}: {data}")

            parts = data.split()
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
                conn.sendall(b"+OK\r\n" if existed else b"$-1\r\n")
            elif command == "EXISTS":
                key = parts[1]
                if key in data_store:
                    conn.sendall(b":1\r\n")
                else:
                    conn.sendall(b":0\r\n")
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
                        conn.sendall(b":1\r\n")
                    else:
                        conn.sendall(b":0\r\n")
                except (IndexError, ValueError):
                    conn.sendall(b"-ERR invalid arguments\r\n")
            elif command == "PERSIST":
                key = parts[1]
                if key in data_store and key in expiry_store:
                    expiry_store.pop(key)
                    conn.sendall(b":1\r\n")
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
                    current = int(data_store[key])
                    current += 1
                    data_store[key] = str(current)
                    conn.sendall(f":{current}\r\n".encode())
                except ValueError:
                    conn.sendall(b"-ERR value is not an integer\r\n")
            elif command == "SAVE":
                save_rdb(RDB_FILE, data_store)
                conn.sendall(b"+OK\r\n")
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