import socket
import threading

data_store = {}

def handle_client(client_socket, address):
    print(f"New connection from {address}")
    while True:
        try:
            data = client_socket.recv(1024).decode().strip()
            if not data:
                break
            print(f"Received from {address}: {data}")
            parts = data.split()
            if not parts:
                continue

            command = parts[0].upper()

            if command == "PING":
                client_socket.sendall(b"+PONG\r\n")
            elif command == "ECHO":
                message = " ".join(parts[1:])
                client_socket.sendall(f"{message}\r\n".encode())
            elif command == "SET" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                data_store[key] = value
                client_socket.sendall(b"+OK\r\n")
            elif command == "GET" and len(parts) == 2:
                key = parts[1]
                value = data_store.get(key)
                if value is not None:
                    client_socket.sendall(f"${len(value)}\r\n{value}\r\n".encode())
                else:
                    client_socket.sendall(b"$-1\r\n")
            else:
                client_socket.sendall(b"-ERR unknown command\r\n")
        except ConnectionResetError:
            break

    client_socket.close()

# ✅ Wrap this in a function
def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 6379))
    server.listen(5)
    print("Redis-like server is running on 127.0.0.1:6379...")

    while True:
        client_socket, addr = server.accept()
        client_handler = threading.Thread(target=handle_client, args=(client_socket, addr))
        client_handler.start()