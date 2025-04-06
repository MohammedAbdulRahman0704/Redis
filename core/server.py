import socket
import threading

def handle_client(conn, addr):
    print(f"Client connected: {addr}")
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            command = data.decode().strip().upper()
            if command == "PING":
                conn.sendall(b"+PONG\r\n")
            else:
                conn.sendall(b"-ERR unknown command\r\n")
    finally:
        conn.close()

def start_server(host="127.0.0.1", port=6379):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Redis-like server is running on {host}:{port}...")

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()
