import socket
import threading

def handle_client(conn, addr):
    print(f"New connection from {addr}")
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            command = data.decode().strip().upper()
            print(f"Received from {addr}: {command}")
            if command == "PING":
                conn.sendall(b"+PONG\r\n")
            else:
                conn.sendall(b"-ERR unknown command\r\n")

def start_server():
    host = '127.0.0.1'
    port = 6379
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    print(f"Redis-like server is running on {host}:{port}...")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    start_server()