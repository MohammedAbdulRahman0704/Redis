import socket
import threading

def handle_client(conn, addr):
    print(f"New connection from {addr}")
    while True:
        try:
            data = conn.recv(1024).decode().strip()
            if not data:
                break
            print(f"Received from {addr}: {data}")
            if data == "PING":
                response = "+PONG\r\n"
            elif data.startswith("ECHO"):
                message = data[5:].strip().strip('"')
                response = f"{message}\r\n"
            else:
                response = "-Unknown command\r\n"
            conn.sendall(response.encode())
        except ConnectionResetError:
            break
    conn.close()

def start_server(host="127.0.0.1", port=6379):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Redis-like server is running on {host}:{port}...")
    while True:
        conn, addr = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()