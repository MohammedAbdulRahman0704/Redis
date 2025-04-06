# core/server.py

import socket

def start_server(host='127.0.0.1', port=6379):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Redis-like server is running on {host}:{port}...")

    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Accepted connection from {client_address}")

        data = client_socket.recv(1024).decode().strip()

        if data.upper() == "PING":
            response = "+PONG\r\n"
        else:
            response = "-Error: Unknown command\r\n"

        client_socket.sendall(response.encode())
        client_socket.close()