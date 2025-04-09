import socket

def send_handshake():
    print("🤝 Sending replica handshake...")

    s = socket.create_connection(('localhost', 6379))  # Master server port
    s.sendall(b"REPLCONF listening-port 6380\r\n")     # Replica's port
    response = s.recv(4096).decode()
    print("📥 Response:\n", response)
    s.close()

if __name__ == "__main__":
    send_handshake()