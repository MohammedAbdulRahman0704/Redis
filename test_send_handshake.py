#  Code when we execute replica in REPLCONF
# import socket

# def send_handshake():
#     print("🤝 Sending replica handshake...")

#     s = socket.create_connection(('localhost', 6379))  # Master server port
#     s.sendall(b"REPLCONF listening-port 6380\r\n")     # Replica's port
#     response = s.recv(4096).decode()
#     print("📥 Response:\n", response)
#     s.close()

# if __name__ == "__main__":
#     send_handshake()



#  Code when we execute replica in PSYNC
import socket

HOST = 'localhost'
PORT = 6379

with socket.create_connection((HOST, PORT)) as sock:
    def send(cmd):
        sock.sendall(cmd.encode() + b'\r\n')
        print("🤝 Sending:", cmd)

    def recv():
        data = sock.recv(4096)
        print("📥 Response:\n", data.decode())

    send("REPLCONF listening-port 6380")
    recv()

    send("REPLCONF capa psync2")
    recv()

    send("PSYNC ? -1")
    recv()