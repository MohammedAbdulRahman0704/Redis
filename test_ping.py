import socket

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", 6379))

for _ in range(3):
    client.sendall(b"PING\r\n")
    response = client.recv(1024)
    print("Response from server:", response.decode().strip())

client.close()