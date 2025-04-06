import socket

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", 6379))

client.sendall(b'ECHO "Hello World"\r\n')
response = client.recv(1024).decode().strip()
print("Response from server:", response)

client.close()