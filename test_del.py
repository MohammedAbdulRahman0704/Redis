import socket
import time

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", 6379))

# Set key
client.sendall(b"SET tempKey DeleteMe\r\n")
print("SET response:", client.recv(1024).decode())

# Delete key
client.sendall(b"DEL tempKey\r\n")
print("DEL response:", client.recv(1024).decode())

# Try GET after delete
client.sendall(b"GET tempKey\r\n")
print("GET after DEL:", client.recv(1024).decode())

client.close()