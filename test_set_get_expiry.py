# test_set_get_expiry.py
import socket
import time

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", 6379))

client.sendall(b"SET temp Abdul EX 3\r\n")
response = client.recv(1024).decode()
print("SET with expiry response:", response.strip())

client.sendall(b"GET temp\r\n")
response = client.recv(1024).decode()
print("GET immediately:", response.strip())

time.sleep(4)

client.sendall(b"GET temp\r\n")
response = client.recv(1024).decode()
print("GET after 4 seconds:", response.strip())

client.close()