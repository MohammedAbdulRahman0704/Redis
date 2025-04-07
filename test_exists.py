# test_exists.py
import socket

def send_command(cmd):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("127.0.0.1", 6379))
        s.sendall(f"{cmd}\r\n".encode())
        return s.recv(1024).decode()

print("SET key: ", send_command("SET mykey Hello"))
print("EXISTS mykey: ", send_command("EXISTS mykey"))
print("EXISTS otherkey: ", send_command("EXISTS otherkey"))