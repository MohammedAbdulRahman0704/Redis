import socket

def send_command(cmd):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 6379))
        s.sendall(cmd.encode())
        return s.recv(1024).decode()

print("SET key with expiry: ", send_command("SET ttlkey somevalue EX 5\r\n"))

print("TTL ttlkey: ", send_command("TTL ttlkey\r\n"))

print("SET key without expiry: ", send_command("SET nonexpiring value\r\n"))

print("TTL nonexpiring: ", send_command("TTL nonexpiring\r\n"))

print("TTL nonexistent: ", send_command("TTL nothinghere\r\n"))