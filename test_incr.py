# test_incr.py
import socket

def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("127.0.0.1", 6379))
        s.sendall(f"{command}\r\n".encode())
        return s.recv(1024).decode()

print("Initial SET: ", send_command("SET counter 10"))
print("INCR: ", send_command("INCR counter"))
print("INCR again: ", send_command("INCR counter"))
print("GET counter: ", send_command("GET counter"))
print("INCR new key: ", send_command("INCR newcounter"))  # should be 1
print("GET newcounter: ", send_command("GET newcounter"))