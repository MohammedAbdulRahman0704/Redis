# tests/test_get_after_restart.py
import socket

def send_command(cmd):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("127.0.0.1", 6379))
        s.sendall(cmd.encode())
        return s.recv(1024).decode()

def test_get_after_restart():
    print("Testing key after restart...")
    resp = send_command("GET hello\r\n")
    print("GET response:", resp)

if __name__ == "__main__":
    test_get_after_restart()