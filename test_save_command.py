import socket

def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', 6379))
        s.sendall(command.encode())
        return s.recv(1024).decode()

def test_save():
    print("Setting key...")
    response1 = send_command("SET hello world\r\n")
    print("SET response:", response1)

    print("Saving to RDB...")
    response2 = send_command("SAVE\r\n")
    print("SAVE response:", response2)

    assert "+OK" in response2, "SAVE command failed"

if __name__ == "__main__":
    test_save()