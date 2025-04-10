import socket

def send_command(sock, command):
    sock.sendall((command + '\r\n').encode())
    return sock.recv(4096).decode()

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 6379))  # Make sure PORT is correct

    print("▶️ Setting key: foo")
    print(send_command(sock, "SET foo bar"))

    print("🔍 TYPE for key: foo")
    print(send_command(sock, "TYPE foo"))

    print("🔍 TYPE for non-existent key: nope")
    print(send_command(sock, "TYPE nope"))

    sock.close()

if __name__ == "__main__":
    main()