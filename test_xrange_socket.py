import socket

HOST = 'localhost'
PORT = 6379

def receive_all(sock):
    buffer = b""
    sock.settimeout(1.0)  # Timeout to prevent hanging
    try:
        while True:
            data = sock.recv(4096)
            if not data:
                break
            buffer += data
    except socket.timeout:
        pass  # Exit loop on timeout
    return buffer.decode()

def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((HOST, PORT))
            s.sendall((command.strip() + '\r\n').encode())
            response = receive_all(s)
            print(f">>> Command: {command.strip()}")
            print("<<< Response:\n", response.strip())
            print("-" * 50)
        except ConnectionRefusedError:
            print("❌ Could not connect to Redis. Is the server running on port 6379?")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    # Reading all entries in the stream
    send_command("XRANGE mystream - +")