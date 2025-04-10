import socket

HOST = '127.0.0.1'  # Redis server IP
PORT = 6379         # Redis server port

def send_command(command):
    """Sends a raw Redis command and prints the response."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((HOST, PORT))
            s.sendall(command.encode())
            response = s.recv(4096).decode()
            print(f">>> Command: {command.strip()}")
            print(f"<<< Response: {response.strip()}")
        except Exception as e:
            print(f"[Error] {e}")
    print('-' * 50)

if __name__ == "__main__":
    # Add entries to the stream
    send_command("XADD mystream * temperature 25 humidity 60\r\n")
    send_command("XADD mystream * temperature 26 humidity 65\r\n")

    # Check type of the key to confirm it's a stream
    send_command("TYPE mystream\r\n")

    # Try GET command (should return error or nil for stream)
    send_command("GET mystream\r\n")

    # Optional: Read from the stream
    send_command("XRANGE mystream - +\r\n")