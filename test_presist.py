import socket

def send_cmd(cmd):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', 6379))
        s.sendall((cmd + '\r\n').encode())
        response = s.recv(4096).decode()
        print(f">>> {cmd}\n{response}")

# Let's test PERSIST functionality
send_cmd("SET mykey Hello EX 10")  # Set key with expiry
send_cmd("TTL mykey")              # Check TTL (should be ~10s)
send_cmd("PERSIST mykey")          # Remove expiry
send_cmd("TTL mykey")              # TTL should now be -1