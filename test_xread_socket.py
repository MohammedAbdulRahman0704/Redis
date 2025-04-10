import socket

HOST = 'localhost'
PORT = 6379

def receive_all(sock):
    """Receives all data from the socket until it's closed or times out."""
    buffer = b""
    while True:
        try:
            data = sock.recv(4096)
            if not data:
                break
            buffer += data
        except socket.timeout:
            break
    return buffer.decode()

def encode_redis_command(*args):
    """Encodes arguments into the Redis RESP protocol."""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
    return cmd.encode()

def send_command(command_args):
    """Connects to Redis, sends a command, and prints the response."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(5)  # 5-second timeout
        try:
            s.connect((HOST, PORT))
            s.sendall(encode_redis_command(*command_args))
            response = receive_all(s)
            print(f">>> Command: {' '.join(command_args)}")
            print("<<< Response:\n", response)
            print("-" * 40)
        except ConnectionRefusedError:
            print("❌ Redis server not running at port 6379.")
        except socket.timeout:
            print("❌ Timeout while communicating with Redis server.")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    # Modify the command below if you want to test with a different one
    send_command(["XREAD", "COUNT", "2", "STREAMS", "mystream", "0"])