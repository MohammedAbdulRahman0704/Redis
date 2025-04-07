import socket

def send_command(command):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", 6379))
    client.sendall((command + "\r\n").encode())
    response = client.recv(4096).decode()
    client.close()
    return response.strip()

# Test SET command
print("SET command response:", send_command("SET name Abdul"))

# Test GET command
print("GET command response:", send_command("GET name"))

# Test GET for unknown key
print("GET (unknown key) response:", send_command("GET unknown"))