# test_info_command.py

import socket

def send_command(command):
    with socket.create_connection(("127.0.0.1", 6379)) as sock:
        sock.sendall((command + "\r\n").encode())
        response = sock.recv(4096).decode()
        return response

def test_info_command():
    print("🧪 Testing INFO command...")
    response = send_command("INFO")
    print("📥 Response:")
    print(response)

    assert "redis_version:" in response, "❌ Missing redis_version"
    assert "connected_clients:" in response, "❌ Missing connected_clients"
    assert "role:" in response, "❌ Missing role"

    print("✅ INFO command test passed!")

if __name__ == "__main__":
    test_info_command()