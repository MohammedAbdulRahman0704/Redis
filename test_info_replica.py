# test_info_replica.py

import socket
from config import IS_REPLICA

def test_info_replica():
    print("🧪 Testing INFO command as a replica...")

    if not IS_REPLICA:
        print("⚠️  Skipped: IS_REPLICA is False. Set it to True in config.py to test this.")
        return

    s = socket.socket()
    s.connect(("127.0.0.1", 6379))
    s.sendall(b"INFO\r\n")

    response = s.recv(4096).decode()
    print("📥 Response:")
    print(response)

    assert "role:replica" in response, "❌ Replica info not returned correctly."
    print("✅ INFO replica test passed!")
    s.close()

if __name__ == "__main__":
    test_info_replica()