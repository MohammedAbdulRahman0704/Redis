import socket

def send_and_receive(command):
    s = socket.create_connection(('localhost', 6379))
    s.sendall(f"{command}\r\n".encode())
    response = s.recv(4096).decode()
    s.close()
    return response

def parse_resp_array(response):
    lines = response.strip().splitlines()
    if lines[0].startswith("*") and lines[1].startswith("$"):
        return lines[2], lines[4]  # e.g., "run_id", "<uuid>"
    return None, None

def test_replication_id_offset():
    print("🧪 Testing run_id and replication_offset...")

    run_id_response = send_and_receive("RUN_ID")
    print("📥 run_id Response:\n", run_id_response)

    label, run_id_value = parse_resp_array(run_id_response)
    assert label == "run_id", "❌ Expected 'run_id' label"
    assert len(run_id_value) > 10, "❌ run_id value seems too short"

    offset_response = send_and_receive("OFFSET")
    print("📥 offset Response:\n", offset_response)
    assert offset_response.startswith(":"), "❌ OFFSET did not return expected format"

    print("✅ run_id and replication_offset test passed!")

if __name__ == "__main__":
    test_replication_id_offset()