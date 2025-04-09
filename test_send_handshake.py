#  Code when we execute replica in REPLCONF
# import socket

# def send_handshake():
#     print("🤝 Sending replica handshake...")

#     s = socket.create_connection(('localhost', 6379))  # Master server port
#     s.sendall(b"REPLCONF listening-port 6380\r\n")     # Replica's port
#     response = s.recv(4096).decode()
#     print("📥 Response:\n", response)
#     s.close()

# if __name__ == "__main__":
#     send_handshake()



#  Code when we execute replica in PSYNC
# import socket

# HOST = 'localhost'
# PORT = 6379

# with socket.create_connection((HOST, PORT)) as sock:
#     def send(cmd):
#         sock.sendall(cmd.encode() + b'\r\n')
#         print("🤝 Sending:", cmd)

#     def recv():
#         data = sock.recv(4096)
#         print("📥 Response:\n", data.decode())

#     send("REPLCONF listening-port 6380")
#     recv()

#     send("REPLCONF capa psync2")
#     recv()

#     send("PSYNC ? -1")
#     recv()



#  Code when we execute with parsing FULLRESYNC
import socket

REPLICA_PORT = 6380
MASTER_HOST = 'localhost'
MASTER_PORT = 6379

def send_and_receive(sock, message):
    print(f"\n🤝 Sending: {message}")
    sock.sendall((message + '\r\n').encode())
    response = sock.recv(4096).decode()
    print(f"📥 Response:\n{response}")
    return response

def connect_to_master():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)  # Optional timeout to avoid hanging
    sock.connect((MASTER_HOST, MASTER_PORT))
    return sock

def parse_fullresync(response):
    lines = response.strip().split()
    if len(lines) == 3 and lines[0] == "+FULLRESYNC":
        run_id = lines[1]
        offset = int(lines[2])
        print(f"✅ FULLRESYNC received. Run ID: {run_id}, Offset: {offset}")
        return run_id, offset
    else:
        print("❌ Invalid FULLRESYNC response.")
        return None, None

def receive_rdb(sock):
    try:
        # Read the $<length>\r\n header
        header = b''
        while not header.endswith(b'\r\n'):
            header += sock.recv(1)
        header_str = header.decode()
        if not header_str.startswith('$'):
            print("❌ Invalid RDB header.")
            return

        length = int(header_str[1:].strip())
        print(f"📦 Expecting RDB of {length} bytes")

        # Receive the RDB data
        received = b''
        while len(received) < length:
            chunk = sock.recv(min(4096, length - len(received)))
            if not chunk:
                print("⚠️ Connection closed before receiving full RDB.")
                break
            received += chunk

        print(f"✅ RDB received ({len(received)} bytes)")

        # Save RDB to file
        with open("replica_dump.rdb", "wb") as f:
            f.write(received)
        print("💾 RDB saved as 'replica_dump.rdb'")

    except Exception as e:
        print(f"❌ Error while receiving RDB: {e}")

def main():
    try:
        sock = connect_to_master()

        send_and_receive(sock, f"REPLCONF listening-port {REPLICA_PORT}")
        send_and_receive(sock, "REPLCONF capa psync2")

        response = send_and_receive(sock, "PSYNC ? -1")
        run_id, offset = parse_fullresync(response)

        if run_id is not None:
            receive_rdb(sock)
        else:
            print("❌ Aborting due to missing FULLRESYNC info.")

    except Exception as e:
        print(f"❌ Connection error: {e}")
    finally:
        sock.close()
        print("🔒 Connection closed.")

if __name__ == "__main__":
    main()