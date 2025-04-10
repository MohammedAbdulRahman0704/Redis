import socket
import time  # add this

def send_command(sock, command):
    print(f"> {command.strip()}")
    sock.sendall((command + "\r\n").encode())

    def recv_line():
        line = b""
        while not line.endswith(b"\r\n"):
            part = sock.recv(1)
            if not part:
                break
            line += part
        return line.decode().strip()

    def parse_response():
        line = recv_line()
        if not line:
            return None
        if line.startswith("+"):
            return line[1:]
        elif line.startswith("-"):
            return f"Error: {line}"
        elif line.startswith(":"):
            return int(line[1:])
        elif line.startswith("$"):
            length = int(line[1:])
            if length == -1:
                return None
            data = b""
            while len(data) < length + 2:
                data += sock.recv(length + 2 - len(data))
            return data[:-2].decode()
        elif line.startswith("*"):
            count = int(line[1:])
            items = []
            for _ in range(count):
                items.append(parse_response())
            return items
        else:
            return f"Unknown: {line}"

    response = parse_response()

    if isinstance(response, list):
        print("Array:")
        for item in response:
            print(f"  {item}")
    else:
        print(response)

    return response

def main():
    HOST = "127.0.0.1"
    PORT = 6379

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))

        send_command(sock, "MULTI")
        time.sleep(0.1)

        send_command(sock, "SET name rahman")
        time.sleep(0.1)

        send_command(sock, "SET city chennai")
        time.sleep(0.1)

        send_command(sock, "GET name")
        time.sleep(0.1)

        send_command(sock, "GET city")
        time.sleep(0.1)

        send_command(sock, "EXEC")
        time.sleep(0.1)

        send_command(sock, "GET name")
        send_command(sock, "GET city")
        send_command(sock, "DISCARD")

if __name__ == "__main__":
    main()