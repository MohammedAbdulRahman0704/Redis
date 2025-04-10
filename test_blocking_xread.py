import socket
import threading
import time

HOST = '127.0.0.1'
PORT = 6379

def send_command(sock, command):
    sock.sendall(f"{command}\r\n".encode())
    return sock.recv(4096).decode()

def xread_blocking_test():
    # Start a connection
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    # Add entry to stream so we can test XREAD
    print("Sending XADD")
    response = send_command(client, "XADD mystream * name alice age 30")
    print("XADD response:", response.strip())

    # XREAD with blocking; start a new thread to read it
    def blocking_xread():
        print("Sending blocking XREAD")
        client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client2.connect((HOST, PORT))
        client2.sendall(b"XREAD STREAMS mystream 0-0\r\n")
        response = client2.recv(4096).decode()
        print("XREAD blocking response:\n", response)
        client2.close()

    xread_thread = threading.Thread(target=blocking_xread)
    xread_thread.start()

    # Wait to simulate blocking
    time.sleep(2)

    # Push a new entry to unblock
    print("Sending another XADD to unblock XREAD")
    response = send_command(client, "XADD mystream * name bob age 25")
    print("Unblocking XADD response:", response.strip())

    # Wait for the thread to finish
    xread_thread.join()
    client.close()

if __name__ == "__main__":
    xread_blocking_test()