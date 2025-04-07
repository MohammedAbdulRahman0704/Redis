import socket

def send_cmd(cmd):
    s = socket.socket()
    s.connect(("localhost", 6379))
    s.sendall((cmd + "\r\n").encode())
    resp = s.recv(1024).decode()
    s.close()
    return resp.strip()

print("SET key:", send_cmd("SET tempkey tempval"))
print("EXPIRE tempkey 3:", send_cmd("EXPIRE tempkey 3"))
print("TTL tempkey:", send_cmd("TTL tempkey"))
print("EXPIRE unknownkey 5:", send_cmd("EXPIRE unknownkey 5"))