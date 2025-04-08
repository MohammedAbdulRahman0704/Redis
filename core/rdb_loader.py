# core/rdb_loader.py
def read_string(f):
    length = f.read(1)
    if not length:
        return None
    length = ord(length)
    return f.read(length).decode()

def load_rdb(filename):
    db = {}
    try:
        with open(filename, 'rb') as f:
            while True:
                type_byte = f.read(1)
                if not type_byte:
                    break
                if type_byte == b'\x00':  # String type
                    key = read_string(f)
                    value = read_string(f)
                    db[key] = value
                elif type_byte == b'\xFF':  # End of file marker
                    break
    except FileNotFoundError:
        print(f"[INFO] No RDB file found at {filename}. Starting fresh.")
    return db