# core/rdb_saver.py

def write_string(f, s):
    encoded = s.encode()
    f.write(bytes([len(encoded)]))  # write 1-byte length
    f.write(encoded)

def save_rdb(filename, db):
    with open(filename, 'wb') as f:
        for key, value in db.items():
            f.write(b'\x00')  # type marker for string
            write_string(f, key)
            write_string(f, value)
        f.write(b'\xFF')  # end of file marker