# core/rdb_saver.py

import io

def write_string(f, s):
    encoded = s.encode()
    f.write(bytes([len(encoded)]))  # write 1-byte length
    f.write(encoded)

def save_rdb(filename, db, return_bytes=False):
    output = io.BytesIO() if return_bytes else open(filename, 'wb')

    try:
        for key, value in db.items():
            output.write(b'\x00')  # type marker for string
            write_string(output, key)
            write_string(output, value)
        output.write(b'\xFF')  # end of file marker

        if return_bytes:
            return output.getvalue()
    finally:
        if not return_bytes:
            output.close()