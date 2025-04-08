# Redis-like Python Server

A lightweight Redis clone built in Python, supporting basic commands, concurrency, and RDB-style persistence.

---

## ✅ Completed Features

- ✅ Bind to a port  
- ✅ Respond to `PING`  
- ✅ Respond to multiple `PING`s  
- ✅ Handle concurrent clients using `threading`  
- ✅ Implement the `ECHO` command  
- ✅ Support `SET`, `GET`, `DEL`, `EXISTS`  
- ✅ Support key expiry with `EX`, `EXPIRE`, `TTL`, `PERSIST`  
- ✅ Support atomic counter via `INCR`  
- ✅ RDB-style persistence:  
  - `SAVE` command to write data to `dump.rdb`  
  - Load data automatically from `dump.rdb` on server restart  

---

## 🧪 Testing

Two test files are included to verify persistence:

- `test_save_command.py` – Sets a key and saves it.
- `test_get_after_restart.py` – Retrieves the key after a simulated restart.

---

## 🚧 Coming Next

- ⏳ Implement `BGSAVE` (asynchronous save)
- ⏳ Support `APPENDONLY` persistence
- ⏳ Add support for `MSET`, `MGET`
- ⏳ Write unit tests using `unittest` or `pytest`

---

## 🚀 Run the Server

```bash
python core/server.py