# 🧠 Redis-like Python Server

A lightweight Redis clone built in Python, supporting basic commands, RDB persistence, concurrency, replication simulation, and more.

---

## ✅ Completed Features

- 🛠 **Basic Server Setup**
  - ✅ Bind to a configurable port
  - ✅ Multi-client support via `threading`
  - ✅ RESP protocol parsing
  
- 🔁 **Core Redis Commands**
  - ✅ `PING`, `ECHO`
  - ✅ `SET`, `GET`, `DEL`, `EXISTS`
  - ✅ Expiry commands: `EX`, `EXPIRE`, `TTL`, `PERSIST`
  - ✅ Atomic increment: `INCR`

- 💾 **Persistence (RDB-style)**
  - ✅ `SAVE` command to persist to `dump.rdb`
  - ✅ Auto-load from `dump.rdb` on server restart

- 🌐 **Replication Testing**
  - ✅ `REPLCONF` command handling
  - ✅ `PSYNC ? -1` to simulate sync start
  - ✅ Parse and respond to `FULLRESYNC`
  - ✅ Receive RDB snapshot and store as `replica_dump.rdb`
  - ✅ Parse and print commands sent by master in real-time

- 🧪 **Testing Scripts**
  - ✅ `test_save_command.py`: Save key to disk
  - ✅ `test_get_after_restart.py`: Validate key loading after restart
  - ✅ `replica_test.py`: Simulate replica handshake and listen for master commands

---

## 🧪 Testing Instructions

Run these scripts after starting the server:

```bash
# 1. Save a key
python test_save_command.py

# 2. Restart server, then check if key persists
python test_get_after_restart.py

# 3. Run replica simulation
python replica_test.py


---

## 🚧 Coming Next

⏳ BGSAVE command for async persistence

⏳ Append-only file (AOF) persistence

⏳ MSET, MGET for bulk operations

⏳ Authentication support (AUTH)

⏳ Pub/Sub system (PUBLISH, SUBSCRIBE)

⏳ Performance improvements using non-blocking I/O (e.g., asyncio)

⏳ Unit testing via pytest or unittest

⏳ Add logging & monitoring

⏳ Add CLI client

---

## 🚀 Run the Server

```bash
python core/server.py