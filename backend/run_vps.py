"""Launcher con auto-restart e logging su file."""
import asyncio
import signal
import sys
import time
import os
import uvicorn

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend.log')


class Logger:
    def __init__(self, filepath):
        self.terminal = sys.__stdout__
        self.log = open(filepath, 'a', encoding='utf-8', errors='replace')

    def write(self, msg):
        try:
            self.terminal.write(msg)
        except Exception:
            pass
        self.log.write(msg)
        self.log.flush()

    def flush(self):
        self.log.flush()


sys.stdout = Logger(LOG_FILE)
sys.stderr = Logger(LOG_FILE)

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Scrivi PID su file per restart sicuro (senza uccidere altri processi Python)
PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'trademachine.pid')
with open(PID_FILE, 'w') as f:
    f.write(str(os.getpid()))


class MonitoredServer(uvicorn.Server):
    async def serve(self, sockets=None):
        print(f"[Runner] PID={os.getpid()} serve() iniziato", flush=True)
        await super().serve(sockets)
        print(f"[Runner] serve() terminato", flush=True)

    def handle_exit(self, sig, frame):
        print(f"[Runner] Ricevuto segnale {sig} — shutdown", flush=True)
        super().handle_exit(sig, frame)


def main():
    while True:
        print(f"[Runner] Avvio server (PID={os.getpid()})...", flush=True)
        try:
            config = uvicorn.Config("main:app", host="0.0.0.0", port=8002, timeout_keep_alive=120, log_config=None)
            server = MonitoredServer(config)
            asyncio.run(server.serve())
        except KeyboardInterrupt:
            print("[Runner] CTRL+C", flush=True)
            break
        except Exception as e:
            print(f"[Runner] Eccezione: {type(e).__name__}: {e}", flush=True)
        print("[Runner] Riavvio in 5 sec...", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
