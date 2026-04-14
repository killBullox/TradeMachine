"""Launcher per il backend con diagnostica."""
import asyncio
import signal
import sys
import time
import os
import uvicorn


class MonitoredServer(uvicorn.Server):
    """Server uvicorn con logging su perché esce."""

    async def serve(self, sockets=None):
        print(f"[Runner] PID={os.getpid()} serve() iniziato", flush=True)
        await super().serve(sockets)
        print(f"[Runner] serve() terminato — should_exit={self.should_exit} started={self.started}", flush=True)

    def handle_exit(self, sig, frame):
        print(f"[Runner] !!! Ricevuto segnale {sig} ({signal.Signals(sig).name}) — avvio shutdown", flush=True)
        super().handle_exit(sig, frame)


def main():
    while True:
        print(f"[Runner] Avvio server (PID={os.getpid()})...", flush=True)
        try:
            config = uvicorn.Config("main:app", host="127.0.0.1", port=8001, timeout_keep_alive=120)
            server = MonitoredServer(config)
            asyncio.run(server.serve())
        except KeyboardInterrupt:
            print("[Runner] CTRL+C — arresto.", flush=True)
            break
        except Exception as e:
            print(f"[Runner] Eccezione: {type(e).__name__}: {e}", flush=True)
        print("[Runner] Riavvio in 5 sec...", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
