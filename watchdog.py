"""Watchdog: avvia il backend subito e lo rilancia se muore."""
import subprocess
import time
import os
import sys
import urllib.request
import threading

BACKEND_URL = "http://127.0.0.1:8001/api/risk-settings"
BACKEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend")
CHECK_INTERVAL = 15


def is_alive():
    try:
        r = urllib.request.urlopen(BACKEND_URL, timeout=5)
        return r.status == 200
    except Exception:
        return False


def run_backend():
    """Avvia run.py e ritorna il processo."""
    print(f"[Watchdog] {time.strftime('%H:%M:%S')} Avvio backend...", flush=True)
    proc = subprocess.Popen(
        [sys.executable, "run.py"],
        cwd=BACKEND_DIR,
    )
    return proc


def main():
    print(f"[Watchdog] Avviato", flush=True)

    while True:
        # Avvia il backend (in primo piano — output visibile)
        proc = run_backend()

        # Aspetta che il backend parta (max 30s)
        for _ in range(6):
            time.sleep(5)
            if is_alive():
                print(f"[Watchdog] {time.strftime('%H:%M:%S')} Backend attivo!", flush=True)
                break

        # Monitora: se il processo muore o non risponde, rilancia
        while True:
            retcode = proc.poll()
            if retcode is not None:
                print(f"[Watchdog] {time.strftime('%H:%M:%S')} Backend terminato (exit={retcode}) — rilancio in 3s", flush=True)
                time.sleep(3)
                break

            if not is_alive():
                time.sleep(CHECK_INTERVAL)
                if not is_alive():
                    print(f"[Watchdog] {time.strftime('%H:%M:%S')} Backend non risponde — rilancio", flush=True)
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    time.sleep(3)
                    break

            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
