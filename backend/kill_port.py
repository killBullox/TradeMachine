import subprocess
import signal
import os

# Trova PIDs sulla porta 8000 via netstat
result = subprocess.run(['netstat', '-ano'], capture_output=True, text=True)
pids = set()
for line in result.stdout.splitlines():
    if ':8000' in line and 'LISTENING' in line:
        parts = line.strip().split()
        if parts:
            pids.add(int(parts[-1]))

if not pids:
    print("Nessun processo su porta 8000")
else:
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"SIGTERM -> PID {pid}")
        except Exception as e:
            print(f"Errore PID {pid}: {e}")
