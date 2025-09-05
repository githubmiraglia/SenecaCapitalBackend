# windowsScheduler.py
import os
import sys
import subprocess

ROOT = os.path.dirname(os.path.abspath(__file__))
PY_EXE = sys.executable

# Absolute paths to the two fetch scripts
SCRIPTS = {
    "Uqbar CRI": os.path.join(ROOT, "playwright_scripts", "fetch_uqbar_cri_to_django.py"),
    "Anbima Indices": os.path.join(ROOT, "playwright_scripts", "fetch_indices.py"),
}

def make_bat(script_path: str, log_file: str) -> str:
    """
    Create a .bat file to run the script with env vars.
    Returns the .bat path.
    """
    bat_path = script_path.replace(".py", ".bat")
    bat = f"""@echo off
setlocal
set DJANGO_BASE={os.getenv("DJANGO_BASE", "http://127.0.0.1:8000")}
set DJANGO_USER={os.getenv("DJANGO_USER", "admin")}
set DJANGO_PASS={os.getenv("DJANGO_PASS", "admin123")}
set UQBAR_EMAIL={os.getenv("UQBAR_EMAIL", "tommymv30@gmail.com")}
set UQBAR_PASSWORD={os.getenv("UQBAR_PASSWORD", "Uqbar281173!!")}
cd /d "{ROOT}"
mkdir logs 2>NUL
"{PY_EXE}" "{script_path}" --headless >> "{log_file}" 2>&1
endlocal
"""
    with open(bat_path, "w", encoding="ascii", errors="ignore") as f:
        f.write(bat)
    return bat_path

def install_schedule(script_name: str, time: str = "22:00"):
    """
    Create/update a Windows Scheduled Task for the given script.
    """
    script_path = SCRIPTS[script_name]
    logs_dir = os.path.join(ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"{script_name.replace(' ', '_').lower()}.log")

    bat_path = make_bat(script_path, log_file)

    task_name = f"{script_name} Scheduler"
    cmd = [
        "SCHTASKS",
        "/Create",
        "/SC", "DAILY",
        "/ST", time,
        "/TN", task_name,
        "/TR", bat_path,
        "/RU", "SYSTEM",
        "/RL", "HIGHEST",
        "/F",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    if res.returncode != 0:
        print("Scheduler create error:", res.stdout or res.stderr)
    else:
        print(f"Scheduled task '{task_name}' set for daily {time}.")
        print(f"Batch: {bat_path}")
        print(f"Log:   {log_file}")

def remove_schedule(script_name: str):
    """
    Remove the scheduled task for the given script.
    """
    task_name = f"{script_name} Scheduler"
    cmd = ["SCHTASKS", "/Delete", "/TN", task_name, "/F"]
    res = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    if res.returncode != 0:
        print("Scheduler delete error:", res.stdout or res.stderr)
    else:
        print(f"Scheduled task '{task_name}' removed.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Manage Windows Scheduled Tasks for data fetchers.")
    parser.add_argument("--install", choices=SCRIPTS.keys(), help="Install schedule for script")
    parser.add_argument("--remove", choices=SCRIPTS.keys(), help="Remove schedule for script")
    parser.add_argument("--time", type=str, default="22:00", help="Daily schedule time (HH:MM)")

    args = parser.parse_args()

    if args.install:
        install_schedule(args.install, args.time)
    elif args.remove:
        remove_schedule(args.remove)
    else:
        print("Use --install or --remove with one of:", ", ".join(SCRIPTS.keys()))
