from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

START_MARK = "# HOT-LITE-FULL QUERY (DECODE+SCAN) — v0"

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    start = s.find(START_MARK)
    if start == -1:
        die(f"❌ could not find marker: {START_MARK}")

    # Find the "if getattr(args, ..." line after the marker
    if_line = s.find("if getattr(args", start)
    if if_line == -1:
        die("❌ could not find hot-lite-full if-branch after marker")

    # We will replace the entire inserted branch by finding its end.
    # Easiest safe end: find the next blank line after a 'pass' that belongs to its cleanup.
    # We'll search forward for the first occurrence of "\n\n" AFTER the marker that ends the block.
    # But to be safer, we replace from the 'if getattr(args' line up to the next line that starts with
    # 4 spaces then something that is NOT indented block content.
    # We'll just find the next occurrence of "\n    # " after the block (next comment section).
    next_section = s.find("\n    #", if_line + 1)
    if next_section == -1:
        die("❌ could not locate next section marker after hot-lite-full branch")

    # Replacement branch (correct Python, correct try/finally)
    replacement = """if getattr(args, "mode", "hot") == "hot-lite-full" or getattr(args, "input", None):
        import os
        import tempfile
        import subprocess

        bin_path = args.input or args.hot
        ql = str(args.q).lower()
        limit = int(args.limit)

        tmp_fd, tmp_path = tempfile.mkstemp(prefix="usc_q_", suffix=".log")
        os.close(tmp_fd)

        try:
            # Decode hot-lite-full -> temp log file
            subprocess.run([
                "python3", "-m", "usc.cli.app", "decode",
                "--mode", "hot-lite-full",
                "--input", str(bin_path),
                "--out", str(tmp_path),
            ], check=True)

            hits = 0
            with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if ql in line.lower():
                        print(line.rstrip("\\n"))
                        hits += 1
                        if hits >= limit:
                            break

            # grep semantics: rc=0 if hits found, rc=1 if no hits
            raise SystemExit(0 if hits > 0 else 1)

        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

"""

    # Perform replace
    s2 = s[:if_line] + replacement + s[next_section+1:]  # +1 keeps formatting stable

    APP.write_text(s2, encoding="utf-8")
    print(f"✅ repaired hot-lite-full branch in: {APP}")

if __name__ == "__main__":
    main()
