from __future__ import annotations

import inspect
import pkgutil
import importlib
import usc

def main():
    hits = []
    for m in pkgutil.walk_packages(usc.__path__, usc.__name__ + "."):
        name = m.name
        try:
            mod = importlib.import_module(name)
        except Exception:
            continue

        for attr_name, obj in inspect.getmembers(mod):
            if callable(obj) and "pfq1" in attr_name.lower():
                try:
                    sig = str(inspect.signature(obj))
                except Exception:
                    sig = "(sig unavailable)"
                hits.append((name, attr_name, sig))

    hits.sort(key=lambda x: (x[0], x[1]))

    print(f"FOUND PFQ1 callables: {len(hits)}\n")
    for modname, fname, sig in hits[:200]:
        print(f"{modname} :: {fname}{sig}")

if __name__ == "__main__":
    main()
