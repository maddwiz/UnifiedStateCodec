from __future__ import annotations

import inspect
import pkgutil
import importlib
import usc

def find_pfq1():
    hits = []
    for m in pkgutil.walk_packages(usc.__path__, usc.__name__ + "."):
        name = m.name
        try:
            mod = importlib.import_module(name)
        except Exception:
            continue
        for attr in ("PFQ1", "PF1", "Bloom", "PacketFilter", "PFQ", "PF"):
            if hasattr(mod, attr):
                obj = getattr(mod, attr)
                if isinstance(obj, type) and attr == "PFQ1":
                    hits.append((name, obj))
    return hits

hits = find_pfq1()
print(f"FOUND PFQ1 classes: {len(hits)}\n")

for modname, cls in hits[:10]:
    print("MODULE:", modname)
    print("CLASS :", cls)
    try:
        print("INIT  :", inspect.signature(cls.__init__))
    except Exception as e:
        print("INIT  : (no sig)", e)

    methods = [m for m in dir(cls) if not m.startswith("_")]
    print("METHODS:", methods[:60])
    print()
