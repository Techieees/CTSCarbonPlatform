"""Verify every lottie_icon('...json') under templates/ references an existing file."""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lottie_dir = os.path.join(ROOT, "static", "lottie")
templates = os.path.join(ROOT, "templates")

pat = re.compile(r"lottie_icon\(\s*'([^']+\.json)'")
seen = set()
refs = []
for dirpath, _, files in os.walk(templates):
    for fn in files:
        if not fn.endswith(".html"):
            continue
        path = os.path.join(dirpath, fn)
        text = open(path, encoding="utf-8").read()
        for m in pat.finditer(text):
            name = m.group(1)
            seen.add(name)
            refs.append((name, os.path.relpath(path, ROOT).replace("\\", "/")))

missing = sorted(f for f in seen if not os.path.isfile(os.path.join(lottie_dir, f)))
print("unique_json", len(seen), "refs", len(refs))
if missing:
    print("MISSING:")
    for m in missing:
        users = sorted({p for n, p in refs if n == m})
        print(" ", m, "<=", users[:5], ("..." if len(users) > 5 else ""))
    raise SystemExit(1)
print("all_ok")
