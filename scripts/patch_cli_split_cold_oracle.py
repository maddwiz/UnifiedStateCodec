from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")
s = APP.read_text(encoding="utf-8", errors="ignore")

# 1) Ensure oracle encoder import exists
if "encode_template_channels_v1_mask" not in s:
    anchor = "from usc.mem.hdfs_templates_v0 import HDFSTemplateBank, parse_hdfs_lines, parse_hdfs_lines_rows\n"
    if anchor not in s:
        raise SystemExit("❌ Expected import anchor not found (hdfs_templates_v0 import line).")
    s = s.replace(
        anchor,
        anchor + "from usc.api.hdfs_template_codec_v1_channels_mask import encode_template_channels_v1_mask\n",
    )

# 2) Find the cold branch
m = re.search(r"\n([ \t]*)(if|elif)\s+mode\s*==\s*['\"]cold['\"]\s*:\s*\n", s)
if not m:
    raise SystemExit("❌ Could not find: mode == 'cold' branch")

indent = m.group(1)
start = m.start()

# 3) Inside cold branch, REMOVE the oracle block we previously injected:
# It begins with our marker comment and ends at the 'return' inside that oracle path.
marker = f"{indent}    # ✅ ORACLE COLD:"
if marker not in s:
    raise SystemExit("❌ Could not find oracle marker inside cold branch (already removed?)")

# remove oracle block by deleting from marker to the first 'return' after it
pre, post = s.split(marker, 1)
# post starts at oracle comment line already removed, now find first '\n{indent}        return\n'
ret_pat = re.compile(rf"\n{re.escape(indent)}\s+return\s*\n", re.MULTILINE)
rm = ret_pat.search(post)
if not rm:
    raise SystemExit("❌ Could not find return inside oracle block to remove")
post_after = post[rm.end():]
s = pre + post_after  # cold is now native-only

# 4) Insert a NEW branch: elif mode == "cold-oracle": right BEFORE cold branch
insert_point = m.start()
oracle_block = (
    f"\n{indent}elif mode == \"cold-oracle\":\n"
    f"{indent}    if not tpl_path:\n"
    f"{indent}        raise SystemExit(\"cold-oracle requires --tpl\")\n"
    f"{indent}    bank = HDFSTemplateBank.from_csv(tpl_path)\n"
    f"{indent}    events, unknown = parse_hdfs_lines(raw_lines, bank)\n"
    f"{indent}    blob = encode_template_channels_v1_mask(events, unknown)\n"
    f"{indent}    Path(args.out).write_bytes(blob)\n"
    f"{indent}    print('BUNDLE:', f\"{{len(blob)/1024.0:.2f}} KB\", ' build=oracle')\n"
    f"{indent}    print('USCC:',   f\"{{len(blob)/1024.0:.2f}} KB\", ' saved ✅')\n"
    f"{indent}    return\n"
)

s = s[:insert_point] + oracle_block + s[insert_point:]

APP.write_text(s, encoding="utf-8")
print("✅ CLI patched: cold=native only, added cold-oracle=templated oracle")
