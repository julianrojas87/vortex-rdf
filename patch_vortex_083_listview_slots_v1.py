#!/usr/bin/env python3
from pathlib import Path
import hashlib, os, shutil, sys, tempfile

FILES = {
    "core/src/common/indexes.rs": {
        "sha256": "76bafe8f1aafe117448005b9cc8b3267774291db836b31dd9fa66041667213ce",
        "old": 'use vortex_array::arrays::listview::ListViewArrayExt;\n',
        "new": 'use vortex_array::arrays::listview::{ListViewArrayExt, ListViewArraySlotsExt};\n',
    },
    "core/src/common/utils.rs": {
        "sha256": "d9225963b8a43323b7d4abcd53f24503d19786a109525d0da31cdf0b33d14e39",
        "old": 'use vortex_array::arrays::listview::{ListViewArray, ListViewArrayExt};\n',
        "new": 'use vortex_array::arrays::listview::{\n    ListViewArray, ListViewArrayExt, ListViewArraySlotsExt,\n};\n',
    },
}
BACKUP_SUFFIX = ".vortex-083-listview-slots-v1.bak"

def fail(message):
    raise SystemExit(f"ERROR: {message}")

def main():
    root = Path(sys.argv[1] if len(sys.argv) == 2 else ".").resolve()
    prepared = {}
    for relative, spec in FILES.items():
        target = root / relative
        if not target.is_file():
            fail(f"target does not exist: {target}")
        raw = target.read_bytes()
        text = raw.decode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        if digest != spec["sha256"]:
            if "ListViewArraySlotsExt" in text:
                fail(f"patch appears already applied or target is not expected revision: {relative}")
            fail(f"unexpected revision for {relative}: sha256={digest}, expected={spec['sha256']}")
        count = text.count(spec["old"])
        if count != 1:
            fail(f"unique-anchor check failed for {relative}: count={count}")
        updated = text.replace(spec["old"], spec["new"], 1)
        if updated.count("ListViewArraySlotsExt") != 1:
            fail(f"postcondition failed for {relative}")
        backup = target.with_name(target.name + BACKUP_SUFFIX)
        if backup.exists():
            fail(f"backup already exists: {backup}")
        prepared[target] = (raw, updated, backup)

    temporaries = {}
    committed = []
    try:
        for target, (_, updated, _) in prepared.items():
            fd, temporary = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=target.parent)
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(updated)
                handle.flush()
                os.fsync(handle.fileno())
            temporaries[target] = Path(temporary)
        for target, (raw, _, backup) in prepared.items():
            backup.write_bytes(raw)
        for target, temporary in temporaries.items():
            os.replace(temporary, target)
            committed.append(target)
    except BaseException:
        for target in committed:
            raw = prepared[target][0]
            target.write_bytes(raw)
        for temporary in temporaries.values():
            try: temporary.unlink()
            except FileNotFoundError: pass
        for _, (_, _, backup) in prepared.items():
            try: backup.unlink()
            except FileNotFoundError: pass
        raise

    for target, (_, _, backup) in prepared.items():
        print(f"patched {target}")
        print(f"backup  {backup}")

if __name__ == "__main__":
    main()
