from __future__ import annotations

import os
import stat
import sys
from pathlib import Path


def binary_path() -> Path:
    filename = "highwater-server.exe" if os.name == "nt" else "highwater-server"
    binary = Path(__file__).with_name("bin") / filename
    if not binary.is_file():
        raise RuntimeError(
            "this Highwater installation does not contain the streaming engine; "
            "install a platform wheel from PyPI"
        )
    return binary


def main() -> None:
    binary = binary_path()
    if os.name != "nt":
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    os.execv(binary, [str(binary), *sys.argv[1:]])


if __name__ == "__main__":
    main()
