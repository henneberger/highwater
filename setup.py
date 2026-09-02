from __future__ import annotations

import os
import platform as platform_module
import sys
from pathlib import Path

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.command.build_py import build_py
from setuptools.errors import SetupError


class PlatformDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        return True


class PlatformWheel(bdist_wheel):
    def get_tag(self) -> tuple[str, str, str]:
        _, _, platform = super().get_tag()
        if sys.platform == "darwin":
            platform = f"macosx_13_0_{platform_module_machine()}"
        return "py3", "none", platform


def platform_module_machine() -> str:
    machine = platform_module.machine().lower()
    if machine in {"arm64", "aarch64"}:
        return "arm64"
    if machine in {"x86_64", "amd64"}:
        return "x86_64"
    raise SetupError(f"unsupported macOS wheel architecture: {machine}")


class EngineBuild(build_py):
    def run(self) -> None:
        filename = "highwater-server.exe" if os.name == "nt" else "highwater-server"
        binary = Path(__file__).parent / "src" / "highwater" / "bin" / filename
        if not binary.is_file():
            raise SetupError(
                "the platform wheel requires a built Highwater streaming engine at "
                f"{binary}"
            )
        super().run()


setup(
    distclass=PlatformDistribution,
    cmdclass={"bdist_wheel": PlatformWheel, "build_py": EngineBuild},
)
