from __future__ import annotations

import os
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
        return "py3", "none", platform


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
