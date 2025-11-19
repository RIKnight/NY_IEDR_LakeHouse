# src/lakehouse/__init__.py
import importlib.metadata
import pathlib
import tomllib

source_location = pathlib.Path(__file__).parent
if (source_location.parent.parent / "pyproject.toml").exists():
    with open(source_location.parent.parent / "pyproject.toml", "rb") as f:
        __version__ = tomllib.load(f)['project']['version']
else:
    __version__ = importlib.metadata.version("package")


__all__ = []