from importlib.metadata import version

try:
    __version__ = version("carrot_transform")  # Defined in the pyproject.toml 
except Exception:
    __version__ = "unknown" 
