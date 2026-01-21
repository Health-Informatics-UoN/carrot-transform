from pathlib import Path

# need this for substition. this should be the folder iwth an "examples/" sub" folder
carrot: Path = Path(__file__).parent.parent


def convert_path(value: str) -> Path:
    # switch to posix separators
    value = value.replace("\\", "/")

    prefix: str = "@carrot/"
    if value.startswith(prefix):
        return carrot / value[len(prefix) :]
    else:
        return Path(value)
