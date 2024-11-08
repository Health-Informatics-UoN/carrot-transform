# Provides an entry point for the built executable
# Build with "pyinstaller --onefile carrot_transform.py"
from carrottransform.cli.command import transform
if __name__ == '__main__':
  transform()
