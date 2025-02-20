#!/bin/bash
set -euo pipefail

# Update the package list and install pipx if needed
sudo apt update
sudo apt install pipx
pipx install poetry
pipx ensurepath

poetry config repositories.pypi https://upload.pypi.org/legacy/
poetry config repositories.pypi || echo "PyPI not configured"

poetry install --no-interaction --no-root

PACKAGE_NAME=$(poetry version  | awk -F' ' '{print $1}')

poetry build
poetry publish --repository pypi --username __token__ --password "$PYPI_API_TOKEN"

sleep 10

# Test if package can be installed and imported
pip install --index-url https://pypi.org/simple/ "$PACKAGE_NAME" --no-deps
if python -c "import $(echo $PACKAGE_NAME | tr '-' '_')" 2>/dev/null; then
    echo "SUCCESS: Package '$PACKAGE_NAME' is available on PyPI!"
else
    echo "ERROR: Package '$PACKAGE_NAME' is NOT installable from PyPI!"
    exit 1
fi