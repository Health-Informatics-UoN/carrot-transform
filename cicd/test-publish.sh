#!/bin/bash
set -euo pipefail

sudo apt update
sudo apt install pipx
pipx install poetry
pipx ensurepath
poetry config repositories.testpypi https://test.pypi.org/legacy/
poetry config repositories.testpypi || echo "testpypi not configured"

poetry install --no-interaction --no-root
PACKAGE_NAME=$(poetry version  | awk -F' ' '{print $1}')
poetry build
poetry publish --repository testpypi --username __token__ --password "$TEST_PYPI_API_TOKEN"

# Wait a few seconds to ensure that the package is available
sleep 10

pip install --index-url https://test.pypi.org/simple/ "$PACKAGE_NAME" --no-deps

if python -c "import $(echo $PACKAGE_NAME | tr '-' '_')" 2>/dev/null; then
    echo "SUCCESS: Package '$PACKAGE_NAME' is available on TestPyPI!"
else
    echo "ERROR: Package '$PACKAGE_NAME' is NOT installable from TestPyPI!"
    exit 1
fi
