#!/bin/bash
set -euo pipefail

# Ensure GitHub CLI is installed
if ! command -v gh &>/dev/null; then
    echo "GitHub CLI ('gh') is not installed. Please install it and try again."
    exit 1
fi

# Ensure authentication with GitHub CLI
if ! gh auth status; then
  echo "GitHub CLI is not authenticated. Please log in using 'gh auth login'."
  exit 1
fi

# Check version update type is provided
if [[ -z "$1" ]]; then
    echo "Error: Please provide version bump type (patch, minor, major)"
    exit 1
elif [[ "$1" != "major" && "$1" != "minor" && "$1" != "patch" ]]; then
    echo "Error: Invalid version bump type. Use: patch, minor, or major."
    exit 1
fi

VERSION=$(poetry version "$1" --short --dry-run)
TAG="v$VERSION"

read -p "Are you sure you want to release version $VERSION? (y/n): " confirmation

if [[ "$confirmation" != "y" ]]; then
    echo "Release cancelled."
    exit 0
fi

poetry version "$1"

git add pyproject.toml
git commit -m "Bump version to $VERSION"
git push origin main

git tag -a "$TAG" -m "Release $VERSION"
git push origin "$TAG"

gh release create "$TAG" --title "$TAG" --notes "Automated release for $VERSION"

echo "Release $VERSION created and pushed successfully!"
