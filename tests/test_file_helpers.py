import pytest
from carrottransform.tools.file_helpers import resolve_paths
import os
import importlib.resources as resources
from pathlib import Path
from unittest.mock import patch

@pytest.mark.unit
def test_resolve_paths_with_resources():
    """Test resolving @carrot paths using resources.path"""
    
    package_path = (resources.files('carrottransform') / '__init__.py').parent
    
    test_paths = ['@carrot/config/test.json']
    expected = [package_path / 'config/test.json']
    
    result = resolve_paths(test_paths)
    assert result == expected

@pytest.mark.unit
def test_resolve_paths_with_fallback():
    """Test resolving @carrot paths using fallback method"""
    # Mock resources.path to raise an exception, forcing fallback
    with patch('importlib.resources.path') as mock_path:
        mock_path.side_effect = Exception("Force fallback")
        
        # The fallback uses carrottransform.__file__
        import carrottransform
        package_path = Path(os.path.dirname(os.path.abspath(carrottransform.__file__)))
        
        test_paths = ['@carrot/config/test.json']
        expected = [package_path / 'config/test.json']
        
        result = resolve_paths(test_paths)
        assert result == expected

@pytest.mark.unit
def test_resolve_paths_without_carrot():
    """Test paths without @carrot prefix remain unchanged"""
    paths = [
        '/normal/path/file.txt',
        'relative/path/file.csv',
        None
    ]
    
    result = resolve_paths(paths)
    assert result == paths

@pytest.mark.unit
def test_resolve_paths_all_none():
    """Test handling list of all None values"""
    paths = [None, None, None]
    result = resolve_paths(paths)
    assert result == paths

@pytest.mark.unit
def test_resolve_paths_empty_list():
    """Test handling empty list"""
    assert resolve_paths([]) == []

@pytest.mark.unit
def test_resolve_paths_windows():
    """Test resolving @carrot paths on Windows"""
    try:
        package_path = (resources.files('carrottransform') / '__init__.py').parent
    except Exception:
        import carrottransform
        package_path = Path(carrottransform.__file__).resolve().parent
    
    test_paths = [
        '@carrot\\config\\test.json',  # Windows backslash
        '@carrot/config\\test.json',   # Mixed slashes
        '@carrot\\config/test.json'    # Mixed slashes
    ]
    expected = [package_path / 'config/test.json']
    
    results = resolve_paths(test_paths)
<<<<<<< HEAD
    print("\nExpected:", expected[0])
    print("Results:")
    for r in results:
        print(r)
=======
>>>>>>> origin/54-carrot-doesnt-work-on-windows
    assert all(r == expected[0] for r in results) 