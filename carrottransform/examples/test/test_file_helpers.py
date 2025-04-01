import pytest
from carrottransform.tools.file_helpers import resolve_paths
import os
import importlib.resources as resources
from unittest.mock import patch

@pytest.mark.unit
def test_resolve_paths_with_resources():
    """Test resolving @carrot paths using resources.path"""
    with resources.path('carrottransform', '__init__.py') as f:
        package_path = str(f.parent)
    
    test_paths = ['@carrot/config/test.json']
    expected = [os.path.join(package_path, 'config/test.json').replace('\\', '/')]
    
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
        package_path = os.path.dirname(os.path.abspath(carrottransform.__file__))
        
        test_paths = ['@carrot/config/test.json']
        expected = [os.path.join(package_path, 'config/test.json').replace('\\', '/')]
        
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