import os
import sys
import json
import importlib.resources as resources

# Function inherited from the "old" CaRROT-CDM (modfied to exit on error)
    
def load_json(f_in):
    try:
        data = json.load(open(f_in))
    except Exception as err:
        print ("{0} not found. Or cannot parse as json".format(f_in))
        sys.exit()

    return data

def resolve_paths(args):
    """Resolve special path syntaxes in command line arguments."""
    try:
        with resources.path('carrottransform', '__init__.py') as f:
            package_path = str(f.parent)
    except Exception:
        # Fallback for development environment
        import carrottransform
        package_path = os.path.dirname(os.path.abspath(carrottransform.__file__))
    
    # Handle None values and replace @carrot with the actual package path
    return [arg.replace('@carrot', package_path) if arg is not None else None for arg in args]
