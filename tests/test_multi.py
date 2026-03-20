import click
from click import Context

class RGBColorParamType(click.ParamType):
    """Custom Click parameter type that accepts color names or individual RGB components."""
    
    # Basic color mapping
    COLOR_MAP = {
        'red': (255, 0, 0),
        'green': (0, 255, 0),
        'blue': (0, 0, 255),
        'yellow': (255, 255, 0),
        'cyan': (0, 255, 255),
        'magenta': (255, 0, 255),
        'black': (0, 0, 0),
        'white': (255, 255, 255),
        'gray': (128, 128, 128),
        'orange': (255, 165, 0),
        'purple': (128, 0, 128),
        'pink': (255, 192, 203),
    }
    
    name = "color"
    
    def convert(self, value, param, ctx):
        """Convert color name to RGB tuple."""
        # If it's already a tuple, return it
        if isinstance(value, tuple):
            return value
        
        # Handle color name
        color_lower = value.lower()
        if color_lower in self.COLOR_MAP:
            return self.COLOR_MAP[color_lower]
        
        self.fail(f"'{value}' is not a valid color name. Available colors: {', '.join(self.COLOR_MAP.keys())}", param, ctx)
    
    def shell_complete(self, ctx, param, incomplete):
        """Shell completion for color names."""
        return [click.shell_completion.CompletionItem(name) 
                for name in self.COLOR_MAP.keys() 
                if name.startswith(incomplete.lower())]


def make_rgb_callback(base_name):
    """Create a callback function that collects RGB components."""
    def rgb_callback(ctx: Context, param: click.Parameter, value: int) -> None:
        # Skip if value is None
        if value is None:
            return None
            
        # Initialize storage for this parameter if not exists
        if not hasattr(ctx, '_rgb_collector'):
            ctx._rgb_collector = {}
        
        if base_name not in ctx._rgb_collector:
            ctx._rgb_collector[base_name] = {'r': None, 'g': None, 'b': None}
        
        # Store the component value
        component = param.name[-1]  # 'r', 'g', or 'b'
        ctx._rgb_collector[base_name][component] = value
        
        # Check if we have all three components
        collector = ctx._rgb_collector[base_name]
        if all(v is not None for v in collector.values()):
            # Set the final value in the context
            rgb_tuple = (collector['r'], collector['g'], collector['b'])
            
            # Store the final tuple in the context
            if not hasattr(ctx, '_rgb_results'):
                ctx._rgb_results = {}
            ctx._rgb_results[base_name] = rgb_tuple
            
            # Clear the stored values
            del ctx._rgb_collector[base_name]
        
        return None
    
    return rgb_callback


@click.command()
@click.option('--hair', type=RGBColorParamType(), help='Hair color (color name)')
@click.option('--shoe', type=RGBColorParamType(), help='Shoe color (color name)')
@click.option('--hair_r', type=int, callback=make_rgb_callback('hair'), expose_value=False, hidden=True)
@click.option('--hair_g', type=int, callback=make_rgb_callback('hair'), expose_value=False, hidden=True)
@click.option('--hair_b', type=int, callback=make_rgb_callback('hair'), expose_value=False, hidden=True)
@click.option('--shoe_r', type=int, callback=make_rgb_callback('shoe'), expose_value=False, hidden=True)
@click.option('--shoe_g', type=int, callback=make_rgb_callback('shoe'), expose_value=False, hidden=True)
@click.option('--shoe_b', type=int, callback=make_rgb_callback('shoe'), expose_value=False, hidden=True)
def main(hair, shoe):
    """Process hair and shoe colors."""
    # Get any RGB component results from the context
    ctx = click.get_current_context()
    
    # If hair wasn't provided via --hair, check if we collected RGB components
    if hair is None and hasattr(ctx, '_rgb_results') and 'hair' in ctx._rgb_results:
        hair = ctx._rgb_results['hair']
    
    # If shoe wasn't provided via --shoe, check if we collected RGB components
    if shoe is None and hasattr(ctx, '_rgb_results') and 'shoe' in ctx._rgb_results:
        shoe = ctx._rgb_results['shoe']
    
    if hair is not None:
        print(f"Hair: RGB{hair}")
    
    if shoe is not None:
        print(f"Shoe: RGB{shoe}")
    
    if hair is None and shoe is None:
        print("No color arguments provided.")

if __name__ == '__main__':
    main()