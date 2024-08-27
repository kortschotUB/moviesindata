import json

def loadPalette(palettePath: str = '../assets/color_palette.json'):
    with open(palettePath) as f:
        palette = json.load(f)
    
    return palette