import json

def loadPalette(palettePath: str = '../assets/color_palette.json'):
    with open(palettePath) as f:
        palette = json.load(f)
    
    return palette

def loadTableStyles():
    return [
        {"selector": "thead th", "props": [("background-color", loadPalette()["canvas_dark"]), 
                                            ("color", "#333"), 
                                            ("font-weight", "bold"), 
                                            ("font-size", "16px"), 
                                            ("font-family", "monospace")]},
        {"selector": "tbody tr:nth-child(even)", "props": [("background-color", loadPalette()["canvas"]), 
                                                            ("font-family", "Lora"), 
                                                            ("font-size", "14px"), 
                                                            ("color", "black")]},
        {"selector": "tbody tr:nth-child(odd)", "props": [("background-color", loadPalette()["canvas"]), 
                                                        ("font-family", "Lora"), 
                                                        ("font-size", "14px"), 
                                                        ("color", "black")]}
    ]