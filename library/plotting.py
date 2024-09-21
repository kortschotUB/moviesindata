import json
from scipy import stats

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

def createBoxplotWithTTests(ax, df, column, grouper = 'classification', labels = ['High RBR', 'Low RBR']):
    group_a = df[df[grouper] == 'good'][column]
    group_b = df[df[grouper] == 'bad'][column]
    
    # Perform t-test
    t_stat, p_value = stats.ttest_ind(group_a, group_b)
    
    # Create boxplot
    ax.boxplot([group_a, group_b], labels=labels)
    ax.set_title(f'{column}')
    ax.set_xlabel('Group')
    ax.set_ylabel('Value')
    
    # Overlay t-test results
    ax.text(1.5, max(group_a.max(), group_b.max()), f't-stat: {t_stat:.2f}\np-value: {p_value:.3f}', 
            horizontalalignment='center', verticalalignment='top', fontsize=10, bbox=dict(facecolor='white', alpha=0.5))