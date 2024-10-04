import json
from scipy import stats
import pandas as pd
import holoviews as hv
from holoviews import opts, dim

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
    
def makeHVChord(graphDf: pd.DataFrame):
    meltedDf = graphDf.melt(id_vars=['value'], value_vars=['source', 'target'], var_name='type', value_name='node')
    meltedDf['index'] = meltedDf['node'].astype('category').cat.codes
    meltedDf.rename(columns={'node':'name', 'value':'group'}, inplace=True)
    meltedDf.reset_index(drop=True, inplace=True)

    nodes = meltedDf[['index','group','name']]
    nodes.drop_duplicates(subset='index', keep='first', inplace=True)

    indexMap = nodes.set_index('name')['index'].to_dict()

    links = graphDf.copy(deep=True)

    links['source'] = links['source'].map(indexMap)
    links['target'] = links['target'].map(indexMap)
    links['value'] = 10

    hv.extension('bokeh')
    hv.output(size=400)

    chord = hv.Chord((links, hv.Dataset(nodes, 'index'))).select(value=(5, None))
    chord.opts(
        opts.Chord(
                cmap='Dark2', 
                edge_cmap='Dark2', 
                edge_color=dim('source').str(), 
                edge_alpha=.1,
                labels='name', 
                bgcolor=loadPalette()['canvas'],
                node_color=dim('index').str())
    )