import pandas as pd
cent = pd.read_csv('centrality_scores.csv')
targets = ['Bella Klein', 'Daphne Wallace', 'Lesley Groff', 'Darren Indyke']
print(cent[cent['node'].isin(targets)].set_index('node')[
    ['betweenness','eigenvector','katz','pagerank','closeness','degree']
].round(4).to_string())