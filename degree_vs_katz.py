import pandas as pd
import numpy as np

cent = pd.read_csv('centrality_scores.csv')
sim  = pd.read_csv('similarity_matrices.csv')

# Get each node's Katz(bin) similarity to Epstein
epstein_katz = sim[
    (sim['node_i'] == 'Jeffrey Epstein') | (sim['node_j'] == 'Jeffrey Epstein')
].copy()
epstein_katz['other'] = np.where(
    epstein_katz['node_i'] == 'Jeffrey Epstein',
    epstein_katz['node_j'],
    epstein_katz['node_i']
)
epstein_katz = epstein_katz[['other', 'katz_bin']].rename(
    columns={'other': 'node', 'katz_bin': 'katz_vs_epstein'}
)

merged = cent.merge(epstein_katz, on='node')
r = merged['degree'].corr(merged['katz_vs_epstein'])
print(f"Pearson r (degree vs katz_bin_with_Epstein): {r:.4f}")
print()
print(merged[['node','degree','betweenness','katz_vs_epstein']]
      .sort_values('katz_vs_epstein', ascending=False)
      .head(15).to_string(index=False))