import pandas as pd
import numpy as np
from scipy import stats

cent = pd.read_csv('centrality_scores.csv')
sim  = pd.read_csv('similarity_matrices.csv')

# Rebuild the degree vs katz_vs_epstein table
epstein_katz = sim[
    (sim['node_i'] == 'Jeffrey Epstein') | (sim['node_j'] == 'Jeffrey Epstein')
].copy()
epstein_katz['node'] = np.where(
    epstein_katz['node_i'] == 'Jeffrey Epstein',
    epstein_katz['node_j'], epstein_katz['node_i']
)
merged = cent.merge(epstein_katz[['node','katz_bin']], on='node')

# OLS residuals: katz_bin ~ degree
slope, intercept, _, _, _ = stats.linregress(merged['degree'], merged['katz_bin'])
merged['katz_predicted'] = intercept + slope * merged['degree']
merged['residual'] = merged['katz_bin'] - merged['katz_predicted']

print("Nodes with HIGHER Katz-Epstein than degree predicts (positive residual):")
print(merged[['node','degree','betweenness','katz_bin','residual']]
      .sort_values('residual', ascending=False).head(10).to_string(index=False))

print("\nNodes with LOWER Katz-Epstein than degree predicts (negative residual):")
print(merged[['node','degree','betweenness','katz_bin','residual']]
      .sort_values('residual').head(10).to_string(index=False))