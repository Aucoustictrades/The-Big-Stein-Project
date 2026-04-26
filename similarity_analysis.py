"""
similarity_analysis.py
======================
Pairwise node similarity measures on the Epstein email network.

Measures
--------
  1. Cosine Similarity      — centrality score feature vectors (role similarity)
  2. Jaccard Coefficient    — combined (in ∪ out) neighborhood sets
  3. Hamming Similarity     — 1 − normalised Hamming distance on binary adjacency
  4. Katz Similarity        — full pairwise matrix  K = (I − αA)^{−1} − I, symmetrised

Outputs
-------
  similarity_heatmaps.png          — 2×2 grid, one panel per measure
  similarity_cross_correlation.png — 4×4 Pearson-r heatmap across measures
  similarity_matrices.csv          — all four upper-triangle vectors in one file

Run from C:\\Users\\conor\\vol_project\\JEproj  (needs edge_list.csv, centrality_scores.csv)
"""

import numpy as np
import pandas as pd
import networkx as nx
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
from numpy.linalg import inv

# ── 0. Seaborn aesthetics ─────────────────────────────────────────────────────
sns.set_theme(style='white', font_scale=0.85)

# ─────────────────────────────────────────────────────────────────────────────
# 1. Load data
# ─────────────────────────────────────────────────────────────────────────────
print("Loading data...")
edges_df     = pd.read_csv('edge_list.csv')
centrality_df = pd.read_csv('centrality_scores.csv')

G = nx.DiGraph()
for _, row in edges_df.iterrows():
    G.add_edge(row['source'], row['target'], weight=row['weight'])

nodes    = sorted(G.nodes())
n        = len(nodes)
node_idx = {node: i for i, node in enumerate(nodes)}
print(f"  Nodes: {n}  |  Edges: {G.number_of_edges()}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Cosine Similarity — centrality feature vectors
# ─────────────────────────────────────────────────────────────────────────────
print("\n[1/4] Cosine Similarity (centrality vectors)...")

feature_cols = [
    'betweenness', 'eigenvector', 'katz', 'pagerank',
    'closeness', 'degree', 'clustering',
    'in_degree_weighted', 'out_degree_weighted'
]

cent = centrality_df.set_index('node').loc[nodes][feature_cols].values.astype(float)

# Min-max scale each feature to [0,1] so high-magnitude cols (weighted degree)
# don't dominate the angle calculation
col_min = cent.min(axis=0)
col_max = cent.max(axis=0)
col_range = np.where(col_max - col_min == 0, 1, col_max - col_min)
cent_scaled = (cent - col_min) / col_range   # shape: (n, 9)

# Cosine sim = (A @ A.T) after L2-normalising each row
norms = np.linalg.norm(cent_scaled, axis=1, keepdims=True)
norms = np.where(norms == 0, 1, norms)
cent_normed = cent_scaled / norms
cosine_sim = cent_normed @ cent_normed.T
cosine_sim = np.clip(cosine_sim, 0, 1)        # numerical safety; all values ≥ 0
np.fill_diagonal(cosine_sim, 1.0)
print(f"  Done. Range: [{cosine_sim.min():.4f}, {cosine_sim.max():.4f}]")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Jaccard Coefficient — combined (in ∪ out) neighbourhood
# ─────────────────────────────────────────────────────────────────────────────
print("\n[2/4] Jaccard Coefficient...")

def combined_neighbors(G, node):
    return (set(G.successors(node)) | set(G.predecessors(node))) - {node}

jaccard_sim = np.zeros((n, n))
neighbor_cache = {node: combined_neighbors(G, node) for node in nodes}

for i, u in enumerate(nodes):
    Nu = neighbor_cache[u]
    for j, v in enumerate(nodes):
        if i == j:
            jaccard_sim[i, j] = 1.0
            continue
        Nv    = neighbor_cache[v]
        union = Nu | Nv
        jaccard_sim[i, j] = len(Nu & Nv) / len(union) if union else 0.0

print(f"  Done. Range: [{jaccard_sim.min():.4f}, {jaccard_sim.max():.4f}]")

# ─────────────────────────────────────────────────────────────────────────────
# 4. Hamming Similarity — binary adjacency (combined), 1 − normalised distance
# ─────────────────────────────────────────────────────────────────────────────
print("\n[3/4] Hamming Similarity...")

adj_binary = np.zeros((n, n), dtype=np.int8)
for i, u in enumerate(nodes):
    for v in combined_neighbors(G, u):
        adj_binary[i, node_idx[v]] = 1

# Vectorised: for each pair count positions that differ, divide by n
diff_count = np.zeros((n, n), dtype=float)
for i in range(n):
    diff_count[i] = (adj_binary[i] != adj_binary).sum(axis=1)

hamming_sim = 1.0 - diff_count / n
np.fill_diagonal(hamming_sim, 1.0)
print(f"  Done. Range: [{hamming_sim.min():.4f}, {hamming_sim.max():.4f}]")

# ─────────────────────────────────────────────────────────────────────────────
# 5. Katz Similarity — K = (I − αA)^{−1} − I, symmetrised, min-max normalised
# ─────────────────────────────────────────────────────────────────────────────
print("\n[4/4] Katz Similarity (full pairwise matrix)...")

A        = nx.to_numpy_array(G, nodelist=nodes)
eigs     = np.linalg.eigvals(A)
lambda1  = max(abs(eigs)).real
alpha    = 0.85 / lambda1               # conservative: 15 % below 1/λ_max
print(f"  λ_max = {lambda1:.4f}   |   α = {alpha:.6f}")

I_mat    = np.eye(n)
K        = inv(I_mat - alpha * A) - I_mat  # asymmetric for directed graph
K_sym    = (K + K.T) / 2                  # symmetrise

# Normalise to [0, 1] per entry (preserve relative magnitude)
K_min, K_max = K_sym.min(), K_sym.max()
katz_sim     = (K_sym - K_min) / (K_max - K_min)
np.fill_diagonal(katz_sim, 1.0)
print(f"  Done. Pre-norm range: [{K_sym.min():.4f}, {K_sym.max():.4f}]")

# ─────────────────────────────────────────────────────────────────────────────
# 6. Plot — individual heatmaps (2×2 grid)
# ─────────────────────────────────────────────────────────────────────────────
print("\nPlotting individual heatmaps...")

# Axis labels: truncate to 14 chars to fit
tick_labels = [name[:14] for name in nodes]

measures = [
    (cosine_sim,  'Cosine Similarity\n(Centrality Feature Vectors)',    'Blues'),
    (jaccard_sim, 'Jaccard Coefficient\n(Combined In ∪ Out Neighbourhood)', 'Greens'),
    (hamming_sim, 'Hamming Similarity\n(1 − Normalised Distance, Combined Adj.)', 'Oranges'),
    (katz_sim,    'Katz Similarity\n(Full Pairwise Matrix, Normalised)', 'Purples'),
]

fig, axes = plt.subplots(2, 2, figsize=(26, 22))
fig.suptitle(
    'Pairwise Node Similarity — Epstein Email Network\n'
    '(diagonal = self-similarity = 1.0)',
    fontsize=15, fontweight='bold', y=0.995
)

for ax, (matrix, title, cmap) in zip(axes.flat, measures):
    sns.heatmap(
        matrix,
        ax=ax,
        cmap=cmap,
        xticklabels=tick_labels,
        yticklabels=tick_labels,
        vmin=0, vmax=1,
        linewidths=0,
        cbar_kws={'shrink': 0.75, 'label': 'Similarity'}
    )
    ax.set_title(title, fontsize=12, fontweight='bold', pad=8)
    ax.tick_params(axis='x', rotation=90, labelsize=5.5)
    ax.tick_params(axis='y', rotation=0,  labelsize=5.5)

plt.tight_layout(rect=[0, 0, 1, 0.98])
plt.savefig('similarity_heatmaps.png', dpi=160, bbox_inches='tight')
plt.close()
print("  Saved: similarity_heatmaps.png")

# ─────────────────────────────────────────────────────────────────────────────
# 7. Cross-measure correlation heatmap
# ─────────────────────────────────────────────────────────────────────────────
print("\nComputing cross-measure correlations...")

def upper_tri(M):
    """Flatten upper triangle (k=1 → exclude diagonal) to a 1-D vector."""
    idx = np.triu_indices(n, k=1)
    return M[idx]

measure_names = ['Cosine', 'Jaccard', 'Hamming', 'Katz']
tri_vectors   = [upper_tri(m) for m, _, _ in measures]

# Pearson r matrix
corr = np.zeros((4, 4))
for i in range(4):
    for j in range(4):
        corr[i, j] = np.corrcoef(tri_vectors[i], tri_vectors[j])[0, 1]

# Mask diagonal so it doesn't visually dominate
mask_diag = np.eye(4, dtype=bool)

fig2, ax2 = plt.subplots(figsize=(7, 6))
sns.heatmap(
    corr,
    ax=ax2,
    annot=True,
    fmt='.3f',
    cmap='RdYlGn',
    center=0,
    vmin=-1, vmax=1,
    xticklabels=measure_names,
    yticklabels=measure_names,
    annot_kws={'size': 13, 'weight': 'bold'},
    linewidths=0.5,
    linecolor='#cccccc',
    cbar_kws={'shrink': 0.8, 'label': 'Pearson r'}
)
ax2.set_title(
    'Cross-Measure Similarity Correlation\n'
    'Pearson r on upper-triangle pairwise scores (n=53 nodes → 1,378 pairs)',
    fontsize=11, fontweight='bold', pad=10
)
plt.tight_layout()
plt.savefig('similarity_cross_correlation.png', dpi=160, bbox_inches='tight')
plt.close()
print("  Saved: similarity_cross_correlation.png")

# Print correlation table to console
corr_df = pd.DataFrame(corr, index=measure_names, columns=measure_names)
print("\n  Pearson r (upper-triangle pairwise scores):")
print(corr_df.round(4).to_string())

# ─────────────────────────────────────────────────────────────────────────────
# 8. Save raw similarity vectors to CSV
# ─────────────────────────────────────────────────────────────────────────────
print("\nSaving similarity matrices to CSV...")

# Build all unique pairs
idx_i, idx_j = np.triu_indices(n, k=1)
pairs_df = pd.DataFrame({
    'node_i':  [nodes[i] for i in idx_i],
    'node_j':  [nodes[j] for j in idx_j],
    'cosine':  tri_vectors[0].round(6),
    'jaccard': tri_vectors[1].round(6),
    'hamming': tri_vectors[2].round(6),
    'katz':    tri_vectors[3].round(6),
})
pairs_df.to_csv('similarity_matrices.csv', index=False)
print(f"  Saved: similarity_matrices.csv  ({len(pairs_df):,} pairs)")

print("\n✓ All done.")