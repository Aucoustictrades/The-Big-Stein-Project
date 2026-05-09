"""
similarity_analysis.py
======================
Pairwise node similarity measures on the Epstein email network.

Measures
--------
  1. Cosine Similarity       — centrality score feature vectors (role similarity)
  2. Jaccard Coefficient     — combined (in+out) neighborhood sets
  3. Hamming Similarity      — 1 - normalised Hamming distance on binary adjacency
  4. Katz Similarity (raw)   — K = (I - aA)^{-1} - I on weighted A  [DIAGNOSTIC]
  5. Katz Similarity (bin)   — same formula on binary (unweighted) A
  6. Katz Similarity (log)   — same formula on log1p-transformed weights

Why three Katz variants?
  The weighted adjacency has lambda_max ~34K (driven by Epstein's email volume).
  This forces alpha so small (~2.5e-5) that K ~= alpha*A -- path counting beyond
  length 1 effectively vanishes.  Binary and log-weight versions restore a
  meaningful alpha, recovering the multi-hop path structure Katz is designed for.

Outputs
-------
  similarity_heatmaps.png          -- 2x3 grid, one panel per measure
  similarity_cross_correlation.png -- 6x6 Pearson-r heatmap across measures
  similarity_matrices.csv          -- all six upper-triangle vectors in one file

Run from project directory (needs edge_list.csv, centrality_scores.csv)
"""

import numpy as np
import pandas as pd
import networkx as nx
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from numpy.linalg import inv

sns.set_theme(style='white', font_scale=0.85)

# ─────────────────────────────────────────────────────────────────────────────
# 1. Load data
# ─────────────────────────────────────────────────────────────────────────────
print("Loading data...")
edges_df      = pd.read_csv('edge_list.csv')
centrality_df = pd.read_csv('centrality_scores.csv')

G = nx.DiGraph()
for _, row in edges_df.iterrows():
    G.add_edge(row['source'], row['target'], weight=row['weight'])

nodes    = sorted(G.nodes())
n        = len(nodes)
node_idx = {node: i for i, node in enumerate(nodes)}
print(f"  Nodes: {n}  |  Edges: {G.number_of_edges()}")


# ─────────────────────────────────────────────────────────────────────────────
# Helper: compute Katz similarity matrix given an adjacency matrix
# ─────────────────────────────────────────────────────────────────────────────
def katz_matrix(A_in, alpha_frac=0.85, label=''):
    eigs    = np.linalg.eigvals(A_in)
    lam1    = max(abs(eigs)).real
    alpha   = alpha_frac / lam1
    I_mat   = np.eye(A_in.shape[0])
    K       = inv(I_mat - alpha * A_in) - I_mat
    K_sym   = (K + K.T) / 2
    K_min, K_max = K_sym.min(), K_sym.max()
    K_norm  = (K_sym - K_min) / (K_max - K_min) if K_max > K_min else K_sym
    np.fill_diagonal(K_norm, 1.0)
    print(f"  {label}: lambda_max={lam1:.4f}  alpha={alpha:.2e}  "
          f"pre-norm range=[{K_sym.min():.4f}, {K_sym.max():.4f}]")
    return K_norm


# ─────────────────────────────────────────────────────────────────────────────
# 2. Cosine Similarity — centrality feature vectors
# ─────────────────────────────────────────────────────────────────────────────
print("\n[1/6] Cosine Similarity (centrality vectors)...")

feature_cols = [
    'betweenness', 'eigenvector', 'katz', 'pagerank',
    'closeness', 'degree', 'clustering',
    'in_degree_weighted', 'out_degree_weighted'
]

cent      = centrality_df.set_index('node').loc[nodes][feature_cols].values.astype(float)
col_min   = cent.min(axis=0)
col_max   = cent.max(axis=0)
col_range = np.where(col_max - col_min == 0, 1, col_max - col_min)
cent_sc   = (cent - col_min) / col_range

norms       = np.linalg.norm(cent_sc, axis=1, keepdims=True)
norms       = np.where(norms == 0, 1, norms)
cosine_sim  = np.clip((cent_sc / norms) @ (cent_sc / norms).T, 0, 1)
np.fill_diagonal(cosine_sim, 1.0)
print(f"  Done. Range: [{cosine_sim.min():.4f}, {cosine_sim.max():.4f}]")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Jaccard Coefficient — combined (in+out) neighbourhood
# ─────────────────────────────────────────────────────────────────────────────
print("\n[2/6] Jaccard Coefficient...")

def combined_neighbors(G, node):
    return (set(G.successors(node)) | set(G.predecessors(node))) - {node}

nb_cache    = {node: combined_neighbors(G, node) for node in nodes}
jaccard_sim = np.zeros((n, n))

for i, u in enumerate(nodes):
    Nu = nb_cache[u]
    for j, v in enumerate(nodes):
        if i == j:
            jaccard_sim[i, j] = 1.0
            continue
        Nv    = nb_cache[v]
        union = Nu | Nv
        jaccard_sim[i, j] = len(Nu & Nv) / len(union) if union else 0.0

print(f"  Done. Range: [{jaccard_sim.min():.4f}, {jaccard_sim.max():.4f}]")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Hamming Similarity — 1 - normalised distance on binary combined adjacency
# ─────────────────────────────────────────────────────────────────────────────
print("\n[3/6] Hamming Similarity...")

adj_bin = np.zeros((n, n), dtype=np.int8)
for i, u in enumerate(nodes):
    for v in combined_neighbors(G, u):
        adj_bin[i, node_idx[v]] = 1

diff_count = np.array(
    [(adj_bin[i] != adj_bin).sum(axis=1) for i in range(n)],
    dtype=float
)
hamming_sim = 1.0 - diff_count / n
np.fill_diagonal(hamming_sim, 1.0)
print(f"  Done. Range: [{hamming_sim.min():.4f}, {hamming_sim.max():.4f}]")


# ─────────────────────────────────────────────────────────────────────────────
# 5-7. Three Katz variants
# ─────────────────────────────────────────────────────────────────────────────
print("\n[4/6] Katz Similarity -- weighted (raw)  [diagnostic]")
A_weighted = nx.to_numpy_array(G, nodelist=nodes)
katz_raw   = katz_matrix(A_weighted, label='Weighted')

print("\n[5/6] Katz Similarity -- binary (unweighted)")
A_binary = (A_weighted > 0).astype(float)
katz_bin = katz_matrix(A_binary, label='Binary  ')

print("\n[6/6] Katz Similarity -- log1p weights")
A_log    = np.log1p(A_weighted)
katz_log = katz_matrix(A_log, label='Log1p   ')


# ─────────────────────────────────────────────────────────────────────────────
# 6. Plot — 2x3 heatmap grid
# ─────────────────────────────────────────────────────────────────────────────
print("\nPlotting heatmaps...")

tick_labels = [name[:14] for name in nodes]

measures = [
    (cosine_sim,  'Cosine Similarity\n(Centrality Feature Vectors)',             'Blues'),
    (jaccard_sim, 'Jaccard Coefficient\n(Combined In+Out Neighbourhood)',        'Greens'),
    (hamming_sim, 'Hamming Similarity\n(1 - Norm. Distance, Binary Adj.)',       'Oranges'),
    (katz_raw,    'Katz [DIAGNOSTIC]\n(Weighted A -- degenerate alpha)',          'Reds'),
    (katz_bin,    'Katz (Binary A)\n(Unweighted -- multi-hop paths recovered)',   'Purples'),
    (katz_log,    'Katz (Log1p A)\n(Log-weight -- multi-hop paths recovered)',    'RdPu'),
]

fig, axes = plt.subplots(2, 3, figsize=(36, 22))
fig.suptitle(
    'Pairwise Node Similarity -- Epstein Email Network\n'
    'diagonal = self-similarity = 1.0  |  Katz [DIAGNOSTIC] shown for comparison only',
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
    ax.set_title(title, fontsize=11, fontweight='bold', pad=8)
    ax.tick_params(axis='x', rotation=90, labelsize=5.2)
    ax.tick_params(axis='y', rotation=0,  labelsize=5.2)

plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig('similarity_heatmaps.png', dpi=160, bbox_inches='tight')
plt.close()
print("  Saved: similarity_heatmaps.png")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Cross-measure correlation heatmap -- 6x6 Pearson r
# ─────────────────────────────────────────────────────────────────────────────
print("\nComputing cross-measure correlations...")

def upper_tri(M):
    idx = np.triu_indices(n, k=1)
    return M[idx]

measure_names = ['Cosine', 'Jaccard', 'Hamming', 'Katz(raw)', 'Katz(bin)', 'Katz(log)']
tri_vectors   = [upper_tri(m) for m, _, _ in measures]

corr = np.zeros((6, 6))
for i in range(6):
    for j in range(6):
        corr[i, j] = np.corrcoef(tri_vectors[i], tri_vectors[j])[0, 1]

fig2, ax2 = plt.subplots(figsize=(9, 7))
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
    annot_kws={'size': 11, 'weight': 'bold'},
    linewidths=0.5,
    linecolor='#cccccc',
    cbar_kws={'shrink': 0.8, 'label': 'Pearson r'}
)
ax2.set_title(
    'Cross-Measure Similarity Correlation\n'
    'Pearson r on upper-triangle pairwise scores  (n=53 nodes -> 1,378 pairs)\n'
    'Katz(raw) shown as diagnostic -- degenerate due to extreme lambda_max',
    fontsize=11, fontweight='bold', pad=10
)
plt.tight_layout()
plt.savefig('similarity_cross_correlation.png', dpi=160, bbox_inches='tight')
plt.close()
print("  Saved: similarity_cross_correlation.png")

corr_df = pd.DataFrame(corr, index=measure_names, columns=measure_names)
print("\n  Pearson r:")
print(corr_df.round(4).to_string())


# ─────────────────────────────────────────────────────────────────────────────
# 8. Save all six measures to CSV
# ─────────────────────────────────────────────────────────────────────────────
print("\nSaving similarity matrices to CSV...")

idx_i, idx_j = np.triu_indices(n, k=1)
pairs_df = pd.DataFrame({
    'node_i':    [nodes[i] for i in idx_i],
    'node_j':    [nodes[j] for j in idx_j],
    'cosine':    tri_vectors[0].round(6),
    'jaccard':   tri_vectors[1].round(6),
    'hamming':   tri_vectors[2].round(6),
    'katz_raw':  tri_vectors[3].round(6),
    'katz_bin':  tri_vectors[4].round(6),
    'katz_log':  tri_vectors[5].round(6),
})
pairs_df.to_csv('similarity_matrices.csv', index=False)
print(f"  Saved: similarity_matrices.csv  ({len(pairs_df):,} pairs)")

print("\nDone.")