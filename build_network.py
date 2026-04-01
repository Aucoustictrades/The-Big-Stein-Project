import duckdb
import pandas as pd
import json
import re
import networkx as nx
from collections import defaultdict

conn = duckdb.connect()

PARQUET_PATH = r'C:\Users\conor\Downloads\emails-slim.parquet'

# ── 1. Name normalization map ────────────────────────────────────────────────
# Maps messy sender strings → canonical names
NAME_MAP = {
    # Epstein variants
    "jeffrey E.":                               "Jeffrey Epstein",
    "jeffrey E. <jeevacation@gmail.com>":       "Jeffrey Epstein",
    "Jeffrey Epstein <jeevacation@gmail.com>":  "Jeffrey Epstein",
    "jeevacation@gmail.com":                    "Jeffrey Epstein",
    "J <jeevacation@gmail.com>":                "Jeffrey Epstein",
    "jeffrey epstein <jeevacation@gmail.com>":  "Jeffrey Epstein",
    "Jeffrey E. <jeevacation@gmail.com>":       "Jeffrey Epstein",
    "Jeevacation <jeevacation@gmail.com>":      "Jeffrey Epstein",
    "1 <jeevacation@gmail.com>":                "Jeffrey Epstein",
    "Jeffrey <jeevacation@gmail.com>":          "Jeffrey Epstein",
    "jeffrey epstein":                          "Jeffrey Epstein",
    "Jeffrey E.":                               "Jeffrey Epstein",
    "Jeffrey":                                  "Jeffrey Epstein",
    "Jeevacation":                              "Jeffrey Epstein",
    "J. Epstein <jeeproject@yahoo.com>":        "Jeffrey Epstein",
    # Maxwell variants
    "G. Max":                                   "Ghislaine Maxwell",
    "Gmax":                                     "Ghislaine Maxwell",
    "Gmax <gmax1@ellmax.com>":                  "Ghislaine Maxwell",
    # Groff variants
    "Lesley Groff <unknown>":                   "Lesley Groff",
    "Lesley Groff <lesley@nysgllc.com>":        "Lesley Groff",
    # Kahn variants
    "Richard Kahn <unknown>":                   "Richard Kahn",
    # Molotkova variants
    "Natalia (Natasha) Molotkova":              "Natalia Molotkova",
    "Natalia Molotkova <█████████>":            "Natalia Molotkova",
    # Klein variants
    "bellaklein":                               "Bella Klein",
    # Gordon variants
    "brice gordon":                             "Brice Gordon",
    # Rothschild variants
    "A. de Rothschild":                         "Ariane de Rothschild",
    # Hanson variants
    "Stephen Hanson":                           "Steve Hanson",
    #Ehud Barak variants
    "ehud barak  <ehbarak1@gmail.com>":         "Ehud Barak",
}

# ── 2. Email address → canonical name map ────────────────────────────────────
EMAIL_MAP = {
    # Epstein
    "jeevacation@gmail.com":         "Jeffrey Epstein",
    "Jeevacation@gmail.com":         "Jeffrey Epstein",
    "jeeproject@yahoo.com":          "Jeffrey Epstein",
    "JEEPROJECT@YAHOO.COM":          "Jeffrey Epstein",
    "littlestjeff@yahoo.com":        "Jeffrey Epstein",
    "jeevacation@gmail.corn":        "Jeffrey Epstein",  # typo in data
    # Maxwell
    "gmax1@mindspring.com":          "Ghislaine Maxwell",
    "gmax1@ellmax.com":              "Ghislaine Maxwell",
    # Groff
    "lesley@nysgllc.com":            "Lesley Groff",
    # Kahn
    "richardkahn12@gmail.com":       "Richard Kahn",
    # Visoski
    "LVJET@aol.com":                 "Larry Visoski",
    # Klein
    "bklein575@gmail.com":           "Bella Klein",
    # Sulayem
    "ssulayem@aol.com":              "Sultan Bin Sulayem",
    # Hanna
    "ehanna@nysgmail.com":           "Emad Hanna",
    # Indyke
    "DKIESQ@aol.com":                "Darren Indyke",
    "dkiesq@aol.com":                "Darren Indyke",
    # Barak
    "ehbarak1@gmail.com":            "Ehud Barak",
    # Nemcova
    "petranemcova@mac.com":          "Petra Nemcova",
    # Mandelson
    "petermandelson@btinternet.com": "Peter Mandelson",
    # Oldfield (Deutsche Bank)
    "stewart.oldfield@db.com":       "Stewart Oldfield",
    # Molyneux
    "jpm6929@aol.com":               "Juan Pablo Molyneux",
    "Jpm6929@aol.com":               "Juan Pablo Molyneux",
}

# ── 3. Whitelist of real identifiable humans ─────────────────────────────────
WHITELIST = {
    "Jeffrey Epstein", "Lesley Groff", "Richard Kahn",
    "Karyna Shuliak", "Larry Visoski", "Natalia Molotkova",
    "Ann Rodriquez", "Boris Nikolic", "Kathy Ruemmler",
    "Bella Klein", "Stewart Oldfield", "Daphne Wallace",
    "Paul Morris", "Cecile de Jongh", "David Mitchell",
    "Lawrence Krauss", "David Stern", "Melanie Spinella",
    "Joi Ito", "Eva Dubin", "Peggy Siegal", "Faith Kates",
    "Deepak Chopra", "Richard Joslin", "Eric Roth",
    "Farkas, Andrew L.", "Vahe Stepanian", "Brice Gordon",
    "Tazia Smith", "Darren Indyke", "Sultan Bin Sulayem",
    "Michael Wolff", "Noam Chomsky", "Pritzker, Tom",
    "Ariane de Rothschild", "Jean Luc Brunel", "Gary Kerney",
    "Eileen Alexanderson", "Nicole Junkermann", "Brad Wechsler",
    "Ghislaine Maxwell", "Larry Summers", "Steve Hanson",
    "Ian Osborne", "Emad Hanna", "Ramsey Elkholy",
    "Nowak, Martin", "Jes Staley", "Valeria Chomsky",
    "Ehud Barak", "Petra Nemcova", "Peter Mandelson",
    "Juan Pablo Molyneux",
}


# ── 4. Helper: parse a name from RFC 2822 string ─────────────────────────────
def parse_display_name(raw: str):
    """
    Extract canonical name from strings like:
      'Ghislaine Maxwell <gmax1@mindspring.com>'
      '<gmax1@mindspring.com>'
      'Ghislaine Maxwell'
    Returns canonical name if resolvable, else None.
    """
    raw = str(raw).strip()

    # Try email address first
    email_match = re.search(r'[\w\.\-]+@[\w\.\-]+', raw)
    if email_match:
        email = email_match.group(0)
        if email in EMAIL_MAP:
            return EMAIL_MAP[email]

    # Try display name (text before <...)
    name_match = re.match(r'^([^<]+)', raw)
    if name_match:
        display = name_match.group(1).strip().strip("'\"")
        if display in NAME_MAP:
            return NAME_MAP[display]
        if display in WHITELIST:
            return display

    return None


# ── 5. Helper: parse JSON recipient list ─────────────────────────────────────
def parse_recipients(json_str):
    """Parse a JSON array of recipient strings into canonical names."""
    if not json_str or json_str == '[]':
        return []
    try:
        entries = json.loads(json_str)
    except Exception:
        return []
    names = []
    for entry in entries:
        name = parse_display_name(entry)
        if name and name in WHITELIST:
            names.append(name)
    return names


# ── 6. Load and filter data ───────────────────────────────────────────────────
print("Loading data...")
df = conn.sql(f"""
    SELECT sender, to_recipients, cc_recipients, bcc_recipients, sent_at, epstein_is_sender
    FROM read_parquet('{PARQUET_PATH}')
    WHERE is_promotional = false
    AND sender IS NOT NULL
""").df()
print(f"Loaded {len(df):,} rows")

# Normalize sender names
df['sender_clean'] = df['sender'].map(NAME_MAP).fillna(df['sender'])

# Filter to whitelist senders only
df = df[df['sender_clean'].isin(WHITELIST)]
print(f"After whitelist filter: {len(df):,} rows")


# ── 7. Build directed weighted edge list ─────────────────────────────────────
print("Building edges...")
edge_counts = defaultdict(int)

for _, row in df.iterrows():
    sender = row['sender_clean']
    recipients = (
        parse_recipients(row['to_recipients']) +
        parse_recipients(row['cc_recipients']) +
        parse_recipients(row['bcc_recipients'])
    )
    for recipient in recipients:
        if recipient != sender:  # no self-loops
            edge_counts[(sender, recipient)] += 1

print(f"Total directed edges: {len(edge_counts):,}")


# ── 8. Build NetworkX graph ───────────────────────────────────────────────────
print("Building graph...")
G = nx.DiGraph()
G.add_nodes_from(WHITELIST)

for (src, dst), weight in edge_counts.items():
    G.add_edge(src, dst, weight=weight)

# Remove isolates (whitelist people with no edges in the data)
isolates = list(nx.isolates(G))
G.remove_nodes_from(isolates)
print(f"Removed {len(isolates)} isolates")
print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")


# ── 9. Basic network stats ────────────────────────────────────────────────────
print("\n=== NETWORK STATS ===")
print(f"Nodes:              {G.number_of_nodes()}")
print(f"Edges:              {G.number_of_edges()}")
print(f"Density:            {nx.density(G):.4f}")

# Top nodes by in-degree (who receives the most emails from inner circle)
print("\n--- Top 10 by In-Degree ---")
in_deg = sorted(G.in_degree(weight='weight'), key=lambda x: x[1], reverse=True)
for name, deg in in_deg[:10]:
    print(f"  {name}: {deg}")

# Top nodes by out-degree (who sends the most)
print("\n--- Top 10 by Out-Degree ---")
out_deg = sorted(G.out_degree(weight='weight'), key=lambda x: x[1], reverse=True)
for name, deg in out_deg[:10]:
    print(f"  {name}: {deg}")

# Betweenness centrality
print("\n--- Top 10 by Betweenness Centrality ---")
betweenness = nx.betweenness_centrality(G, weight='weight')
for name, score in sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {name}: {score:.4f}")

# ── Eigenvector Centrality ────────────────────────────────────────────────────
print("\n--- Top 10 by Eigenvector Centrality ---")
try:
    eigenvector = nx.eigenvector_centrality(G, weight='weight', max_iter=1000)
    for name, score in sorted(eigenvector.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {name}: {score:.4f}")
except nx.PowerIterationFailedConvergence:
    print("  Eigenvector failed to converge - try increasing max_iter")

# ── Katz Centrality ───────────────────────────────────────────────────────────
print("\n--- Top 10 by Katz Centrality ---")
# alpha must be < 1/largest eigenvalue
eigenvalues = nx.adjacency_spectrum(G)
lambda1 = max(abs(eigenvalues)).real
alpha = 0.9 / lambda1
katz = nx.katz_centrality(G, alpha=alpha, weight='weight')
for name, score in sorted(katz.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {name}: {score:.4f}")

# ── PageRank ──────────────────────────────────────────────────────────────────
print("\n--- Top 10 by PageRank ---")
pagerank = nx.pagerank(G, weight='weight')
for name, score in sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {name}: {score:.4f}")

# ── Closeness Centrality ──────────────────────────────────────────────────────
print("\n--- Top 10 by Closeness Centrality ---")
closeness = nx.closeness_centrality(G)
for name, score in sorted(closeness.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {name}: {score:.4f}")

# ── Degree Centrality ─────────────────────────────────────────────────────────
print("\n--- Top 10 by Degree Centrality (undirected) ---")
G_undirected = G.to_undirected()
degree_centrality = nx.degree_centrality(G_undirected)
for name, score in sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {name}: {score:.4f}")

# ── Clustering Coefficient ────────────────────────────────────────────────────
print("\n--- Top 10 by Clustering Coefficient (undirected) ---")
clustering = nx.clustering(G_undirected)
print(f"  Average clustering coefficient: {nx.average_clustering(G_undirected):.4f}")
for name, score in sorted(clustering.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {name}: {score:.4f}")

# ── Ego Network (Epstein removed) ─────────────────────────────────────────────
print("\n--- Ego Network Analysis (Epstein removed) ---")
G_no_epstein = G.copy()
G_no_epstein.remove_node("Jeffrey Epstein")
print(f"  Nodes: {G_no_epstein.number_of_nodes()}")
print(f"  Edges: {G_no_epstein.number_of_edges()}")
print(f"  Density: {nx.density(G_no_epstein):.4f}")

print("\n  Top 10 by Betweenness (no Epstein):")
bet_no_je = nx.betweenness_centrality(G_no_epstein, weight='weight')
for name, score in sorted(bet_no_je.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"    {name}: {score:.4f}")

print("\n  Top 10 by PageRank (no Epstein):")
pr_no_je = nx.pagerank(G_no_epstein, weight='weight')
for name, score in sorted(pr_no_je.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"    {name}: {score:.4f}")

# ── Save all centrality scores ────────────────────────────────────────────────
all_nodes = list(G.nodes())
centrality_df = pd.DataFrame({
    'node': all_nodes,
    'betweenness': [betweenness[n] for n in all_nodes],
    'eigenvector': [eigenvector.get(n, 0) for n in all_nodes],
    'katz': [katz[n] for n in all_nodes],
    'pagerank': [pagerank[n] for n in all_nodes],
    'closeness': [closeness[n] for n in all_nodes],
    'degree': [degree_centrality[n] for n in all_nodes],
    'clustering': [clustering[n] for n in all_nodes],
    'in_degree_weighted': [G.in_degree(n, weight='weight') for n in all_nodes],
    'out_degree_weighted': [G.out_degree(n, weight='weight') for n in all_nodes],
})
centrality_df.to_csv('centrality_scores.csv', index=False)
print("\nAll centrality scores saved to centrality_scores.csv")


# ── 10. Save outputs ──────────────────────────────────────────────────────────
edges_df = pd.DataFrame(
    [(u, v, d['weight']) for u, v, d in G.edges(data=True)],
    columns=['source', 'target', 'weight']
)
edges_df.to_csv('edge_list.csv', index=False)
print("\nEdge list saved to edge_list.csv")

nodes_df = pd.DataFrame({'node': list(G.nodes())})
nodes_df.to_csv('node_list.csv', index=False)
print("Node list saved to node_list.csv")