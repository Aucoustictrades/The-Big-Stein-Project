import pandas as pd
import networkx as nx
from pyvis.network import Network
import math

edges = pd.read_csv('edge_list.csv')

G = nx.DiGraph()
for _, row in edges.iterrows():
    G.add_edge(row['source'], row['target'], weight=row['weight'])

net = Network(height='900px', width='100%', directed=True, notebook=False)

betweenness = nx.betweenness_centrality(G, weight='weight')
for node in G.nodes():
    size = 10 + (betweenness[node] * 200)
    net.add_node(node, label=node, size=size, title=f"{node}\nBetweenness: {betweenness[node]:.3f}")

for u, v, data in G.edges(data=True):
    width = max(0.5, math.log(data['weight'] + 1) * 0.5)
    net.add_edge(u, v, width=width, title=f"{data['weight']} emails")

net.set_options("""
{
  "physics": {
    "barnesHut": {
      "gravitationalConstant": -8000,
      "springLength": 200,
      "springConstant": 0.02
    }
  },
  "nodes": {
    "font": {"size": 14}
  }
}
""")

net.save_graph('network.html')
print("Saved to network.html")