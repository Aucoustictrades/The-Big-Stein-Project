import pandas as pd
df = pd.read_csv('similarity_matrices.csv')

core_loose = df[(df['katz_bin'] > 0.3) & (df['hamming'] < 0.65)]
print(f"Loosened core pairs: {len(core_loose)}")
print(core_loose.sort_values('katz_bin', ascending=False).head(20).to_string(index=False))