import pandas as pd

df = pd.read_csv('similarity_matrices.csv')

# Pairs where Hamming is high but Katz(bin) is low — peripheral pairs
peripheral = df[(df['hamming'] > 0.75) & (df['katz_bin'] < 0.2)]
print(f"High-Hamming / Low-Katz pairs: {len(peripheral)}")
print(peripheral.sort_values('hamming', ascending=False).head(10).to_string(index=False))

print()

# Pairs where Katz(bin) is high but Hamming is low — core pairs
core = df[(df['katz_bin'] > 0.5) & (df['hamming'] < 0.5)]
print(f"High-Katz / Low-Hamming pairs: {len(core)}")
print(core.sort_values('katz_bin', ascending=False).head(10).to_string(index=False))