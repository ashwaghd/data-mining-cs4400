import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

with open('datasets/jaccard_similarity_results.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)
df[['Article 1', 'Article 2']] = pd.DataFrame(df['Article Pair'].tolist(), index=df.index)
df = df.drop(columns=['Article Pair'])
articles = pd.unique(df[['Article 1', 'Article 2']].values.ravel('K'))
pivot_df = pd.DataFrame(index=articles, columns=articles)

for idx, row in df.iterrows():
    art1 = row['Article 1']
    art2 = row['Article 2']
    sim = row['Jaccard Similarity']
    pivot_df.loc[art1, art2] = sim
    pivot_df.loc[art2, art1] = sim  
np.fill_diagonal(pivot_df.values, 1)

pivot_df = pivot_df.fillna(0)

plt.figure(figsize=(10, 8))
sns.heatmap(pivot_df.astype(float), annot=True, cmap="coolwarm", cbar=True)
plt.title("Jaccard Similarity Heatmap")
plt.savefig("heatmap.png")
print("Heatmap saved as heatmap.png")
