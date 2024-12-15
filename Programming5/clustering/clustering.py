import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import pairwise_distances_argmin
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
import numpy as np

with open('articles.json', 'r') as file:
    data = json.load(file)

articles = [article['article'] for article in data]
cleaned_articles = [article.lower().replace('\n', ' ') for article in articles]
vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
X = vectorizer.fit_transform(cleaned_articles).toarray()

def kmeans(X, k, max_iters=100, tolerance=1e-4):
    np.random.seed(42)
    random_indices = np.random.choice(len(X), size=k, replace=False)
    centroids = X[random_indices]
    
    for iteration in range(max_iters):
        labels = pairwise_distances_argmin(X, centroids)
        new_centroids = np.array([X[labels == i].mean(axis=0) for i in range(k)])
        if np.all(np.abs(new_centroids - centroids) < tolerance):
            print(f"Convergence reached at iteration {iteration}")
            break
        
        centroids = new_centroids
    
    return labels, centroids

# Visualization function
def plot_clusters(X_reduced, labels, k, method='PCA'):
    plt.figure(figsize=(10, 7))
    for cluster in range(k):
        cluster_points = X_reduced[labels == cluster]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'Cluster {cluster}', alpha=0.6)
    
    plt.title(f"K-Means Clusters Visualization ({method})", fontsize=16)
    plt.xlabel(f"{method} Component 1", fontsize=14)
    plt.ylabel(f"{method} Component 2", fontsize=14)
    plt.legend()
    plt.show()

k = 4
labels, centroids = kmeans(X, k)

# PCA Visualization
pca = PCA(n_components=2, random_state=42)
X_pca = pca.fit_transform(X)
plot_clusters(X_pca, np.array(labels), k, method='PCA')


