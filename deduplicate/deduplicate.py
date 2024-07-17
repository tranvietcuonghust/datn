import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from gensim.models.fasttext import load_facebook_model

# Assuming 'df' is your DataFrame and the necessary columns and model are already set

def vectorize_text(text, model):
    words = text.split()
    word_vectors = [model.wv[word] for word in words if word in model.wv]
    return np.mean(word_vectors, axis=0) if word_vectors else np.zeros(300)

# Load FastText model
print("loading model")
model = load_facebook_model('./cc.vi.300.bin')
print("model loaded")
df = pd.read_csv('./processed_data/merged_dataframe.csv',encoding='utf-8')
df['combined_text'] = df['Title'] + ' ' + df['Description']
df['combined_text'] = df['combined_text'].astype(str)

# Vectorize text
df['vectorized_text'] = df['combined_text'].apply(lambda x: vectorize_text(x, model))
print("vectorized")
# Combine text vectors with numeric features
scaler = StandardScaler()
# price_scaled = scaler.fit_transform(df[['Acreage']])
text_vectors = np.stack(df['vectorized_text'].values)
# 
combined_vectors =text_vectors 
#combined_vectors = np.hstack((text_vectors, price_scaled))

# FAISS indexing and search
import faiss
num_dimensions = combined_vectors.shape[1]
index = faiss.IndexFlatL2(num_dimensions)
index.add(combined_vectors.astype(np.float32))
k = 200  # consider adjusting k based on dataset size
D, I = index.search(combined_vectors.astype(np.float32), k)

# Set a similarity threshold
threshold = 0.1  # Example threshold, adjust based on your data

# Identify duplicates based on the threshold
is_duplicate = np.zeros(len(df), dtype=bool)
for i in range(len(D)):
    for j in range(1, k):  # skip i=j (self-matching)
        if D[i][j] < threshold:
            is_duplicate[I[i][j]] = True

# Filter DataFrame to remove duplicates
df_deduplicated = df[~is_duplicate]

# Display the result
print(df.count())
print(df_deduplicated.count())

df_deduplicated.to_csv('./processed_data/dedupicated_data.csv',index=False)
