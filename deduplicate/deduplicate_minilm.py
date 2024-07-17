import pandas as pd
import re
import torch
from transformers import AutoTokenizer, AutoModel
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
from sentence_transformers import SentenceTransformer
import torch.nn.functional as F
import tqdm


class RemoveDuplicate:
    def __init__(self, max_area_diff=0.03, max_price_diff=0.03, max_time_diff=1728000, min_thres=10):
        self.max_area_diff = max_area_diff
        self.max_price_diff = max_price_diff
        self.max_time_diff = max_time_diff
        self.min_thres = min_thres
        self.model_name = "sentence-transformers/all-MiniLM-L6-v2"
        
        # Load PhoBERT model and tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        
        # Connect to Milvus
        connections.connect("default", host="localhost", port="19530")
        
        # Define the schema
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384)
        ]
        schema = CollectionSchema(fields, "Property details collection")
        
        # Create collection
        self.collection_name = "property_details_minilm_2"
        if utility.has_collection(self.collection_name):
            self.collection = Collection(self.collection_name)
            self.collection.drop()
            
        self.collection = Collection(self.collection_name, schema)

    

    def preprocess(self, text):
        text = re.sub("\d+[\.-]\d+[\.-]\d+", " dien_thoai ", text)
        text = re.sub("[mM]\s*2", " met_vuong ", text)
        text = re.sub("\d+([\.,]\d+)?", " gia_tri_so ", text)
        print(">>>>>>>>>>>>>>.processed text:", text)
        return text

    def get_corpus_from_dataframe(self, df, column_name):
        # Preprocess and extract the corpus from the specified column
        corpus = df[column_name].astype(str).apply(self.preprocess).tolist()
        return corpus

    def generate_embeddings(self, texts):
        def mean_pooling(model_output, attention_mask):
            token_embeddings = model_output[0] #First element of model_output contains all token embeddings
            input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
            return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)
        print("--------------. embedding text")
        inputs = self.tokenizer(texts, return_tensors='pt', padding=True, truncation=True)
        with torch.no_grad():
            outputs = self.model(**inputs)
        embeddings = mean_pooling(outputs, inputs['attention_mask'])
        embeddings=F.normalize(embeddings, p=2, dim=1)
        return embeddings

    def insert_vectors(self, vectors, ids):
        # entities = [ids, vectors.tolist()]
        batch_size = 1000
        for i in range(0, len(vectors), batch_size):
            batch_vectors = vectors[i:i+batch_size]
            batch_ids = ids[i:i+batch_size]
            entities = [batch_ids, batch_vectors.tolist()]
            self.collection.insert(entities)

    def check_constrain(self, item1, item2):
        return True

    def remove_inside(self, df):
        df['milvus_id'] = range(1, len(df) + 1) # Ensure unique IDs
        texts = [self.preprocess(text) for text in df["Description"]]
        X_vectors = self.generate_embeddings(texts)
        
        # Insert vectors into Milvus
        self.insert_vectors(X_vectors, df['milvus_id'].tolist())
        
        # Create an index
        self.collection.create_index(field_name="embedding", index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
        
        # Load collection into memory
        self.collection.load()

        # Search for similar vectors
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}

        size = len(df)
        drop_set = set()
        batch_size = 1000
        for i in range(0, len(X_vectors), batch_size):  # Add a progress bar
            batch_vectors = X_vectors[i:i+batch_size]
            results = self.collection.search(batch_vectors, "embedding", search_params, limit=100)

            for j, result in enumerate(results):
                for hit in result[1:]:  # Skip the first hit because it's the query itself
                    if hit.distance < self.min_thres:
                        item1 = df.iloc[i+j]
                        item2 = df.loc[hit.id]
                        if self.check_constrain(item1, item2):
                            drop_set.add(hit.id)

        keep = set(df.index).difference(drop_set)
        return df.loc[list(keep)]

    def remove_duplicate(self, df):
        if len(df) > 0:
            df.set_index(["Ward", "District", "City", "RE_Type"], inplace=True)
            df.sort_index(inplace=True)
            result = []
            for ward, district, province, type in set(df.index):
                df_sub = df.loc[[(ward, district, province, type)]]
                df_sub = self.remove_inside(df_sub)
                result.append(df_sub)
            df = pd.concat(result, ignore_index=True)
            return df
        else:
            return None

# Example usage
if __name__ == "__main__":
    df = pd.read_csv('./processed_data/merged_dataframe.csv', encoding='utf-8')
    df = df.astype(str)

    deduplicator = RemoveDuplicate()
    deduplicated_df = deduplicator.remove_inside(df)
    deduplicated_df.to_csv('./processed_data/deduplicated_data_minilm.csv', index=False)
    print(deduplicated_df)
