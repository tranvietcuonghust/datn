import pandas as pd
import re
import torch
from transformers import AutoTokenizer, AutoModel
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility


class RemoveDuplicate:
    def __init__(self, max_area_diff=0.03, max_price_diff=0.03, max_time_diff=1728000, min_thres=0.8):
        self.max_area_diff = max_area_diff
        self.max_price_diff = max_price_diff
        self.max_time_diff = max_time_diff
        self.min_thres = min_thres
        self.model_name = "vinai/phobert-base"
        
        # Load PhoBERT model and tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        
        # Connect to Milvus
        connections.connect("default", host="localhost", port="19530")
        
        # Define the schema
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=768)
        ]
        schema = CollectionSchema(fields, "Property details collection")
        
        # Create collection
        self.collection_name = "property_details_8"
        if utility.has_collection(self.collection_name):
            self.collection = Collection(self.collection_name)
        else:
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
        print("--------------. embedding text")
        inputs = self.tokenizer(texts, return_tensors='pt', padding=True, truncation=True)
        with torch.no_grad():
            outputs = self.model(**inputs)
        embeddings = outputs.last_hidden_state.mean(dim=1).cpu().numpy()
        return embeddings

    def insert_vectors(self, vectors, ids):
        entities = [ids, vectors.tolist()]
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
        results = self.collection.search(X_vectors.tolist(), "embedding", search_params, limit=200, expr=None)

        size = len(df)
        drop_set = set()
        for i in range(size):
            for j in range(1, len(results[i])):
                if results[i][j].distance < self.min_thres:
                    item1 = df.iloc[i]
                    matched_indices = df.index[df['milvus_id'] == results[i][j].id].tolist()
                    if matched_indices:  # Check if the list is not empty
                        item2_idx = matched_indices[0]
                        item2 = df.loc[item2_idx]
                        if self.check_constrain(item1, item2):
                            drop_set.add(item2_idx)

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
    deduplicated_df = deduplicator.remove_duplicate(df)
    deduplicated_df.to_csv('./processed_data/deduplicated_data.csv', index=False)
    print(deduplicated_df)
