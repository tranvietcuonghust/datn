import pandas as pd
import re
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
from milvus_model.sparse import BM25EmbeddingFunction
from milvus_model.sparse.bm25.tokenizers import build_default_analyzer
from tqdm import tqdm
from pyvi.ViTokenizer import tokenize
import string
# from rank_bm25 import BM25Okapi
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
# from sentence_transformers import SentenceTransformer
from milvus_model.dense import SentenceTransformerEmbeddingFunction
from fuzzywuzzy import fuzz


class RemoveDuplicate:
    def __init__(self, max_area_diff=0.03, max_price_diff=0.03, max_time_diff=1728000, min_thres=0.1, type = "ban"):
        self.max_area_diff = max_area_diff
        self.max_price_diff = max_price_diff
        self.max_time_diff = max_time_diff
        self.min_thres = min_thres
        self.thredhold = 6
        # self.analyzer = build_default_analyzer(language="vi")
        # self.bm25_ef = BM25EmbeddingFunction(self.analyzer)
        self.weights = {
            'contact_phone': 0.1,
            'district': 0.15,
            'ward': 0.2,
            'city': 0.25,
            'acreage': 0.05,
            'text_distance': 0.25
        }
        self.sentence_transformer_ef = SentenceTransformerEmbeddingFunction(
            model_name='all-MiniLM-L6-v2', # Specify the model name
            device='cpu' # Specify the device to use, e.g., 'cpu' or 'cuda:0'
        )
        # Connect to Milvus

        # connections.connect("milvus-network", host="milvus-standalone", port="19530")
        try:
            # Connect to Milvus
            connections.connect("default", host="milvus-standalone", port="19530")

            # Interact with Milvus
            if utility.has_collection(self.collection_name):
                # Your code here
                pass
        except:
            print("Error: Connection to Milvus server could not be established. Please check if the server is running and accessible.")

        # Define the schema
        self.collection_name = f"property_details_minilm_{type}"
        self.collection_name_daily = f"property_details_minilm_daily_{type}"

        
    def clean_text(self,text):
        text = re.sub('<.*?>', '', text).strip()
        text = re.sub('(\s)+', r'\1', text)
        return text
    def normalize_text(self,text):
        listpunctuation = string.punctuation.replace('_', '')
        for i in listpunctuation:
            text = text.replace(i, ' ')
        return text.lower()


    def remove_stopword(self,text):
        filename = '/opt/airflow/data/vietnamese-stopwords.csv'
        data = pd.read_csv(filename, sep="\t", encoding='utf-8')
        list_stopwords = data['stopwords']
        pre_text = []
        words = text.split()
        for word in words:
            if word not in list_stopwords:
                pre_text.append(word)
        text2 = ' '.join(pre_text)

        return text2
    def word_segment(self,sent):
        sent = tokenize(sent.encode('utf-8').decode('utf-8'))
        return sent
    def preprocess(self, text):
        text = re.sub("\d+[\.-]\d+[\.-]\d+", " dien_thoai ", text)
        text = re.sub("[mM]\s*2", " met_vuong ", text)
        text = re.sub("\d+([\.,]\d+)?", " gia_tri_so ", text)
        text = self.clean_text(text)
        text = self.word_segment(text)
        text = self.normalize_text(text)
        text = self.remove_stopword(text)
        print(">>>>>>>>>>>>>>.processed text:", text)
        return text
    def get_corpus_from_dataframe(self, df, column_name):
        # Preprocess and extract the corpus from the specified column
        corpus = df[column_name].astype(str).apply(self.preprocess).tolist()
        return corpus
    # def generate_bm25_embeddings(self, texts):
    #     print("--------------. generating BM25 embeddings")
    #     # bm25_model = BM25Okapi(texts)
    #     # embeddings = [bm25_model.get_scores(text) for text in texts]
    #     embeddings = self.bm25_ef.encode_queries(texts)
    #     return embeddings
    
    def generate_tfidf_embeddings(self, texts):
        print("--------------. generating TF-IDF embeddings")
        vectorizer = TfidfVectorizer()
        embeddings = vectorizer.fit_transform(texts)
        return embeddings.toarray()
    def generate_sentence_transformer_embeddings(self, texts):
        print("--------------. generating Sentence Transformer embeddings")
        # model = SentenceTransformer('all-MiniLM-L6-v2')  # Use a lightweight model
        embeddings = []
        for text in tqdm(texts, desc="Generating embeddings"):
            embedding = self.sentence_transformer_ef.encode_queries([text])
            embeddings.append(embedding)
        return np.vstack(embeddings)
    def insert_vectors(self, vectors, df):
        dense_vectors = vectors
        ids =  df.index.tolist()
        df = df.astype(str)
        contact_phone_list = df['Contact_phone'].tolist()
        url_list = df['URL'].tolist()
        acreage_list = df['Acreage'].tolist()
        district_list = df['District'].tolist()
        ward_list = df['Ward'].tolist()
        city_list = df['City'].tolist()
        # dim = dense_vectors[0].shape[0]
        # dim = vectors[0].shape[1]
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim = 384),
            FieldSchema(name="contact_phone", dtype=DataType.VARCHAR, max_length=255),
            FieldSchema(name="url", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="acreage", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="district", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="ward", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="city", dtype=DataType.VARCHAR,max_length=255)    
        ]
        schema = CollectionSchema(fields, "Property details collection")
        if utility.has_collection(self.collection_name):
            self.collection = Collection(self.collection_name)
            # self.collection.drop()
        else:
            self.collection = Collection(self.collection_name, schema)
        # Create collection
        
        # print("<<<<<<<<<<<<<<<<<+",len(ids))
        # print("><<<<<<<<<<<<<<<<",vectors.shape[0])
            # Insert the vectors in batches
        batch_size = 1000
        for i in range(0, dense_vectors.shape[0], batch_size):
            batch_vectors = dense_vectors[i:i+batch_size]
            batch_ids = ids[i:i+batch_size]
            batch_contact_phone = contact_phone_list[i:i+batch_size]
            batch_url = url_list[i:i+batch_size]
            batch_acreage = acreage_list[i:i+batch_size]
            batch_district = district_list[i:i+batch_size]
            batch_ward = ward_list[i:i+batch_size]
            batch_city = city_list[i:i+batch_size]

            entities = [
                batch_ids,
                batch_vectors,
                batch_contact_phone,
                batch_url,
                batch_acreage,
                batch_district,
                batch_ward,
                batch_city
            ]
            self.collection.insert(entities)

    def insert_vectors_daily(self, vectors, df):
        dense_vectors = vectors
        ids =  df.index.tolist()
        df = df.astype(str)
        contact_phone_list = df['Contact_phone'].tolist()
        url_list = df['URL'].tolist()
        acreage_list = df['Acreage'].tolist()
        district_list = df['District'].tolist()
        ward_list = df['Ward'].tolist()
        city_list = df['City'].tolist()
        # dim = dense_vectors[0].shape[0]
        # dim = vectors[0].shape[1]
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim = 384),
            FieldSchema(name="contact_phone", dtype=DataType.VARCHAR, max_length=255),
            FieldSchema(name="url", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="acreage", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="district", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="ward", dtype=DataType.VARCHAR,max_length=255),
            FieldSchema(name="city", dtype=DataType.VARCHAR,max_length=255)    
        ]
        schema = CollectionSchema(fields, "Property details collection")
        if utility.has_collection(self.collection_name_daily):
            self.collection = Collection(self.collection_name_daily)
            self.collection.drop()
        else:
            self.collection = Collection(self.collection_name_daily, schema)
        # Create collection
        
        # print("<<<<<<<<<<<<<<<<<+",len(ids))
        # print("><<<<<<<<<<<<<<<<",vectors.shape[0])
            # Insert the vectors in batches
        batch_size = 1000
        for i in range(0, dense_vectors.shape[0], batch_size):
            batch_vectors = dense_vectors[i:i+batch_size]
            batch_ids = ids[i:i+batch_size]
            batch_contact_phone = contact_phone_list[i:i+batch_size]
            batch_url = url_list[i:i+batch_size]
            batch_acreage = acreage_list[i:i+batch_size]
            batch_district = district_list[i:i+batch_size]
            batch_ward = ward_list[i:i+batch_size]
            batch_city = city_list[i:i+batch_size]

            entities = [
                batch_ids,
                batch_vectors,
                batch_contact_phone,
                batch_url,
                batch_acreage,
                batch_district,
                batch_ward,
                batch_city
            ]
            self.collection.insert(entities)

    def check_constrain(self, item1, item2, distance):
        item1_contact_phone=item1['Contact_phone']
        item1_district=item1['District']
        item1_ward=item1['Ward']
        item1_city= item1['City']
        item1_acreage=item1['Acreage']
        item2_contact_phone=item2['Contact_phone']
        item2_district=item2['District']
        item2_ward=item2['Ward']
        item2_city= item2['City']
        item2_acreage=item2['Acreage']
        if item2_city != "" and item1_city != "" and item1_city != item2_city:
            return False
        if item2_district != "" and item1_district != "" and item1_district != item2_district:
            return False
        scores = {
            'contact_phone': 0.1*fuzz.partial_ratio(item1_contact_phone, item2_contact_phone),
            'district': 0.1*fuzz.partial_ratio(item1_district, item2_district),
            'ward': 0.1*fuzz.partial_ratio(item1_ward, item2_ward),
            'city': 0.1*fuzz.partial_ratio(item1_city, item2_city),
            'acreage': 0.1*fuzz.partial_ratio(str(item1_acreage), str(item2_acreage)),
            'text_distance' : 10*distance
        }

        # scores = {
        #     'contact_phone': 1 if item1['Contact_phone'] == item2['Contact_phone'] else 0,
        #     'district': 1 if item1['District'] == item2['District'] else 0,
        #     'ward': 1 if item1['Ward'] == item2['Ward'] else 0,
        #     'city': 1 if item1['City'] == item2['City'] else 0,
        #     'acreage': fuzz.partial_ratio(str(item1_acreage), str(item2_acreage)),
        #     'text_distance' : distance
        # }
        print("--------Comparing:",item1['URL'],"and",item2['URL'] ,"----------------")
        print("Scores:", scores)

        weighted_score = sum(self.weights[key] * scores[key] for key in scores) / sum(self.weights.values())
        print("Weighted score:", weighted_score)
        if weighted_score < self.thredhold:
            
            return True
        return False
    
    # def load_data_to_milvus(self, df):
    #     df['milvus_id'] = range(1, len(df) + 1)  # Ensure unique IDs
    #     texts = [self.preprocess(text) for text in df["Description"]]
    #     X_vectors = self.generate_bm25_embeddings(texts)

    #     # Insert vectors into Milvus
    #     self.insert_vectors(X_vectors, df['milvus_id'].tolist())


    def remove_inside(self, df):
            # corpus = self.get_corpus_from_dataframe(df, "Description")
            # self.bm25_ef.fit(corpus)
        df['milvus_id'] = range(1, len(df) + 1)  # Ensure unique IDs
        texts = [self.preprocess(text) for text in df["Description"]]
        # X_vectors = self.generate_bm25_embeddings(texts)
        # X_vectors = self.generate_tfidf_embeddings(texts)
        X_vectors = self.generate_sentence_transformer_embeddings(texts)
        # X_vectors = [vector.toarray()[0] for vector in X_vectors]
        # Insert vectors into Milvus
        print("inserting vectors to milvus")
        self.insert_vectors(X_vectors, df)
        
        # Create an index
        self.collection.create_index(field_name="embedding", index_params={"index_type": "FLAT", "metric_type": "L2", "params": {}})

        # Load collection into memory
        self.collection.load()

        # Search for similar vectors
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        print("loading vectors from milvus")
        # results = self.collection.search(X_vectors, "embedding", search_params, limit=200, expr=None)

        size = len(df)
        drop_set = set()
        batch_size = 1000
        for i in range(0, len(X_vectors), batch_size):
              # Add a progress bar
            print("Processing batch", i, "out of", size)
            batch_vectors = X_vectors[i:i+batch_size]
            results = self.collection.search(batch_vectors, "embedding", search_params, limit=100,output_fields =["url"])

            for j, result in enumerate(results):
                # print("result",result)
                current_id = df.iloc[i+j].name
                if current_id in drop_set:
                    print("Skipping", current_id, "because it's already in the drop set")
                    continue
                for hit in result[1:]:  # Skip the first hit because it's the query itself
                    # print(hit)
                    if hit.distance < self.min_thres:
                        item1 = df.iloc[i+j]
                        # item2 =df.loc[hit.id]
                        # item2 = df[df['URL']==hit.url]
                        filtered_df = df[df['URL']==hit.url]
                        if not filtered_df.empty:
                            item2 = filtered_df.iloc[0]
                        else:
                            continue 
                        if self.check_constrain(item1, item2, hit.distance):
                            print("Dropping", hit.id, "because it's similar to", current_id)
                            drop_set.add(hit.url)

        keep = set(df.index).difference(drop_set)
        return  df[~df['URL'].isin(drop_set)]

    def remove_inside_daily(self, df):
        # corpus = self.get_corpus_from_dataframe(df, "Description")
        # self.bm25_ef.fit(corpus)
        df['milvus_id'] = range(1, len(df) + 1)  # Ensure unique IDs
        texts = [self.preprocess(text) for text in df["Description"]]
        # X_vectors = self.generate_bm25_embeddings(texts)
        # X_vectors = self.generate_tfidf_embeddings(texts)
        X_vectors = self.generate_sentence_transformer_embeddings(texts)
        # X_vectors = [vector.toarray()[0] for vector in X_vectors]
        # Insert vectors into Milvus
        print("inserting vectors to milvus")
        self.insert_vectors_daily(X_vectors, df)
        
        # Create an index
        self.collection.create_index(field_name="embedding", index_params={"index_type": "FLAT", "metric_type": "L2", "params": {}})

        # Load collection into memory
        self.collection.load()

        # Search for similar vectors
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        print("loading vectors from milvus")
        # results = self.collection.search(X_vectors, "embedding", search_params, limit=200, expr=None)

        size = len(df)
        drop_set = set()
        batch_size = 1000
        for i in range(0, len(X_vectors), batch_size):
              # Add a progress bar
            print("Processing batch", i, "out of", size)
            batch_vectors = X_vectors[i:i+batch_size]
            results = self.collection.search(batch_vectors, "embedding", search_params, limit=100,output_fields =["url"])

            for j, result in enumerate(results):
                # print("result",result)
                current_id = df.iloc[i+j].name
                if current_id in drop_set:
                    print("Skipping", current_id, "because it's already in the drop set")
                    continue
                for hit in result[1:]:  # Skip the first hit because it's the query itself
                    # print(hit)
                    if hit.distance < self.min_thres:
                        item1 = df.iloc[i+j]
                        # item2 =df.loc[hit.id]
                        # item2 = df[df['URL']==hit.url]
                        filtered_df = df[df['URL']==hit.url]
                        if not filtered_df.empty:
                            item2 = filtered_df.iloc[0]
                        else:
                            continue 
                        if self.check_constrain(item1, item2, hit.distance):
                            print("Dropping", hit.id, "because it's similar to", current_id)
                            drop_set.add(hit.url)
        keep = set(df.index).difference(drop_set)
        return  df[~df['URL'].isin(drop_set)]
    def remove_duplicate_daily(self, df):
        removed_inside_df = self.remove_inside_daily(df)
        return removed_inside_df

        

# Example usage
# if __name__ == "__main__":
#     df = pd.read_csv('./processed_data/merged_dataframe.csv', encoding='utf-8')
#     df = df.drop_duplicates(subset='URL', keep='first')
#     df = df.astype(str)
#     ban_df =  df[df["Type"] == "ban"]
#     thue_df = df[df["Type"] == "thue"]
#     ban_deduplicator = RemoveDuplicate(type="ban")
#     ban_deduplicated_df = ban_deduplicator.remove_inside(ban_df)
#     thue_deduplicator = RemoveDuplicate(type="thue")
#     thue_deduplicated_df = thue_deduplicator.remove_inside(thue_df)
#     deduplicated_df = pd.concat([ban_deduplicated_df, thue_deduplicated_df])
#     deduplicated_df.to_csv('./processed_data/deduplicated_data_minilm.csv', index=False)
#     print(deduplicated_df)
