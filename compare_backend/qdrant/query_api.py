from flask import Flask, request, jsonify
import torch
import pandas as pd
from transformers import AutoModelForMaskedLM, AutoTokenizer
from sentence_transformers import SentenceTransformer
from langchain.schema import Document
from qdrant_client import QdrantClient, models

# Initialize Flask App
app = Flask(__name__)

# Setup Qdrant client
client = QdrantClient(host='192.168.4.43', port=6333, timeout=30)
COLLECTION_NAME = 'products_collection'

# Load and prepare models
doc_model_id = "naver/efficient-splade-VI-BT-large-doc"
doc_tokenizer = AutoTokenizer.from_pretrained(doc_model_id)
doc_model = AutoModelForMaskedLM.from_pretrained(doc_model_id)
query_model_id = "naver/efficient-splade-VI-BT-large-query"
query_tokenizer = AutoTokenizer.from_pretrained(query_model_id)
query_model = AutoModelForMaskedLM.from_pretrained(query_model_id)
device = 'cuda' if torch.cuda.is_available() else 'cpu'
dense_model = SentenceTransformer('sentence-transformers/clip-ViT-B-32', device=device)

# Read and prepare dataset
def factorize_article_type(metadata):
    return pd.factorize(metadata['articleType'])[0]

def prepare_dataset(metadata):
    metadata['articleType'] = metadata['category_name']
    metadata['productDisplayName'] = metadata['name']
    metadata['simplifiedDisplayName'] = metadata['simplified_name']
    metadata['articleType_label'] = factorize_article_type(metadata)
    dataset = metadata[[ 'id', 'articleType', 'productDisplayName', 'articleType_label', 'simplifiedDisplayName','image_url', 'price', 'category_name', 'shop_id', 'page_url']]
    dataset = dataset.rename(columns={"id": "Pid"})
    return dataset

metadata = pd.read_csv('metadata.csv') 
product_df = prepare_dataset(metadata)

def compute_vector(text, tokenizer, sparse_model):
    """
    Computes a vector from logits and attention mask using ReLU, log, and max operations.
    """
    tokens = tokenizer(text, return_tensors="pt")
    output = sparse_model(**tokens)
    logits, attention_mask = output.logits, tokens.attention_mask
    relu_log = torch.log(1 + torch.relu(logits))
    weighted_log = relu_log * attention_mask.unsqueeze(-1)
    max_val, _ = torch.max(weighted_log, dim=1)
    vec = max_val.squeeze()

    return vec, tokens

def return_results(query_indices,query_values,dense_vec):
  results  = client.search_batch(
        collection_name=COLLECTION_NAME,
        requests=[
            models.SearchRequest(
                vector=models.NamedVector(
                    name="text-dense",
                    vector=dense_vec.tolist(),
                ),
                limit=30,
            ),
            models.SearchRequest(
                vector=models.NamedSparseVector(
                    name="text-sparse",
                    vector=models.SparseVector(
                        indices=query_indices.tolist(),
                        values=query_values.tolist(),
                    ),
                ),
                limit=30,
            ),
        ],
    )
  return results


def get_product_details(dense_match, sparse_match):
    details = []
    product_ids = []
    
    # Helper function to extract required fields
    def extract_details(pid):
        row = product_df[product_df['Pid'] == pid].iloc[0]
        return {
            "product_id": int(row['Pid']),  # Convert numpy int64 to Python int
            "name": row['productDisplayName'],
            "price": float(row['price']),  # Ensure float or convert to str if necessary
            "category_name": row['category_name'],
            "shop_id": int(row['shop_id']),
            "page_url": row['page_url'],
            "image_url": row['image_url']
        }

    # Collect details for dense matches
    for item in dense_match:
        pid = product_df.iloc[item - 64335]['Pid']
        product_ids.append(pid)
        details.append(extract_details(pid))
    
    # Collect details for sparse matches
    for item in sparse_match:
        pid = product_df.iloc[item]['Pid']
        product_ids.append(pid)
        details.append(extract_details(pid))
    
    return details

def create_docss(details, scores):
    docs = []
    if not details:
        print("No product details provided.")
        return docs

    for detail, score in zip(details, scores):
        if not detail:
            print(f"Missing detail for a product, skipping document creation.")
            continue

        metadata = {
            'pid': str(detail.get('product_id', 'N/A')),
            'name': detail.get('name', 'N/A'),
            'category_name': detail.get('category_name', 'N/A'),
            'price': detail.get('price', 'N/A'),
            'shop_id': detail.get('shop_id', 'N/A'),
            'page_url': detail.get('page_url', 'N/A'),
            'image_url': detail.get('image_url', 'N/A'),
            'score': score 
        }

        doc = Document(page_content="") 
        doc.metadata = metadata  

        docs.append(doc)

    return docs

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('query') 
    print(query)
    if not query or not query.strip():
        return jsonify({"error": "Query parameter is missing or empty"}), 400

    try:
        dense_vec = dense_model.encode(query)
        query_vec, query_tokens = compute_vector(query, query_tokenizer, query_model)
        query_indices = query_vec.nonzero().numpy().flatten()
        query_values = query_vec.detach().numpy()[query_indices]

        results = return_results(query_indices, query_values, dense_vec)
        dense_match = [result.id for result in results[0]]
        sparse_match = [result.id for result in results[1]]
        scores = [result.score for result in results[0]]

        details = get_product_details(dense_match, sparse_match)
        docs = create_docss(details, scores)

        serialized_docs = [doc.metadata for doc in docs]

        return jsonify({"status": "success", "products": serialized_docs})
    except Exception as e:
        return jsonify({"error": f"An error occurred during processing: {str(e)}"}), 

if __name__ == '__main__':
    app.run(debug=True)
