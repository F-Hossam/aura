import pandas as pd
from chromadb.utils import embedding_functions
from evidently import Report 
import chromadb
from evidently.presets import DataDriftPreset
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from config import CHROMADB_COLLECTIONS, HUGGINGFACE_MODEL, CHROMADB_PATH


def get_embedding_function():
    """
    Initializes and returns the embedding function using Hugging Face's Transformers.
    """
    print(f"Loading Hugging Face model: {HUGGINGFACE_MODEL}...")
    hf_embed_func = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=HUGGINGFACE_MODEL
    )
    return hf_embed_func


def initialize_chroma_collections(embed_func):
    """
    Initializes all three ChromaDB collections (customers, products, purchases).
    Returns a dictionary of collection objects.
    """
    CHROMA_CLIENT = chromadb.PersistentClient(path=CHROMADB_PATH)
    collections = {}
    
    for collection_type, collection_name in CHROMADB_COLLECTIONS.items():
        print(f"Initializing Chroma Collection: {collection_name}")
        try:
            collection = CHROMA_CLIENT.get_or_create_collection(
                name=collection_name,
                embedding_function=embed_func
            )
            collections[collection_type] = collection
            print(f"✅ Collection '{collection_name}' initialized successfully.")
        except Exception as e:
            print(f"❌ Error initializing ChromaDB collection '{collection_name}': {e}")
            collections[collection_type] = None
    
    return collections


def initialize_chroma_collection(embed_func, collection_type):
    """
    Initializes a specific ChromaDB collection by type (customers/products/purchases).
    """
    CHROMA_CLIENT = chromadb.PersistentClient(path=CHROMADB_PATH)
    collection_name = CHROMADB_COLLECTIONS.get(collection_type)
    
    if not collection_name:
        print(f"❌ Invalid collection type: {collection_type}")
        return None
    
    print(f"Initializing Chroma Collection: {collection_name}")
    try:
        collection = CHROMA_CLIENT.get_or_create_collection(
            name=collection_name,
            embedding_function=embed_func
        )
        print(f"✅ Collection '{collection_name}' initialized successfully.")
        return collection
    except Exception as e:
        print(f"❌ Error initializing ChromaDB collection '{collection_name}': {e}")
        return None


def initialize_evidently_monitor(collection_type, baseline_dir="observability/baseline_data"):
    """
    Loads Evidently baseline data for a specific collection type.
    Baseline files should be named: baseline_customers.csv, baseline_products.csv, baseline_purchases.csv
    """
    baseline_path = os.path.join(baseline_dir, f"baseline_{collection_type}.csv")
    
    if not os.path.exists(baseline_path):
        print(f"⚠️ Evidently baseline not found at {baseline_path}. Monitoring disabled for {collection_type}.")
        return None, None

    try:
        baseline_data = pd.read_csv(baseline_path)

        if baseline_data.empty:
            print(f"⚠️ Evidently baseline CSV for {collection_type} is empty. Monitoring disabled.")
            return None, None

        data_drift_report = Report(metrics=[DataDriftPreset()])
        print(f"✅ Evidently Monitor initialized for {collection_type}.")
        return baseline_data, data_drift_report

    except Exception as e:
        print(f"⚠️ Could not load Evidently baseline for {collection_type}: {e}")
        return None, None


def initialize_all_evidently_monitors(baseline_dir="observability/baseline_data"):
    """
    Initializes Evidently monitors for all three collection types.
    Returns a dictionary of baselines.
    """
    baselines = {}
    
    for collection_type in CHROMADB_COLLECTIONS.keys():
        baseline, report = initialize_evidently_monitor(collection_type, baseline_dir)
        baselines[collection_type] = baseline
    
    return baselines


def run_evidently_check(baseline, current_data, collection_type):
    """
    Runs a safe Evidently drift check on a single Kafka message for a specific collection type.
    """
    if baseline is None:
        return  # Evidently disabled → skip

    try:
        # Convert incoming record to DataFrame
        current_df = pd.DataFrame([current_data])

        report = Report(metrics=[DataDriftPreset()])
        report.run(reference_data=baseline, current_data=current_df)

        result = report.as_dict()

        dataset_drift = (
            result.get("metrics", [{}])[0]
                  .get("result", {})
                  .get("dataset_drift", False)
        )

        if dataset_drift:
            print(f"🚨 Evidently detected DATA DRIFT for {collection_type} data!")

        # Save report to HTML with collection type in filename
        os.makedirs("observability/data_reports", exist_ok=True)
        report.save_html(f"observability/data_reports/{collection_type}_drift_report.html")

    except Exception as e:
        print(f"⚠️ Evidently drift check failed for {collection_type}: {e}")