# config.py
import os

# --- MongoDB Configuration ---
MONGO_URI = os.getenv('MONGO_URI', "")
MONGO_DB = "aura"
COLLECTION_NAMES = ["customers", "products", "purchases"]

# --- Kafka Configuration ---
# Use environment variable with fallback for different environments
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')

# --- Kafka Topic Definitions ---
KAFKA_TOPICS = {
    "customers": "mongodb_customers_updates",
    "products": "mongodb_products_updates", 
    "purchases": "mongodb_purchases_updates"
}

# --- Kafka Consumer Configuration ---
KAFKA_CONSUMER_GROUP_ID = "embedding_processor_group"

# --- Hugging Face Configuration ---
HUGGINGFACE_API_TOKEN = os.getenv('HUGGINGFACE_API_TOKEN', "")
HUGGINGFACE_MODEL = "sentence-transformers/all-mpnet-base-v2"

# --- ChromaDB Configuration ---
CHROMADB_PATH = os.getenv('CHROMADB_PATH', "./chroma_data")
CHROMADB_COLLECTIONS = {
    "customers": "customers_embeddings",
    "products": "products_embeddings",
    "purchases": "purchases_embeddings"
}

# --- Resume Token Storage ---
RESUME_TOKEN_COLLECTION = "resume_tokens"

# --- Retry Configuration ---
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds