import json
from pathlib import Path
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from config import MONGO_URI, CHROMADB_PATH, MONGO_DB, CHROMADB_COLLECTIONS
from consumer.utils import get_embedding_function, initialize_chroma_collections
import time

COLLECTION_NAMES = ["customers", "products", "purchases"] 


def create_searchable_text(doc, collection_type):
    """
    Converts MongoDB document to human-readable text for better embeddings.
    Different format for each collection type based on actual data structure.
    """
    if collection_type == "customers":
        address = doc.get('address', {})
        return f"""
Customer: {doc.get('name', 'N/A')}
Email: {doc.get('email', 'N/A')}
Phone: {doc.get('phone', 'N/A')}
Location: {address.get('city', 'N/A')}, {address.get('street', 'N/A')}
Postal Code: {address.get('postal_code', 'N/A')}
Last Purchase: {doc.get('last_purchase', 'N/A')}
Last Engaged: {doc.get('last_engaged', 'N/A')}
Member Since: {doc.get('created_date', 'N/A')}
        """.strip()
    
    elif collection_type == "products":
        # Products structure: name, description, price, category, stock, created_date, updated_date
        return f"""
Product: {doc.get('name', 'N/A')}
Description: {doc.get('description', 'N/A')}
Category: {doc.get('category', 'N/A')}
Price: ${doc.get('price', 'N/A')}
Stock Available: {doc.get('stock', 'N/A')} units
Created: {doc.get('created_date', 'N/A')}
Last Updated: {doc.get('updated_date', 'N/A')}
        """.strip()
    
    elif collection_type == "purchases":
        # Purchases structure: customer_id, items array, delivery_info, total_price, purchase_date, status
        items = doc.get('items', [])
        item_count = len(items)
        
        # Get delivery info
        delivery_info = doc.get('delivery_info', {})
        delivery_address = delivery_info.get('address', {})
        delivery_city = delivery_address.get('city', 'N/A')
        delivery_date = delivery_info.get('delivery_date', 'N/A')
        
        # Build items description
        items_text = []
        for item in items[:3]:  # Show first 3 items
            product_id = item.get('product_id', 'Unknown')
            quantity = item.get('quantity', 0)
            unit_price = item.get('unit_price', 0)
            items_text.append(f"Product {product_id} (Qty: {quantity}, Price: ${unit_price})")
        
        items_summary = '; '.join(items_text)
        if item_count > 3:
            items_summary += f" and {item_count - 3} more items"
        
        return f"""
Purchase Date: {doc.get('purchase_date', 'N/A')}
Customer ID: {doc.get('customer_id', 'N/A')}
Total Price: ${doc.get('total_price', 'N/A')}
Status: {doc.get('status', 'N/A')}
Items ({item_count}): {items_summary}
Delivery City: {delivery_city}
Delivery Date: {delivery_date}
        """.strip()
    
    else:
        # Fallback to JSON for unknown types
        return json.dumps(doc)


def extract_metadata(doc, collection_type):
    """
    Extracts key metadata fields for filtering and display.
    Based on actual MongoDB data structure.
    ChromaDB only accepts: bool, int, float, str (no None values allowed).
    """
    metadata = {
        "source_collection": collection_type,
        "collection_type": collection_type,
        "initial_load": True,
        "raw_json": json.dumps(doc)  # Keep original data for reference
    }
    
    if collection_type == "customers":
        # Only add fields that have non-None values
        if doc.get('name'):
            metadata["name"] = str(doc.get('name'))
        if doc.get('email'):
            metadata["email"] = str(doc.get('email'))
        if doc.get('phone'):
            metadata["phone"] = str(doc.get('phone'))
        
        city = doc.get('address', {}).get('city')
        if city:
            metadata["city"] = str(city)
        
        if doc.get('last_purchase'):
            metadata["last_purchase"] = str(doc.get('last_purchase'))
    
    elif collection_type == "products":
        # Products: name, description, price, category, stock, created_date, updated_date
        if doc.get('name'):
            metadata["name"] = str(doc.get('name'))
        if doc.get('description'):
            metadata["description"] = str(doc.get('description'))
        if doc.get('category'):
            metadata["category"] = str(doc.get('category'))
        if doc.get('price') is not None:
            metadata["price"] = float(doc.get('price'))
        if doc.get('stock') is not None:
            metadata["stock"] = int(doc.get('stock'))
        if doc.get('created_date'):
            metadata["created_date"] = str(doc.get('created_date'))
        if doc.get('updated_date'):
            metadata["updated_date"] = str(doc.get('updated_date'))
    
    elif collection_type == "purchases":
        # Purchases: customer_id, items, delivery_info, total_price, purchase_date, status
        if doc.get('customer_id'):
            metadata["customer_id"] = str(doc.get('customer_id'))
        if doc.get('total_price') is not None:
            metadata["total_price"] = float(doc.get('total_price'))
        if doc.get('purchase_date'):
            metadata["purchase_date"] = str(doc.get('purchase_date'))
        if doc.get('status'):
            metadata["status"] = str(doc.get('status'))
        
        # Handle nested delivery_info safely
        delivery_info = doc.get('delivery_info', {})
        if delivery_info:
            delivery_address = delivery_info.get('address', {})
            if delivery_address and delivery_address.get('city'):
                metadata["delivery_city"] = str(delivery_address.get('city'))
            if delivery_info.get('delivery_date'):
                metadata["delivery_date"] = str(delivery_info.get('delivery_date'))
        
        # Add item count
        items = doc.get('items', [])
        if items:
            metadata["item_count"] = int(len(items))
    
    return metadata


def check_chroma_persistence():
    """
    Checks if ChromaDB data already exists to avoid duplicate initial loads.
    """
    chroma_path = Path(CHROMADB_PATH)
    chroma_path.mkdir(parents=True, exist_ok=True)
    
    has_chroma_data = any(
        p.name in ("chroma.sqlite3", "index", "index.sqlite") 
        for p in chroma_path.iterdir()
    )
    
    return has_chroma_data


def run_initial_load():
    """
    Performs the initial bulk ingestion of historical MongoDB records into ChromaDB.
    Each MongoDB collection is loaded into its corresponding ChromaDB collection.
    Now creates human-readable embeddings for better semantic search.
    """
    # 1. Persistence Check
    if check_chroma_persistence():
        print("ChromaDB persistence detected. Skipping initial load.")
        return

    print("ChromaDB is empty. Starting initial load with searchable embeddings...")

    # --- Setup Connections and Embeddings ---
    
    try:
        # Initialize the embedding function (Hugging Face model)
        embed_function = get_embedding_function() 
        
        # Initialize all 3 ChromaDB collections
        chroma_collections = initialize_chroma_collections(embed_function)
        
        # Check if all collections initialized successfully
        if not all(chroma_collections.values()):
            failed = [k for k, v in chroma_collections.items() if v is None]
            print(f"❌ Failed to initialize collections: {failed}. Aborting initial load.")
            return
        
        # Connect to MongoDB
        mongo_client = MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=False, server_api=ServerApi('1'))
        db = mongo_client[MONGO_DB]
        
    except Exception as e:
        print(f"Error during setup: {e}")
        return

    # --- 2. Iterate and Load Collections ---
    
    total_documents_loaded = 0
    
    for collection_name in COLLECTION_NAMES:
        print(f"\n{'='*60}")
        print(f"Processing MongoDB collection: {collection_name}")
        print(f"{'='*60}")
        
        # Get the corresponding ChromaDB collection
        chroma_collection = chroma_collections.get(collection_name)
        
        if not chroma_collection:
            print(f"⚠️ No ChromaDB collection found for {collection_name}. Skipping.")
            continue
        
        # Fetch all documents from MongoDB
        mongo_collection = db[collection_name]
        all_documents = list(mongo_collection.find({}))
        
        if not all_documents:
            print(f"No records found in {collection_name}. Skipping.")
            continue

        # Prepare data for ChromaDB batch insertion
        documents_to_embed = []
        ids = []
        metadatas = []
        
        for doc in all_documents:
            doc_id = str(doc.pop('_id'))  # Extract and remove _id from document
            
            # NEW: Create human-readable text for better embeddings
            searchable_text = create_searchable_text(doc, collection_name)
            
            # NEW: Extract structured metadata for filtering
            metadata = extract_metadata(doc, collection_name)
            
            ids.append(doc_id)
            documents_to_embed.append(searchable_text)
            metadatas.append(metadata)
        
        # --- 3. Store in ChromaDB ---  
        
        try:
            chroma_collection.add(
                documents=documents_to_embed,
                metadatas=metadatas,
                ids=ids
            )
            
            count = len(all_documents)
            total_documents_loaded += count
            print(f"✅ Successfully loaded {count} documents from {collection_name} into {CHROMADB_COLLECTIONS[collection_name]}")
            print(f"   Documents embedded as searchable text with structured metadata")
            
        except Exception as e:
            print(f"❌ Error loading {collection_name}: {e}")
            continue

    print(f"\n{'='*60}")
    print(f"Initial Load Complete")
    print(f"Total documents indexed: {total_documents_loaded}")
    print(f"✨ All documents now have searchable embeddings!")
    print(f"{'='*60}")
    
    mongo_client.close()


if __name__ == "__main__":
    print("Waiting 10 seconds before starting initial load...")
    time.sleep(10) 
    run_initial_load()