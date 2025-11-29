import json
from kafka import KafkaConsumer 
from utils import get_embedding_function, initialize_chroma_collections, initialize_all_evidently_monitors, run_evidently_check
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS


# Initialize embedding function
embed_function = get_embedding_function() 

# Initialize all 3 ChromaDB collections
collections = initialize_chroma_collections(embed_function)

# Initialize all 3 Evidently baselines
baselines = initialize_all_evidently_monitors()

# Map Kafka topics to collection types
TOPIC_TO_COLLECTION = {
    KAFKA_TOPICS["customers"]: "customers",
    KAFKA_TOPICS["products"]: "products",
    KAFKA_TOPICS["purchases"]: "purchases"
}


def get_collection_type_from_topic(topic_name):
    """
    Determines which collection type (customers/products/purchases) 
    based on the Kafka topic name.
    """
    return TOPIC_TO_COLLECTION.get(topic_name)


def process_change_event(message_value):
    """
    Handles a single change event from Kafka and performs the corresponding 
    operation on the appropriate ChromaDB collection.
    """
    data = message_value
    op_type = data['operation_type']
    doc_id = data['id']
    topic_name = data['topic_name']
    
    # Determine which collection to use
    collection_type = get_collection_type_from_topic(topic_name)
    
    if not collection_type:
        print(f"⚠️ Unknown topic: {topic_name}. Skipping message.")
        return
    
    collection = collections.get(collection_type)
    baseline = baselines.get(collection_type)
    
    if not collection:
        print(f"❌ Collection for {collection_type} not initialized. Skipping message.")
        return

    # --- Step 1: Observability Check (Evidently) ---
    if baseline and data.get('full_document'):
        run_evidently_check(baseline, data['full_document'], collection_type)
    
    # --- Step 2 & 3: Embedding and ChromaDB Operations ---
    
    if op_type == 'insert' or op_type == 'update':
        
        full_doc = data['full_document']
        
        # Prepare content for embedding
        content_to_embed = json.dumps(full_doc) 
        
        if op_type == 'update':
            # Update operation: Delete old embedding first
            collection.delete(ids=[doc_id])
            print(f"[{collection_type}] Deleted old embedding for ID: {doc_id}")

        # Insert new or updated document
        collection.add(
            documents=[content_to_embed],
            metadatas=[{
                "source_topic": topic_name,
                "collection_type": collection_type
            }],
            ids=[doc_id]
        )
        print(f"[{collection_type}] Processed and indexed {op_type} for ID: {doc_id} in ChromaDB.")

    elif op_type == 'delete':
        # Delete operation: Remove embedding by ID
        collection.delete(ids=[doc_id])
        print(f"[{collection_type}] Removed embedding for ID: {doc_id} from ChromaDB.")


def start_consumer():
    """
    Subscribes to all MongoDB change topics and processes updates continuously.
    """
    # Get all topic values from KAFKA_TOPICS dict
    topics_list = list(KAFKA_TOPICS.values())
    
    consumer = KafkaConsumer(
        *topics_list,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='chroma_embedding_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        session_timeout_ms=300000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=900000, 
        max_poll_records=50, 
        fetch_max_wait_ms=500,
        fetch_min_bytes=1
    )
    
    print(f"Listening for updates on topics: {topics_list}")

    try:
        for message in consumer:
            # Add topic name to message value
            message.value['topic_name'] = message.topic 
            process_change_event(message.value)

    except KeyboardInterrupt:
        print("Consumer shut down.")
    finally:
        consumer.close()


if __name__ == "__main__":
    # Check if all collections are initialized
    if all(collections.values()):
        print("✅ All ChromaDB collections initialized successfully.")
        start_consumer()
    else:
        failed = [k for k, v in collections.items() if v is None]
        print(f"❌ Failed to initialize ChromaDB collections: {failed}. Cannot start consumer.")