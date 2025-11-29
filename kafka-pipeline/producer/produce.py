import json
import sys
import os
from pymongo.mongo_client import MongoClient
from kafka import KafkaProducer 

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from config import MONGO_URI, MONGO_DB, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, COLLECTION_NAMES

# --- Setup Connections (External Detail) ---
MONGO_CLIENT = MongoClient(MONGO_URI)
DB = MONGO_CLIENT[MONGO_DB]
KAFKA_PRODUCER = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def handle_change(change, topic):
    """
    Processes a single change stream event and sends a structured payload to Kafka.
    """
    op_type = change['operationType']
    document_id = str(change['documentKey']['_id']) 
    
    payload = {
        'id': document_id,
        'operation_type': op_type,
        'full_document': None,
    }

    # Extract the necessary data based on the operation type
    if op_type == 'insert':
        payload['full_document'] = change['fullDocument']
    
    elif op_type == 'update':
        payload['full_document'] = change.get('fullDocument')

    elif op_type == 'delete':
        pass
    
    else:
        # Ignore other operations (e.g., replace, invalidate)
        return

    KAFKA_PRODUCER.send(topic, value=payload)
    print(f"Sent {op_type} for ID {document_id} to {topic}")


def start_change_stream_listener(collection_name):
    """
    Sets up and continuously listens to the MongoDB Change Stream for a single collection.
    This ensures continuous data collection and maintenance, a key tenet of LLMOps [1].
    """
    collection = DB[collection_name]
    topic = KAFKA_TOPICS[collection_name]
    
    # Listen only to insert, update, and delete operations
    pipeline = [
        {
            '$match': {
                'operationType': {'$in': ['insert', 'update', 'delete']}
            }
        }
    ]
    
    try:
        print(f"Starting change stream for collection: {collection_name}")
        # Start watching for changes
        with collection.watch(pipeline=pipeline, full_document='updateLookup') as stream:
            for change in stream:
                handle_change(change, topic)
    except Exception as e:
        print(f"Error in change stream for {collection_name}: {e}")
        # In a production environment, implement retry logic here

def main():
    """
    Launches listeners for all defined collections (customers, products, purchases).
    """
    # Use threading/multiprocessing (external detail) to run multiple listeners concurrently
    import threading

    threads = []
    for collection_name in COLLECTION_NAMES:
        thread = threading.Thread(target=start_change_stream_listener, args=(collection_name,))
        threads.append(thread)
        thread.start()

    # Keep the main thread alive to monitor the listeners
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()