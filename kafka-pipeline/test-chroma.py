import chromadb
import json

# Connect to ChromaDB
db_path = "./chroma_data"
client = chromadb.PersistentClient(path=db_path)

# Get collections
customers_collection = client.get_collection(name="customers_embeddings")
products_collection = client.get_collection(name="products_embeddings")
purchases_collection = client.get_collection(name="purchases_embeddings")

print(f"Connected to ChromaDB at: {db_path}")
print(f"Customers: {customers_collection.count()}")
print(f"Products: {products_collection.count()}")
print(f"Purchases: {purchases_collection.count()}")


def display_results(results, collection_type, show_distances=False):
    """
    Display search results with metadata and optional distances.
    Documents are now human-readable text, metadata contains structured data.
    """
    if not results['ids'][0]:
        print("No results found.")
        return
    
    for i, (doc_id, doc_text, metadata) in enumerate(zip(
        results['ids'][0], 
        results['documents'][0],
        results['metadatas'][0]
    )):
        print(f"\n{'='*60}")
        print(f"Result {i+1}")
        if show_distances and 'distances' in results:
            print(f"Similarity Distance: {results['distances'][0][i]:.4f}")
        print(f"{'='*60}")
        print(f"ID: {doc_id}")
        
        # Display based on collection type
        if collection_type == "customers":
            print(f"Name: {metadata.get('name', 'N/A')}")
            print(f"Email: {metadata.get('email', 'N/A')}")
            print(f"Phone: {metadata.get('phone', 'N/A')}")
            print(f"City: {metadata.get('city', 'N/A')}")
            print(f"Last Purchase: {metadata.get('last_purchase', 'N/A')}")
        
        elif collection_type == "products":
            print(f"Name: {metadata.get('name', 'N/A')}")
            print(f"Category: {metadata.get('category', 'N/A')}")
            print(f"Price: ${metadata.get('price', 'N/A')}")
            print(f"Stock: {metadata.get('stock', 'N/A')} units")
            print(f"Description: {metadata.get('description', 'N/A')[:80]}...")
        
        elif collection_type == "purchases":
            print(f"Customer ID: {metadata.get('customer_id', 'N/A')}")
            print(f"Total Price: ${metadata.get('total_price', 'N/A')}")
            print(f"Purchase Date: {metadata.get('purchase_date', 'N/A')}")
            print(f"Status: {metadata.get('status', 'N/A')}")
            print(f"Delivery City: {metadata.get('delivery_city', 'N/A')}")
            print(f"Items: {metadata.get('item_count', 'N/A')}")
        
        # Show searchable text (first 200 chars)
        print(f"\nSearchable Text Preview:")
        print(f"{doc_text[:200]}...")
        
        # Optionally show full raw JSON
        if 'raw_json' in metadata:
            raw_data = json.loads(metadata['raw_json'])
            print(f"\nOriginal Data Available: Yes ({len(str(raw_data))} chars)")


# ========== TEST QUERIES ==========

print("\n\n" + "="*60)
print("TESTING CUSTOMERS COLLECTION")
print("="*60)

# Test 1: Search by location
print("\n🏙️ Query: 'customers in Rabat'")
results = customers_collection.query(
    query_texts=["customers in Rabat"],
    n_results=3,
    include=["documents", "metadatas", "distances"]
)
display_results(results, "customers", show_distances=True)

# Test 2: Search by name
print("\n\n👤 Query: 'Youssef El Amrani'")
results = customers_collection.query(
    query_texts=["Youssef El Amrani"],
    n_results=2,
    include=["documents", "metadatas", "distances"]
)
display_results(results, "customers", show_distances=True)

# Test 3: Filter by city metadata
print("\n\n🎯 Filter: customers in 'Marrakech'")
results = customers_collection.get(
    where={"city": "Marrakech"},
    include=["documents", "metadatas"],
    limit=5
)
# Convert get() results to query() format
results_formatted = {
    'ids': [results['ids']],
    'documents': [results['documents']],
    'metadatas': [results['metadatas']]
}
display_results(results_formatted, "customers")


print("\n\n" + "="*60)
print("TESTING PRODUCTS COLLECTION")
print("="*60)

# Test 4: Search for electronics
print("\n📱 Query: 'smartphone electronics'")
results = products_collection.query(
    query_texts=["smartphone electronics"],
    n_results=3,
    include=["documents", "metadatas", "distances"]
)
display_results(results, "products", show_distances=True)

# Test 5: Filter by category
print("\n\n🎯 Filter: products in 'Electronics' category")
results = products_collection.get(
    where={"category": "Electronics"},
    include=["documents", "metadatas"],
    limit=5
)
results_formatted = {
    'ids': [results['ids']],
    'documents': [results['documents']],
    'metadatas': [results['metadatas']]
}
display_results(results_formatted, "products")


print("\n\n" + "="*60)
print("TESTING PURCHASES COLLECTION")
print("="*60)

# Test 6: Search for completed orders
print("\n✅ Query: 'completed purchases'")
results = purchases_collection.query(
    query_texts=["completed purchases"],
    n_results=3,
    include=["documents", "metadatas", "distances"]
)
display_results(results, "purchases", show_distances=True)

# Test 7: Filter by status
print("\n\n🎯 Filter: purchases with status 'completed'")
results = purchases_collection.get(
    where={"status": "completed"},
    include=["documents", "metadatas"],
    limit=5
)
results_formatted = {
    'ids': [results['ids']],
    'documents': [results['documents']],
    'metadatas': [results['metadatas']]
}
display_results(results_formatted, "purchases")

# Test 8: Search by delivery location
print("\n\n🚚 Query: 'orders delivered to Rabat'")
results = purchases_collection.query(
    query_texts=["orders delivered to Rabat"],
    n_results=3,
    include=["documents", "metadatas", "distances"]
)
display_results(results, "purchases", show_distances=True)


print("\n\n" + "="*60)
print("✅ ALL TESTS COMPLETE")
print("="*60)