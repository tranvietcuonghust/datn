from pymilvus import connections, exceptions,utility

try:
    # Connect to Milvus
    connections.connect("default", host="milvus-standalone", port="19530")

    # Interact with Milvus
    if utility.has_collection("property_details_minilm_daily_ban"):
        # Your code here
        pass
except exceptions.ConnectionNotExistException:
    print("Error: Connection to Milvus server could not be established. Please check if the server is running and accessible.")