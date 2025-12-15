from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

user = os.getenv("MONGODB_CLUSTER_USER")
password = os.getenv("MONGODB_CLUSTER_PASSWORD")
cluster = os.getenv("MONGODB_CLUSTER1")

def ensure_collection_exists(database, collection_name):
    if collection_name in database.list_collection_names():
        print(f"La colección '{collection_name}' ya existe.")
        return database[collection_name]
    else:
        print(f"Creando la colección '{collection_name}'...")
        return database.create_collection(collection_name)
    
# Conexión con mongoDB:
def get_database(user, password, cluster, database):
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = f'mongodb+srv://{user}:{password}@{cluster}.mongodb.net/{database}'
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client[database]