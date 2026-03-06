from pymongo import MongoClient, UpdateOne
from faker import Faker
import random
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("mongo_operations.log"),
        logging.StreamHandler()
    ]
)

# MongoDB connection
MONGO_URI = "mongodb+srv://fahaddeveloper09:krtyr7bijjvcVJEO@cluster0.23difxl.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)

# Database and Collection
db = client["poc_db"]
collection = db["users"]

fake = Faker()

TOTAL_RECORDS = 1_000_000
BATCH_SIZE = 5000

def generate_user():
    return {
        "name": fake.name(),
        "email": fake.unique.email(),
        "address": fake.address(),
        "age": random.randint(18, 70),
        "created_at": fake.date_time_this_decade().isoformat()
    }

def insert_records():
    start_time = time.time()
    batch = []

    for i in range(1, TOTAL_RECORDS + 1):
        batch.append(generate_user())

        if i % BATCH_SIZE == 0:
            collection.insert_many(batch)
            batch = []
            logging.info(f"Inserted {i} records so far...")

    if batch:
        collection.insert_many(batch)
        logging.info(f"Inserted {TOTAL_RECORDS} records in total.")

    end_time = time.time()
    logging.info(f"✅ Completed insertion of {TOTAL_RECORDS} records in {round(end_time - start_time, 2)} seconds")

def update_records():
    start_time = time.time()
    operations = []
    total_updated = 0

    for doc in collection.find({}, {"_id": 1}):
        operations.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"status": "active"}}))

        if len(operations) == BATCH_SIZE:
            collection.bulk_write(operations)
            total_updated += len(operations)
            logging.info(f"Updated {total_updated} records so far...")
            operations = []

    if operations:
        collection.bulk_write(operations)
        total_updated += len(operations)
        logging.info(f"Updated {total_updated} records in total.")

    end_time = time.time()
    logging.info(f"✅ Completed updating {total_updated} records in {round(end_time - start_time, 2)} seconds")

if __name__ == "__main__":
    insert_records()
    update_records()
