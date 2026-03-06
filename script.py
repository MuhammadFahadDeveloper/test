from pymongo import MongoClient, UpdateOne
from faker import Faker
import random
import time

MONGO_URI = "mongodb+srv://fahaddeveloper09:krtyr7bijjvcVJEO@cluster0.23difxl.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)

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

def insert_and_update():
    start_time = time.time()
    operations = []

    for i in range(1, TOTAL_RECORDS + 1):
        user = generate_user()
        operations.append(UpdateOne({"email": user["email"]}, {"$set": user}, upsert=True))

        if i % BATCH_SIZE == 0:
            collection.bulk_write(operations)
            operations = []
            print(f"Processed {i} records...")

    if operations:
        collection.bulk_write(operations)

    end_time = time.time()
    print(f"Completed {TOTAL_RECORDS} upserts in {round(end_time - start_time, 2)} seconds")

if __name__ == "__main__":
    insert_and_update()

