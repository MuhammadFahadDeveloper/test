import json
import os
import requests
import gzip
from pymongo import MongoClient, UpdateOne

# File paths
compressed_file = 'ol_dump_authors_latest.txt.gz'
decompressed_file = 'ol_dump_authors_latest.txt'


# Step 1: Download the dataset
def download_file():
    url = 'https://openlibrary.org/data/ol_dump_authors_latest.txt.gz'
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(compressed_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded to {compressed_file}")
    else:
        raise Exception(f"Download failed: status code {response.status_code}")


# Step 2: Decompress the file
def decompress_file():
    print(f"Decompressing {compressed_file}...")
    with gzip.open(compressed_file, 'rb') as f_in, open(decompressed_file, 'wb') as f_out:
        for data in iter(lambda: f_in.read(100 * 1024), b''):
            f_out.write(data)
    print(f"Decompressed to {decompressed_file}")


# Step 3: Load exactly 10 million JSON rows into MongoDB
def load_to_mongodb():
    # Connect to MongoDB (update URI if needed)
    client = MongoClient('mongodb://localhost:27017/')
    db = client['openlibrary_db']
    collection = db['authors']

    collection.create_index('_id')

    batch_size = 1000
    batch = []
    max_rows = 10_000_000
    row_count = 0

    print(f"Loading data from {decompressed_file} into MongoDB...")
    with open(decompressed_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            if row_count >= max_rows:
                print(f"Reached {max_rows} rows, stopping.")
                break

            try:
                parts = line.strip().split('\t')
                if len(parts) != 5:
                    print(f"Skipping invalid line {line_num}")
                    continue

                json_str = parts[4]
                doc = json.loads(json_str)

                if 'key' in doc:
                    doc['_id'] = doc['key']
                else:
                    print(f"Skipping record without 'key' at line {line_num}")
                    continue

                batch.append(
                    UpdateOne(
                        {'_id': doc['_id']},
                        {'$set': doc},
                        upsert=True
                    )
                )
                row_count += 1

                if len(batch) == batch_size:
                    collection.bulk_write(batch)
                    print(f"Processed {row_count} records")
                    batch = []

            except json.JSONDecodeError:
                print(f"JSON decode error at line {line_num}")
            except Exception as e:
                print(f"Error at line {line_num}: {e}")

    if batch:
        collection.bulk_write(batch)
        print(f"Processed {row_count} records")

    print(f"Loading complete! Total rows processed: {row_count}")

    print("Cleaning up files...")
    if os.path.exists(compressed_file):
        os.remove(compressed_file)
    if os.path.exists(decompressed_file):
        os.remove(decompressed_file)
    print("Cleanup complete!")


if __name__ == "__main__":
    try:
        if not os.path.exists(compressed_file):
            download_file()
        if not os.path.exists(decompressed_file):
            decompress_file()
        load_to_mongodb()
    except Exception as e:
        print(f"Error: {e}")