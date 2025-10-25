import json
from google.cloud import storage

def load_schema_from_gcs(schema_path: str):
    """Load BigQuery schema from a JSON file stored in GCS."""
    if schema_path.startswith("gs://"):
        client = storage.Client()
        bucket_name, blob_path = schema_path[5:].split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        schema_data = blob.download_as_text()
        return json.loads(schema_data)
    else:
        with open(schema_path, "r") as f:
            return json.load(f)
