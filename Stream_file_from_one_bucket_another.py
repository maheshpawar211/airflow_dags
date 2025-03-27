from google.cloud import storage
from io import BytesIO

def stream_copy_gcs_file(bucket_name, source_path, destination_path):
    """Streams data from one GCS file to another without loading into memory."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Read source file as a stream
    source_blob = bucket.blob(source_path)
    source_stream = BytesIO()
    source_blob.download_to_file(source_stream)
    source_stream.seek(0)  # Reset stream pointer
    
    # Prepare destination blob and write stream to it
    destination_blob = bucket.blob(destination_path)
    destination_stream = BytesIO()
    
    # Streaming data from source to destination
    chunk_size = 1024 * 1024  # 1MB chunks (adjust as needed)
    while chunk := source_stream.read(chunk_size):
        destination_stream.write(chunk)
    
    # Upload the written content to GCS
    destination_stream.seek(0)
    destination_blob.upload_from_file(destination_stream, content_type=source_blob.content_type)

    print(f"Successfully copied from '{source_path}' to '{destination_path}' in bucket '{bucket_name}'.")

# Example usage
bucket_name = "your-bucket-name"
source_path = "path/to/source_file.txt"
destination_path = "path/to/destination_file.txt"

stream_copy_gcs_file(bucket_name, source_path, destination_path)
