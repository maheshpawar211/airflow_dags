import os
import gnupg
from google.cloud import storage

# GCS Configuration
GCS_SOURCE_BUCKET = "source-bucket-name"
GCS_DEST_BUCKET = "destination-bucket-name"
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB

# Initialize GCS Client
storage_client = storage.Client()

# Initialize GnuPG
gpg = gnupg.GPG()

def decrypt_in_chunks(source_blob_name, dest_blob_name):
    """
    Reads an encrypted file from GCS in chunks, decrypts each chunk, and writes decrypted data to another GCS bucket.
    """
    source_bucket = storage_client.bucket(GCS_SOURCE_BUCKET)
    dest_bucket = storage_client.bucket(GCS_DEST_BUCKET)

    source_blob = source_bucket.blob(source_blob_name)
    dest_blob = dest_bucket.blob(dest_blob_name)

    # Open the source encrypted file
    with source_blob.open("rb") as encrypted_stream, dest_blob.open("wb") as decrypted_stream:
        while True:
            chunk = encrypted_stream.read(CHUNK_SIZE)  # Read a chunk of encrypted data
            if not chunk:
                break  # Stop when no more data

            # Decrypt the chunk
            decrypted_chunk = gpg.decrypt(chunk)

            if not decrypted_chunk.ok:
                print(f"Decryption failed: {decrypted_chunk.stderr}")
                return False

            # Write decrypted chunk to destination file
            decrypted_stream.write(decrypted_chunk.data)

    print(f"Decrypted file uploaded to {GCS_DEST_BUCKET}/{dest_blob_name}")
    return True

# Example usage
source_file = "large_encrypted_file.pgp"
dest_file = "large_decrypted_file.txt"

decrypt_in_chunks(source_file, dest_file)
