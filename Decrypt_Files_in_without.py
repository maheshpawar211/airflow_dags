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

    # Open encrypted file stream
    with source_blob.open("rb") as encrypted_stream, dest_blob.open("wb") as decrypted_stream:
        gpg_process = gpg.decrypt_file(encrypted_stream, output=decrypted_stream)

        if not gpg_process.ok:
            print(f"Decryption failed: {gpg_process.stderr}")
            return False

    print(f"Decrypted file uploaded to {GCS_DEST_BUCKET}/{dest_blob_name}")
    return True

# Example usage
source_file = "large_encrypted_file.pgp"
dest_file = "large_decrypted_file.txt"

decrypt_in_chunks(source_file, dest_file)
