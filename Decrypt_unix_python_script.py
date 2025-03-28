import subprocess

# Define variables
source_bucket = "source-bucket"
destination_bucket = "destination-bucket"
encrypted_file = "encrypted-file.pgp"
decrypted_file = "decrypted-file.tt"
passphrase = "your_passphrase_here"  # Use secure storage instead of hardcoding

# Construct command
cat_cmd = ["gsutil", "cat", f"gs://{source_bucket}/{encrypted_file}"]
gpg_cmd = ["gpg", "--batch", "--yes", "--passphrase", passphrase, "--decrypt"]
gsutil_cmd = ["gsutil", "-o", "GSUtil:prefer_api=xml", "cp", "-", f"gs://{destination_bucket}/{decrypted_file}"]

# Open processes
cat_process = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
gpg_process = subprocess.Popen(gpg_cmd, stdin=cat_process.stdout, stdout=subprocess.PIPE)
gsutil_process = subprocess.Popen(gsutil_cmd, stdin=gpg_process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# Wait for process to complete and check for errors
stdout, stderr = gsutil_process.communicate()

if gsutil_process.returncode == 0:
    print(f"Decryption successful. File stored at gs://{destination_bucket}/{decrypted_file}")
else:
    print("Decryption failed!", stderr.decode())
