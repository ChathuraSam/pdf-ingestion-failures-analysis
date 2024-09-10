import subprocess

stage = 'prod'
bucket_name = f'etext-{stage}-ereader-ingest'
sub_folder = 'incoming/epub/wiley.com'

with open('keys.txt', 'r') as file:
    keys = [f'{sub_folder}/{line.strip()}.pdf' for line in file]


def get_file_size(bucket, key):
    try:
        result = subprocess.run(
            ['aws', 's3api', 'head-object', '--bucket', bucket, '--key', key, '--query', 'ContentLength', '--output', 'text'],
            capture_output=True,
            text=True,
            check=True
        )
        file_size_bytes = int(result.stdout.strip())
        file_size_mb = file_size_bytes / (1024 * 1024)  # Convert bytes to megabytes
        # round off the file size to 2 decimal places
        file_size_mb = round(file_size_mb, 1)
        return file_size_mb
    except subprocess.CalledProcessError as e:
        if e.returncode == 255:
            print(f"File {key} not found (404).")
        else:
            print(f"Error retrieving {key}: {e}")
        return None

for key in keys:
    file_size = get_file_size(bucket_name, key)
    if file_size is not None:
        print(f"File size of {key}: {file_size} MB")
    else:
        print(f"File {key} not found or could not retrieve file size.")

# store the file size in a text file
with open('file_sizes.txt', 'w') as file:
    for key in keys:
        file_size = get_file_size(bucket_name, key)
        if file_size is not None:
            file.write(f"{file_size}\n")
        else:
            file.write(f"{key}: Not found\n")