import json
import subprocess

def read_file(file_name):
    blocks = []
    current_block = {}
    attributes = ['Name', 'Status', 'Start Time', 'End Time', 'Duration']
    
    with open(file_name, 'r') as file:
        i = 0
        for line in file:
            stripped_line = line.strip()
            if stripped_line == '-' and current_block:
                blocks.append(current_block)
                current_block = {}
                i = 0
            elif stripped_line != '-':
                current_block[attributes[i]] = stripped_line
                i += 1
        if current_block:
            blocks.append(current_block)
    return blocks

def format_raw_data(raw_data):
    print(raw_data)
    
def write_file(file_name, data):
    with open(file_name, 'w') as file:
        json.dump(data, file, indent=4)
        
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
    
# write a function to filter the failed items only and store them in a text file
def filter_failed_items(data):
    failed_items = [item for item in data if item['Status'] == 'Failed']
    with open('failed_items.txt', 'w') as file:
        for item in failed_items:
            file.write(f"{item['Name']}\n")

def main():
    # stage = 'prod'
    # bucket_name = f'etext-{stage}-ereader-ingest'
    # sub_folder = 'incoming/epub/wiley.com'
    
    raw_data = read_file('raw_data.txt')
    write_file('data.json', raw_data)
    # print(raw_data)
        
    
    # Create a new list to store the filtered data
    filtered_data = raw_data
    
    # Iterate over the raw data and filter out the items
    for item1 in raw_data:
        item1_isbn = item1['Name'].split('-')[0]
        item1_uuid = f"{item1['Name'].split('-')[0]}-{item1['Name'].split('-')[1]}-{item1['Name'].split('-')[2]}-{item1['Name'].split('-')[3]}-{item1['Name'].split('-')[4]}-{item1['Name'].split('-')[5]}"
        if item1['Status'] == 'Succeeded':
            is_manual_ingestion = False
            failed_count = 0
            for item2 in raw_data:
                item2_isbn = item2['Name'].split('-')[0]
                item2_uuid = f"{item1['Name'].split('-')[0]}-{item2['Name'].split('-')[1]}-{item2['Name'].split('-')[2]}-{item2['Name'].split('-')[3]}-{item2['Name'].split('-')[4]}-{item2['Name'].split('-')[5]}"
                if item2_isbn == item1_isbn and item2['Status'] == 'Failed':
                    failed_count += 1
                if failed_count >= 2:
                    is_manual_ingestion = True
                    break
            if is_manual_ingestion:
                filtered_data.remove(item1)
    
    # Write the filtered data to the JSON file
    write_file('filtered_data.json', filtered_data)
    
    
    # read the data.json file and get the keys
    
    
    
    # file_sizes = []
    
    # with open('data.json', 'r') as file:
    #     data = json.load(file)
    #     names = [item['Name'] for item in data if 'Name' in item]
        
    # filter_failed_items(data)
    
    # for name in names:
    #     key = f'{sub_folder}/{name.split('-')[0]}.pdf'
    #     file_size = get_file_size(bucket_name, key)
    #     if file_size is not None:
    #         print(f"File size of {key}: {file_size} MB")
    #         file_sizes.append(file_size)
    #     else:
    #         print(f"File {key} not found or could not retrieve file size.")
    
    # for obj, size in zip(data, file_sizes):
    #     obj['size'] = size
        
    # with open('data.json', 'w') as file:
    #     json.dump(data, file, indent=4)
    
main()