import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Helper function to convert duration to seconds
def duration_to_seconds(duration):
    h, m, s = duration.split(':')
    s, ms = s.split('.')
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000

# Load the JSON data from the file
with open('filtered_data.json', 'r') as file:
    data = json.load(file)

# Convert the data into a DataFrame
df = pd.DataFrame(data)

# Convert the 'Duration' field into seconds
df['Duration in Seconds'] = df['Duration'].apply(duration_to_seconds)

# Plot failed statuses in red
failed = df[df['Status'] == 'Failed']
plt.scatter(failed['Duration in Seconds'], failed['Status'], color='red', label='Failed')

# Plot succeeded statuses in green
succeeded = df[df['Status'] == 'Succeeded']
plt.scatter(succeeded['Duration in Seconds'], succeeded['Status'], color='green', label='Succeeded')

# Customize the plot
plt.title('Scatter Plot of Duration vs Status (Succeeded vs Failed)')
plt.xlabel('Duration (seconds)')
plt.ylabel('Status')
plt.legend()

# Show the plot
plt.show()
