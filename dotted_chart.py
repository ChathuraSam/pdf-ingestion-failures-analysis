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
with open('data.json', 'r') as file:
    data = json.load(file)

# Convert the data into a DataFrame
df = pd.DataFrame(data)

# Convert the 'Duration' field into seconds
df['Duration in Seconds'] = df['Duration'].apply(duration_to_seconds)

# Create a pivot table for the heatmap
# Group by 'Status' and aggregate the average duration
heatmap_data = df.pivot_table(index='Status', values='Duration in Seconds', aggfunc='median')

# Plot the heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(heatmap_data, cmap="coolwarm", annot=True, fmt=".2f", cbar_kws={'label': 'Average Duration (seconds)'})

# Customize the plot
plt.title('Heatmap of Average Duration by Status (Succeeded vs. Failed)')
plt.xlabel('Status')
plt.ylabel('Duration (seconds)')
plt.show()
