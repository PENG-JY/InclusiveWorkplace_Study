import json
import pandas as pd
import os
from tqdm import tqdm  # For progress bar

# Directory containing the JSON files
directory_path = './companyfacts/'

# Function to extract useful financial data from a single JSON file
def extract_financial_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    facts = data.get("facts", {})
    extracted_data = []
    
    # Loop through categories and metrics to extract values
    for category, items in facts.items():
        for metric, details in items.items():
            if "units" in details:
                for unit, records in details["units"].items():
                    for record in records:
                        extracted_data.append({
                            "FileName": os.path.basename(file_path),  # Add file name for reference
                            "Category": category,
                            "Metric": metric,
                            "Unit": unit,
                            "Value": record.get("val"),
                            "EndDate": record.get("end"),
                            "FiscalYear": record.get("fy"),
                            "FiscalPeriod": record.get("fp"),
                            "FormType": record.get("form"),
                        })
    return extracted_data

# Initialize an empty list to store data from all files
all_data = []

# Get the list of files to process
json_files = [f for f in os.listdir(directory_path) if f.startswith("CIK000") and f.endswith(".json")]

# Limit to the first 1000 files
json_files = json_files[:1000]

# Define batch processing parameters
batch_size = 1000  # Save intermediate results every 1000 files
batch_counter = 0

# Process JSON files with a progress bar
print(f"Processing the first {len(json_files)} JSON files...")
for i, file_name in enumerate(tqdm(json_files, desc="Processing Files")):
    file_path = os.path.join(directory_path, file_name)
    try:
        # Extract financial data and append it to the main list
        file_data = extract_financial_data(file_path)
        all_data.extend(file_data)
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
    
    # Save intermediate results to avoid excessive memory usage
    if (i + 1) % batch_size == 0 or (i + 1) == len(json_files):  # Final batch
        batch_counter += 1
        temp_df = pd.DataFrame(all_data)
        temp_output_path = f'./financial_data_batch_{batch_counter}.csv'
        temp_df.to_csv(temp_output_path, index=False)
        print(f"Batch {batch_counter} saved to: {temp_output_path}")
        all_data.clear()  # Clear processed data to free memory

# Combine all batch files into a single CSV
combined_data = []
for batch_file in [f for f in os.listdir('.') if f.startswith("financial_data_batch_") and f.endswith(".csv")]:
    batch_df = pd.read_csv(batch_file)
    combined_data.append(batch_df)

final_df = pd.concat(combined_data, ignore_index=True)

# Filter specific metrics if needed
selected_metrics = ["EntityPublicFloat", "AccountsPayableTradeCurrent", "AccountsReceivableNetCurrent"]
filtered_df = final_df[final_df["Metric"].isin(selected_metrics)]

# Save the final combined data to a CSV file
output_path = './combined_financial_data.csv'
filtered_df.to_csv(output_path, index=False)

# Print a preview of the DataFrame
print(f"\nProcessing completed! Combined data saved to: {output_path}")
print(filtered_df.head())