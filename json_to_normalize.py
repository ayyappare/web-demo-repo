import requests
import pandas as pd

def fetch_api_data(api_url, api_key):
    """Fetch data from API."""
    headers = {"x-api-key": api_key}
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()  # Raise error if request fails
    return response.json()

def flatten_json(df):
    """
    Recursively normalizes JSON, automatically handling nested lists and dictionaries, including lists of dicts inside lists.
    """
    while True:
        list_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
        if not list_cols:
            break  # Stop if no more lists to normalize
        for col in list_cols:
            df = df.explode(col).reset_index(drop=True)  # Expand lists into rows
            try:
                normalized = pd.json_normalize(df.pop(col), sep="_")  # Normalize nested dicts
                df = df.join(normalized, rsuffix="_expanded")  # Join back
            except:
                df[col] = df[col].astype(str)  # Convert remaining non-dict lists to strings
    return df

def main():
    API_URL = ""
    API_KEY = ""
    
    # Step 1: Fetch data from API
    data = fetch_api_data(API_URL, API_KEY)
    print("Raw Data:", data)
    
    # Step 2: Convert to DataFrame & Handle List of Lists
    if isinstance(data, list):
        data = [item for sublist in data for item in sublist] if all(isinstance(i, list) for i in data) else data
    
    # Step 3: Initial Normalization
    df = pd.json_normalize(data, sep="_")
    print("After Initial Normalization:\n", df)
    
    # Step 4: Further Normalization
    df = flatten_json(df)
    print("After Further Normalization:\n", df)
    
    # Step 5: Save to CSV
    csv_filename = "lot-location.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Data saved to {csv_filename}")

if __name__ == "__main__":
    main()
