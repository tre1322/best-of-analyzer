import requests
import csv
import time
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ.get("GOOGLE_API_KEY")

<<<<<<< HEAD
=======
import os

import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ.get("GOOGLE_API_KEY")

>>>>>>> ec2525d (Use environment variable for Google API key)

# Define your category search terms (simplified and matched manually)
categories = [
    "restaurant", "bakery", "coffee shop", "steakhouse", "pizza", "sushi",
    "bar", "brewery", "winery", "ice cream shop", "auto repair", "insurance",
    "law office", "tattoo shop", "spa", "chiropractor", "dentist", "hospital",
    "pharmacy", "gift shop", "book store", "clothing store", "thrift shop",
    "hardware store", "art gallery", "yoga studio", "daycare", "photographer"
]

cities = [
    "Des Moines, IA", "West Des Moines, IA", "Ankeny, IA", "Urbandale, IA",
    "Clive, IA", "Altoona, IA", "Johnston, IA", "Waukee, IA"
]

results = []

def query_places(query, location):
    base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": f"{query} in {location}",
        "key": API_KEY
    }
    response = requests.get(base_url, params=params)
    time.sleep(1)  # avoid rate limiting
    return response.json().get("results", [])

for category in categories:
    for city in cities:
        print(f"Searching for: {category} in {city}")
        places = query_places(category, city)
        for place in places:
            results.append({
                "Business Name": place.get("name"),
                "Address": place.get("formatted_address"),
                "Place ID": place.get("place_id"),
                "Category Query": category,
                "City": city
            })

# Save to CSV
output_path = "business_master.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

print(f"✅ Done! {len(results)} businesses saved to business_master.csv")