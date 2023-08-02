import requests
import csv

url = "https://imdb8.p.rapidapi.com/auto-complete"

querystring = {"q":"game of thr","s":"spiderman"}

headers = {
	"X-RapidAPI-Key": "4e246d96f5mshbe6cfe2fb54dfe0p1db841jsn79fb227bbd4a",
	"X-RapidAPI-Host": "imdb8.p.rapidapi.com"
}

response = requests.get(url,headers=headers, params=querystring)

data = response.json()
print(data)
# Check if 'd' key is present in the JSON response
if 'd' in data:
    items = data['d']
    if not items:
        print("No data found.")
        exit()

    # Extract relevant attributes and write to CSV
    with open("imdb_results.csv", mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['Title', 'Year', 'Type']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for item in items:
            title = item.get('l', '')  # Title of the movie or TV show
            year = item.get('y', '')   # Year of release
            content_type = item.get('q', '')  # Type of content (e.g., movie, tvSeries)

            writer.writerow({'Title': title, 'Year': year, 'Type': content_type})

    print("Data saved successfully to 'imdb_results.csv'.")
else:
    print("Error in the API response.")






