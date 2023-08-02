#https://developers.google.com/youtube/v3/docs/channels/list
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import csv


api_key = 'AIzaSyBkUCK6I6TsCCYsHCCE2wseX8cv99MB424'
channel_id = 'UCnz-ZXXER4jOvuED5trXfEA' #https://commentpicker.com/youtube-channel-id.php

youtube =build('youtube','v3',developerKey = api_key) #.json file for 0auth

#extract channel details

def get_channel_stats (youtube,channel_id):
    request = youtube.channels().list(part='snippet,contentDetails,statistics',id=channel_id)
    response = request.execute()
    return response

# Get channel statistics and print them , direct calling the function was not giving any output
response = get_channel_stats(youtube, channel_id)
#print(response)

# To access specific statistics and print them
statistics = response['items'][0]['statistics']
#print(statistics)


# Extract relevant information from the API response
channel_data = response['items'][0]
channel_snippet = channel_data['snippet']
channel_statistics = channel_data['statistics']

data_to_write = {
    'Channel Title': channel_snippet['title'],
    'Channel Description': channel_snippet['description'],
    'Subscriber Count': channel_statistics['subscriberCount'],
    'View Count': channel_statistics['viewCount'],
    'Video Count': channel_statistics['videoCount'],
}

# Define the CSV file name
csv_file_name = f'{channel_snippet["title"]}_stats.csv'

# Write data to CSV
with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=data_to_write.keys())
    writer.writeheader()
    writer.writerow(data_to_write)

print(f'Data for the channel "{channel_snippet["title"]}" has been saved to {csv_file_name}.')
