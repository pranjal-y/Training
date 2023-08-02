import os
import base64
import pandas as pd
import io
import requests
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import re
from email import message_from_bytes
from bs4 import BeautifulSoup

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

flow = InstalledAppFlow.from_client_secrets_file(
    'path_to_your_client_secret.json',
    SCOPES)
credentials = flow.run_local_server()
service = build('gmail', 'v1', credentials=credentials)


def get_attachments(message_id):
    message = service.users().messages().get(userId='me', id=message_id, format='full').execute()
    parts = message['payload']['parts']
    attachments = []
    for part in parts:
        if part['filename']:
            if 'body' in part and 'attachmentId' not in part['body']:
                file_data = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
            else:
                attachment = service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=part['body']['attachmentId']
                ).execute()
                file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
            attachments.append((part['filename'], file_data))
    return attachments

def fetch_data_from_hyperlink(hyperlink):
    response = requests.get(hyperlink)
    if response.status_code == 200:
        try:
            data_df = pd.read_csv(io.StringIO(response.text), low_memory=False)
            return data_df
        except pd.errors.ParserError:
            pass
    return None


def extract_hyperlinks_with_data_from_email_body(email_body):
    soup = BeautifulSoup(email_body, 'html.parser')
    links_with_data = []
    for link in soup.find_all('a', href=True):
        hyperlink = link['href']
        data_df = fetch_data_from_hyperlink(hyperlink)
        if data_df is not None:
            links_with_data.append(data_df)
    return links_with_data


def get_hyperlinks_with_data_from_email_body(message_id):
    message = service.users().messages().get(userId='me', id=message_id, format='raw').execute()
    msg_bytes = base64.urlsafe_b64decode(message['raw'].encode('UTF-8'))
    msg = message_from_bytes(msg_bytes)

    links_with_data = []

    for part in msg.walk():
        if part.get_content_type() == "text/html":
            email_body = part.get_payload(decode=True).decode()
            links_with_data = extract_hyperlinks_with_data_from_email_body(email_body)
            break

    return links_with_data


def create_dataframe_from_hyperlinks_with_data(links_with_data):
    # Flatten the list of DataFrames into a single DataFrame
    df = pd.concat(links_with_data, ignore_index=True)
    return df


def detect_links_and_store_data(message_id):
    links_with_data = get_hyperlinks_with_data_from_email_body(message_id)
    if links_with_data:
        df = create_dataframe_from_hyperlinks_with_data(links_with_data)

        desktop_path = os.path.join(os.path.join(os.environ['HOME']), 'Desktop')
        output_file_path = os.path.join(desktop_path, 'hyperlinks_data_dataframe.csv')
        df.to_csv(output_file_path, index=False)
        print("DataFrame with data from hyperlinks has been saved as 'hyperlinks_data_dataframe.csv' on the desktop.")
    else:
        print("No hyperlinks with data found in the email.")


results = service.users().messages().list(userId='me', q='is:inbox', maxResults=1).execute()
message = results['messages'][0]
message_id = message['id']

detect_links_and_store_data(message_id)







