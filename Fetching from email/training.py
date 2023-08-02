import os
import base64
import pandas as pd
import io
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

flow = InstalledAppFlow.from_client_secrets_file(
    'path_to_your_client_secret.json', SCOPES)
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

results = service.users().messages().list(userId='me', q='is:inbox', maxResults=1).execute()
message = results['messages'][0]

attachments = get_attachments(message['id'])
csv_data = None
for filename, data in attachments:
    if filename.lower().endswith('.csv'):
        csv_data = data
        break

if csv_data:
    df = pd.read_csv(io.BytesIO(csv_data))

    print("Column Names:", df.columns)
    print("Original DataFrame:")
    print(df)
    df = df.drop([0, 1]) #[-1,-2]
    print("\nDataFrame after dropping first and second rows:")
    print(df)

    # Save the modified DataFrame to a CSV file on the desktop
    desktop_path = os.path.join(os.path.join(os.environ['HOME']), 'Desktop')
    output_file_path = os.path.join(desktop_path, 'modified_dataframe.csv')
    df.to_csv(output_file_path, index=False)
    print(f"\nModified DataFrame has been saved as 'modified_dataframe.csv' on the desktop.")






