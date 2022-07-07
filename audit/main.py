import os.path

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd

# 1) Creating a service account - https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount
# 2) Delegating domain-wide authority to the service account - https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority
# 3) Change Email in email variable
# 4) Run this main.py script

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

"""Authorization under the email from which we conduct the audit"""
email = 'example@email.com'


def mainAuthorization(email):
    credentials = None
    if os.path.exists('service_account.json'):
        credentials = service_account.Credentials.from_service_account_file(
            'service_account.json', scopes=SCOPES)
        delegated_credentials = credentials.with_subject(email)
        service = build('drive', 'v3', credentials=delegated_credentials)
        return service


"""Get all permissions for a specific fileId"""


def mainCredentials(fileId):
    service = mainAuthorization(email)
    results = service.permissions().list(
        fileId=fileId,
        pageSize=100,
        fields="nextPageToken,kind,permissions,permissions(id,displayName,type,kind,role,emailAddress)"
    ).execute()
    return results['permissions']


"""Get a list of files in one page and a nextPageToken for pagination"""


def mainFiles(nextPageToken=None):
    service = mainAuthorization(email)
    results = service.files().list(
        pageToken=nextPageToken,
        pageSize=100,
        fields="nextPageToken, files(id, name)"
    ).execute()
    items = results.get('files', [])
    try:
        pageToken = results['nextPageToken']
    except:
        pageToken = None
    return items, pageToken


"""Get a list of all files on account"""


def mainAllFiles():
    df_all_files = []
    newPageToken = ''
    while newPageToken is not None:
        df, pageToken = mainFiles(newPageToken)
        newPageToken = pageToken
        df_all_files = df_all_files + df
    return pd.DataFrame(df_all_files)


"""The main function that stores all permissions to all files in Good Data excel table
and stores in Bad Data excel table all files in which it is impossible to view permissions"""


def main():
    df_good = pd.DataFrame(
        columns=[
            'id',
            'displayName',
            'type',
            'kind',
            'role',
            'emailAddress',
            'allowFileDiscovery',
            'fileId'
        ])
    df_bad = pd.DataFrame(
        columns=[
            'message'
        ])
    print('Start extracting all file ids...')
    ids = mainAllFiles()
    print('Finish extracting all file ids...')
    print('Creating a DataFrame...')
    for id in ids['id']:
        try:
            df = pd.DataFrame(mainCredentials(id))
            df['fileId'] = id
            df_good = pd.concat([df_good, df], ignore_index=True)
        except:
            df_msg = pd.DataFrame(
                {'message':
                    [f'You do not have permission to access this file metadata from {email}: https://drive.google.com/open?id={id}']})
            df_bad = pd.concat([df_bad, df_msg], ignore_index=True)
    print('Start saving all the data to excel file...')
    with pd.ExcelWriter(f'credentialsAudit_{email}.xlsx') as writer:
        df_good.to_excel(writer, sheet_name='Good Data')
        df_bad.to_excel(writer, sheet_name='Bad Data')
    print('Finish saving all the data to excel file...')


if __name__ == '__main__':
    main()
