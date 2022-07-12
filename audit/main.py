import os.path

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from multiprocessing import Pool

# 1) Creating a service account - https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount
# 2) Delegating domain-wide authority to the service account - https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority
# 3) Change Email in email variable
# 4) Run this main.py script

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

"""Authorization under the email from which we conduct the audit"""
email = 'test@abc.com'


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
    results = []
    service = mainAuthorization(email)
    try:
        response = service.permissions().list(
            fileId=fileId,
            pageToken=None,
            pageSize=100,
            fields="nextPageToken,kind,permissions,permissions(id,displayName,type,kind,role,emailAddress)"
        ).execute()
        results = response.get('permissions', [])
    except:
        pass
    try:
        while 'nextPageToken' in response:
            response = service.permissions().list(
                fileId=fileId,
                pageToken=response.get('nextPageToken', []),
                pageSize=100,
                fields="nextPageToken,kind,permissions,permissions(id,displayName,type,kind,role,emailAddress)"
            ).execute()
            results.extend(response.get('files', []))
    except:
        pass
    df = pd.DataFrame(results)
    if not df.empty:
        df['fileId'] = fileId
    return df


"""Get a list of all files on account"""


def mainAllFiles():
    results = []
    service = mainAuthorization(email)
    response = service.files().list(
        pageToken=None,
        pageSize=100,
        fields="nextPageToken, files(id, name)"
    ).execute()
    results = response.get('files', [])
    while 'nextPageToken' in response:
        response = service.files().list(
            pageToken=response.get('nextPageToken', []),
            pageSize=100,
            fields='nextPageToken, files(id, name)'
        ).execute()
        results.extend(response.get('files', []))
    return pd.DataFrame(results)


"""The main function that stores all permissions to all files in excel table"""


def main():
    df_all = pd.DataFrame(
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
    ids = mainAllFiles()
    p = Pool(os.cpu_count())
    df_pool = p.map(mainCredentials, ids['id'])
    for df in df_pool:
        if not df.empty:
            df_all = pd.concat([df_all, df], ignore_index=True)
    df_all.to_excel(f'credentialsAudit_{email}.xlsx')


if __name__ == '__main__':
    main()


# TODO: mainCredentials try - except problem
# TODO: add bad files
# TODO: test on a big Google Drive account
