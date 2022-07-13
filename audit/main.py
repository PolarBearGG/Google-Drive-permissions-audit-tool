import os.path

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from multiprocessing import Pool

# 1) Creating a service account - https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount
# 2) Delegating domain-wide authority to the service account - https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority
# 3) Change Email in email variable
# 4) Run this main.py script
# 5) In the test, the execution of the script with 139316 files took around 2 hours

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

"""Authorization under the EMAIL from which we conduct the audit"""

EMAIL = 'test@abc.com'


def mainAuthorization(EMAIL):
    credentials = None
    if os.path.exists('service_account.json'):
        credentials = service_account.Credentials.from_service_account_file(
            'service_account.json', scopes=SCOPES)
        delegated_credentials = credentials.with_subject(EMAIL)
        service = build('drive', 'v3', credentials=delegated_credentials)
        return service


"""Get all permissions for a specific fileId"""


def mainCredentials(fileId):
    results = []
    service = mainAuthorization(EMAIL)
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
    service = mainAuthorization(EMAIL)
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


"""The main function that stores all permissions to all files in csv file"""


def main():
    ids = mainAllFiles()
    p = Pool(os.cpu_count())
    df_pool = p.map(mainCredentials, ids['id'])
    df_all = pd.concat(df_pool, ignore_index=True)
    df_all.to_csv(f'credentialsAudit_{EMAIL}.csv', index=False)


if __name__ == '__main__':
    main()


# TODO: mainCredentials try - except problem
# TODO: add bad files
