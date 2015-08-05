import sys
import httplib2
import argparse
import tempfile
import json

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow, argparser

from config import get_config, decode_var


env = get_config([('io', 'google_drive')])

GDRIVE_CLIENT_SECRET, GDRIVE_TOKEN_SECRET = decode_var(env.GDRIVE_CLIENT_SECRET, True),\
                                            decode_var(env.GDRIVE_TOKEN_SECRET, True)

def create_tempfile_from_var(var, file_suffix='.json'):
    var_tempfile = tempfile.NamedTemporaryFile(suffix=file_suffix)
    json.dump(var, var_tempfile)
    var_tempfile.flush()
    return var_tempfile

# The file with the OAuth 2.0 Client details for authentication and authorization.
SECRETS_FILE = create_tempfile_from_var(GDRIVE_CLIENT_SECRET)
#The file with the refresh token and access code
TOKEN_FILE = create_tempfile_from_var(GDRIVE_TOKEN_SECRET, file_suffix='.dat')

# A file to store the access token
PARSER = argparse.ArgumentParser(parents=[argparser])
FLAGS = PARSER.parse_args()

class GoogleAuth(object):
    """Base Google Authentication Object"""
    def __init__(self):
        self.scope = None
        self.service_name = ''
        self.version_name = ''

    def prepare_credentials(self, flags=None):
        # Retrieve existing credendials
        storage = Storage(TOKEN_FILE.name)
        credentials = storage.get()
        # If existing credentials are invalid and Run Auth flow
        # the run method will store any new credentials
        if credentials is None or credentials.invalid:
            # The Flow object to be used if we need to authenticate.
            self.flow = flow_from_clientsecrets(SECRETS_FILE.name,
                    scope=self.scope, message='Client Secret file is missing')
            credentials = run_flow(self.flow, storage, FLAGS) #run Auth Flow and store credentials
        return credentials

    def initialize_service(self):
        try:
            assert self.service_name, self.version_name
        except:
            sys.stderr.write('NO SERVICE OR VERSION SPECIFIED')
        # 1. Create an http object
        http = httplib2.Http()

        # 2. Authorize the http object
        # In this tutorial we first try to retrieve stored credentials. If
        # none are found then run the Auth Flow. This is handled by the
        # prepare_credentials() function defined earlier in the tutorial
        credentials = self.prepare_credentials()
        http = credentials.authorize(http)  # authorize the http object

        # 3. Build the Analytics Service Object with the authorized http object
        return build(self.service_name, self.version_name, http=http)

class GoogleAnalytics(GoogleAuth):
    """Google Analytics Object"""

    def __init__(self, initialize=False):
        self.scope = 'https://www.googleapis.com/auth/analytics.readonly'
        self.service_name = 'analytics'
        self.version_name = 'v3'
        if initialize:
            sys.stdout.write('Initializing Google Analytics Service')
            self.service = self.initialize_service()

    def get_accounts(self):
        # Get a list of all Google Analytics accounts for this user
        return self.service.management().accounts().list().execute()

    def get_properties(self, account_id):
        """ Get a list of all the Web Properties for an account"""
        return self.service.management().webproperties().list(accountId=account_id).execute()

    def get_profiles(self, property_id, account_id):
        """Get all the profiles for a Web Property of an account"""
        return self.service.management().profiles().list(
        accountId=account_id,
        webPropertyId=property_id).execute()

    def query(self, query_params):
        """ Use the Analytics Service Object to query the Core Reporting API
        Arguments:
        query_params: dictionary of query parameters,
            sample query parameters are:
                ids: id of the profperty
                start_date: format YYYY-MM-DD
                end_date: format YYYY-MM-DD
                metrics: e.g. ga:sessions
                dimensions: e.g. ga:hour
        """
        return self.service.data().ga().get(**query_params).execute()

class GoogleDrive(GoogleAuth):
    """Google Drive API Object"""

    def __init__(self, initialize=False):
        self.scope = 'https://www.googleapis.com/auth/drive.readonly'
        self.service_name = 'drive'
        self.version_name = 'v2'
        if initialize:
            sys.stdout.write('Initializing Google Drive Service\n')
            self.service = self.initialize_service()

    def get_file_download_url(self, file_id, **kw):
        """Given a file_id it returns the download_filed link"""
        drive_file = self.service.files().get(fileId=file_id).execute()
        download_url = drive_file.get('downloadUrl')
        if download_url:
            return download_url
        else:
            gdoc_link = drive_file.get('exportLinks')
            if gdoc_link and kw.get('download_format'):
                return gdoc_link[kw['download_format']]
        return None

    def download_file(self, download_url, **kw):
        """with a download_url it downloads the content"""
        resp, content = self.service._http.request(download_url)
        if resp.status == 200:
            if content:
                if kw.get('save_to'):
                    with open(kw['save_to'], 'wb') as file:
                        file.write(content)
                        file.close()
                        return kw['save_to']
                return content
            else:
                print('No content on the file')
                return download_url
        else:
            print('An error occurred: {}'.format(resp))
            return None

    def get_file(self, file_id, **kw):
        download_url = self.get_file_download_url(file_id, **kw)
        if download_url:
            self.download_file(download_url, **kw)
        else:
            # The file doesn't have any content stored on Drive.
            print('no file info found')
            return None

    def download_spreadsheet(self, file_id, save_to):
        '''downloads a google spreadsheet to a local file'''
        file_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return self.get_file(file_id, download_format=file_type, save_to=save_to)

    def retrieve_all_files(self):
        """Retrieve a list of File resources."""
        result = []
        page_token = None
        try:
            while True:
                param = {}
                if page_token:
                    param['pageToken'] = page_token
                files = self.service.files().list(**param).execute()

                result.extend(files['items'])
                page_token = files.get('nextPageToken')
                if not page_token:
                    break
        except HttpError as error:
            print('An error occurred: {}'.format(error))
        return result
