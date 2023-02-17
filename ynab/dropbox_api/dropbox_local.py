import webbrowser
import json
import requests
import logging

from utilities import DROPBOX_CREDS, DROPBOX_TOKEN, DROPBOX_ENDPOINTS

_logger = logging.getLogger(__name__)


def read_json(file):
    with open(file) as json_file:
        data = json.load(json_file)
    return data


def write_json(file, data):
    json_object = json.dumps(data, indent=4)

    with open(file, "w") as outfile:
        outfile.write(json_object)


class DropboxLocal:
    def __init__(self) -> None:
        self._token_file = DROPBOX_TOKEN
        self._creds = read_json(DROPBOX_CREDS)
        self._endpoints = read_json(DROPBOX_ENDPOINTS)

        self._tokens = read_json(self._token_file)
        self._sign_in()

    def _sign_in(self):
        creds_file = DROPBOX_CREDS
        token_file = DROPBOX_TOKEN
        if self._tokens["refresh_token"] != "":
            _logger.info("refresh token found...")
            _logger.info("validating if refresh token is valid...")
            if self._get_sl_token():
                _sign_in_required = False
            else:
                _logger.info("refresh token is invalid...")
                _logger.info("invalidating access code and refresh token...")
                self._creds["access_code"] = ""
                self._tokens["refresh_token"] = ""
                _sign_in_required = True
        else:
            _logger.info("refresh token not found...")
            _logger.info("invalidating access code...")
            self._creds["access_code"] = ""
            _sign_in_required = True

        if _sign_in_required:
            _logger.info("obtaining access code...")
            self._creds["access_code"] = self._get_access_code()
            write_json(file=creds_file, data=self._creds)
            _logger.info(f"access code: {self._creds['access_code']}")

            _logger.info("obtaining refresh token...")
            self._tokens["refresh_token"] = self._get_refresh_token()
            write_json(file=token_file, data=self._tokens)
            _logger.info(f"refresh_token: {self._tokens['refresh_token']}")

    def _get_access_code(self):
        uri = f"{self._endpoints['access_uri']}"
        uri += f"{self._creds['app_key']}"
        uri += f"{self._creds['access_uri_query']}"

        webbrowser.open(uri)
        access_code = input('Access code: ')
        return access_code

    def _get_refresh_token(self):
        data = f"code={self._creds['access_code']}"
        data += "&grant_type=authorization_code"
        uri = self._creds["token_uri"]
        app_key = self._creds["app_key"]
        app_secret = self._creds["app_secret"]
        response = requests.post(
            uri,
            data=data,
            auth=(app_key, app_secret))
        token = json.loads(response.text)
        _logger.info(token)
        return token

    def _get_sl_token(self):
        uri = self._endpoints["token_uri"]
        data = f"refresh_token={self._tokens['refresh_token']}"
        data += "&grant_type=refresh_token"
        app_key = self._creds["app_key"]
        app_secret = self._creds["app_secret"]
        response = requests.post(
            uri,
            data=data,
            auth=(app_key, app_secret))
        token = json.loads(response.text)
        if "access_token" in token:
            _logger.info("refresh token is valid...")
            self._tokens["access_token"] = token
            write_json(file=self._token_file, data=self._tokens)
            return True
        else:
            return False

    def get_files(self):
        _ = self._get_sl_token()
        uri = self._endpoints["list_folder_uri"]
        auth_token = self._tokens["access_token"]["access_token"]
        header = {
            'Authorization': 'Bearer ' + auth_token
        }
        data = {"path": ""}

        response = requests.post(uri, json=data, headers=header)
        files = response.json()["entries"]
        return files
