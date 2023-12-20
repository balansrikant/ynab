import webbrowser
import json
import requests
import logging

from dropbox import Dropbox

from utilities import DBX_CREDS_FILE, DBX_TOKEN_FILE, DBX_ENDPOINTS_FILE

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
        self._token_file = DBX_TOKEN_FILE
        self._creds_file = DBX_CREDS_FILE
        self._endpoints_file = DBX_ENDPOINTS_FILE

        self._creds = read_json(DBX_CREDS_FILE)
        self._endpoints = read_json(DBX_ENDPOINTS_FILE)
        self._tokens = read_json(self._token_file)

        self._file_metadata = []
        self._sign_in()

    def _sign_in(self):
        _logger.info("validating if refresh token is populated...")
        if self._tokens["refresh_token"] != "":
            _logger.info("...refresh token found")
            _logger.info("validating if refresh token is valid...")
            _token = self._get_sl_token()
            if isinstance(_token, dict) and "access_token" in _token:
                self._tokens["access_token"] = _token
                _logger.info("...refresh token is valid...")
                _sign_in_required = False
            else:
                _logger.info("...refresh token is invalid")
                _logger.info("...invalidating access code and refresh token")
                self._creds["access_code"] = ""
                self._tokens["refresh_token"] = ""
                _sign_in_required = True
            write_json(file=self._token_file, data=self._tokens)
        else:
            _logger.info("...refresh token not found")
            _logger.info("...invalidating access code")
            self._creds["access_code"] = ""
            self._tokens['access_token'] = ""
            _sign_in_required = True

        if _sign_in_required:
            _logger.info("sign-in required...")
            _logger.info("...obtaining access code")
            self._creds["access_code"] = self._get_access_code()
            write_json(file=self._creds_file, data=self._creds)
            _logger.info(f"access code: {self._creds['access_code']}")

            _logger.info("...obtaining refresh token")
            self._tokens["refresh_token"] = self._get_refresh_token()
            _logger.info(f"...refresh_token: {self._tokens['refresh_token']}")
            write_json(file=self._token_file, data=self._tokens)
        else:
            _logger.info("sign-in not required...")

    def _get_access_code(self):
        uri = f"{self._endpoints['access_uri']}"
        uri += f"{self._creds['app_key']}"
        uri += f"{self._endpoints['access_uri_query']}"

        webbrowser.open(uri)
        access_code = input('Access code: ')
        return access_code

    def _get_refresh_token(self):
        data = f"code={self._creds['access_code']}"
        data += "&grant_type=authorization_code"
        uri = self._endpoints["token_uri"]
        app_key = self._creds["app_key"]
        app_secret = self._creds["app_secret"]
        response = requests.post(
            uri,
            data=data,
            auth=(app_key, app_secret))
        token = json.loads(response.text)
        return token["refresh_token"]

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
        return token

    def get_file_metadata(self):
        """ download list of files and their metadata """
        _ = self._get_sl_token()
        _auth_token = self._tokens["access_token"]["access_token"]
        _dbx = Dropbox(_auth_token)
        _response = _dbx.files_list_folder("")
        _entries = _response.entries
        _files = []
        for _entry in _entries:
            sm = _entry.server_modified.strftime("%Y-%m-%d %H:%M:%S")
            _file = {
                "id": _entry.id,
                "name": _entry.name,
                "server_modified": sm,
                "path": _entry.path_display
                }
            _files.append(_file)
        self._file_metadata = _files
        return True

    def sync_files(self, filename="", data_files={}):
        """Synchronize files on server vs local files

        Args:
            file_path (Path): Pathlib Path
            extension (str): extension to search for
            file_type (str): type of file e.g. tabula, processed, ynab, transaction

        Returns:
            list: list of files (dict) -> statement_date, pathlib path
        """

        def _write_file(_local_path, content):
            with open(_local_path, 'w', encoding='UTF8', newline='') as f:
                f.write(content.decode("utf-8"))

        _ = self._get_sl_token()
        _auth_token = self._tokens["access_token"]["access_token"]
        _dbx = Dropbox(_auth_token)
        if not filename:
            _server_paths = [f["path"] for f in self._file_metadata]
            _common_files = {f: files_dict[f] for f in files_dict.keys()
                             if files_dict[f]["server_path"] in _server_paths}
            for k in _common_files.keys():
                _server_path = _common_files[k]["server_path"]
                _local_path = _common_files[k]["local_path"]
                _, _response = _dbx.files_download(_server_path)
                _write_file(_local_path, _response.content)
        else:
            _server_path = files_dict[filename]["server_path"]
            _, _response = _dbx.files_download(_server_path)
            print(_response.content.decode("utf-8"))
            _local_path = files_dict[filename]["local_path"]
            _write_file(_local_path, _response.content)
