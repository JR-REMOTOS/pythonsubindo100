import requests
import logging

class XtreamAPI:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.logger = logging.getLogger(__name__)

    def _make_request(self, endpoint, params=None, stream=False):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=60, stream=stream)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error making request to {url}: {e}")
            raise

    def authenticate(self):
        params = {
            'username': self.username,
            'password': self.password
        }
        try:
            response = self._make_request('player_api.php', params=params)
            data = response.json()
            if data.get('user_info', {}).get('auth') == 1:
                self.logger.info("Authentication successful.")
                return True
            else:
                self.logger.warning("Authentication failed. Check credentials.")
                return False
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False

    def download_m3u(self):
        if not self.authenticate():
            raise Exception("Authentication failed. Cannot download M3U.")

        params = {
            'username': self.username,
            'password': self.password,
            'type': 'm3u_plus',
            'output': 'ts'
        }
        try:
            response = self._make_request('get.php', params=params, stream=True)
            return response.iter_content(chunk_size=1024)
        except Exception as e:
            self.logger.error(f"Failed to download M3U: {e}")
            raise
