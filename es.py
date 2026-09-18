import os
import requests

class ES:
    def __init__(self):
        self.base = os.environ["ELASTIC_URL"].rstrip("/")
        self.verify = os.getenv("ELASTIC_VERIFY_TLS", "true").lower() == "true"
        ca = os.getenv("ELASTIC_CA_CERT", "").strip()
        if ca:
            self.verify = ca
        self.auth = None
        self.headers = {"Content-Type": "application/json"}
        api_key = os.getenv("ELASTIC_API_KEY")
        if api_key:
            self.headers["Authorization"] = f"ApiKey {api_key}"
        elif os.getenv("ELASTIC_USERNAME"):
            self.auth = (os.environ["ELASTIC_USERNAME"], os.environ.get("ELASTIC_PASSWORD", ""))

    def request(self, method, path, body=None, params=None, timeout=60, allow_404=False):
        r = requests.request(method, self.base + path, headers=self.headers, auth=self.auth,
                             verify=self.verify, json=body, params=params, timeout=timeout)
        if allow_404 and r.status_code == 404:
            return r.json() if r.content else {}
        if not r.ok:
            raise RuntimeError(f"Elasticsearch {method} {path} failed: {r.status_code} {r.text[:2000]}")
        return r.json() if r.content else {}
