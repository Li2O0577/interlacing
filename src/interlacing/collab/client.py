"""Small synchronous Python client for the collaboration HTTP API."""
import json
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class ServiceError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code = status, code


class Client:
    def __init__(self, base_url: str, token: str, *, timeout=30):
        self.url = base_url.rstrip("/") + "/v1/request"
        self.token, self.timeout = token, timeout

    def request(self, action: str, **args):
        body = json.dumps({"action": action, "args": args}, allow_nan=False).encode()
        request = Request(self.url, data=body, headers={"Content-Type": "application/json",
                                                        "Authorization": "Bearer " + self.token})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except HTTPError as exc:
            with exc:
                try:
                    detail = json.load(exc)
                except (ValueError, UnicodeError):
                    detail = {}
            if not isinstance(detail, dict):
                detail = {}
            raise ServiceError(exc.code, detail.get("code", "http_error"),
                               detail.get("error", str(exc.reason))) from exc

    def events(self, run, *, after=0, scope=None, follow=False, interval=0.5):
        """Yield ordered events. Resume from the last seq; follow polls through human waits."""
        if follow and interval <= 0:
            raise ValueError("polling interval must be positive")
        while True:
            # Read status first so the final page cannot race with a newly committed end.
            finished = not follow or self.request("status", run=run)["state"] in ("done", "failed", "stopped", "preview")
            page = self.request("history", run=run, after=after, scope=scope, limit=100)
            for item in page:
                after = item["seq"]
                yield item
            if len(page) < 100:
                if finished:
                    return
                time.sleep(interval)
