"""Best-effort HTTP delivery of committed-bar notifications."""

from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from market_data_service.ports.committed_bar_notifier import CommittedBarNotification

_CLOSED_BAR_PATH = "/v1/webhooks/closed-bar"
_EXPECTED_BODY = {"status": "accepted"}


class CommittedBarDeliveryError(RuntimeError):
    """Raised when a committed-bar notification could not be confirmed delivered."""


class HttpCommittedBarNotifier:
    """One-attempt POST of one committed-bar notification; never retries."""

    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self._url = f"{base_url.rstrip('/')}{_CLOSED_BAR_PATH}"
        self._timeout_seconds = timeout_seconds

    def send(self, notification: CommittedBarNotification) -> None:
        body = json.dumps(
            {
                "instrument": notification.instrument,
                "timeframe": notification.timeframe,
                "open_time_ms": notification.open_time_ms,
            }
        ).encode("utf-8")
        request = Request(
            self._url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                status = response.status
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise CommittedBarDeliveryError(f"POST {self._url} failed: {exc}") from exc
        except (URLError, TimeoutError) as exc:
            raise CommittedBarDeliveryError(f"POST {self._url} failed: {exc}") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CommittedBarDeliveryError(f"POST {self._url} returned a non-JSON body") from exc
        if status != 200 or payload != _EXPECTED_BODY:
            raise CommittedBarDeliveryError(
                f"POST {self._url} returned unexpected status={status} body={payload!r}"
            )
