"""SOAP client handling, including CapturisStream base class.

The Capturis (NISC) ``BillingInfo`` service is a SOAP 1.1 endpoint. There is no
OAuth/token flow: the username and password are passed directly as positional
SOAP arguments on every operation (see ``tap_capturis.streams`` for the call
hierarchy). This module builds SOAP envelopes, posts them, and converts the XML
responses into plain Python dicts/lists.
"""

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

import backoff
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError
from http.client import RemoteDisconnected
from requests.exceptions import ChunkedEncodingError

from hotglue_singer_sdk import Stream
from hotglue_singer_sdk.exceptions import RetriableAPIError
from hotglue_singer_sdk.tap_base import InvalidCredentialsError

logging.getLogger("backoff").setLevel(logging.CRITICAL)

DEFAULT_API_URL = "https://portal.capturis.com/ubp-ws/v4/BillingInfo"
SOAP_ENV_NS = "http://schemas.xmlsoap.org/soap/envelope/"
BILLING_NS = "http://billing.v4.ws.cni.nisc.cc/"

# Index of the password argument in SOAP calls, redacted when logging.
_PASSWORD_ARG_INDEX = 1

# Field names that hold the identifier in getCustomers / getSiteList responses.
CUSTOMER_ID_FIELD = "customerNbr"
SITE_ID_FIELD = "siteId"


def _localname(tag: str) -> str:
    """Return an XML tag's local name, dropping any ``{namespace}`` prefix."""
    return tag.split("}", 1)[1] if "}" in tag else tag


def element_to_obj(element: ET.Element) -> Any:
    """Recursively convert an XML element into a scalar / dict / list.

    Leaf elements become their stripped text (or ``None``). Elements with
    children become a dict keyed by child local name; repeated child tags are
    collapsed into a list, which is how SOAP encodes array members.
    """
    children = list(element)
    if not children:
        text = (element.text or "").strip()
        return text or None

    result: Dict[str, Any] = {}
    for child in children:
        key = _localname(child.tag)
        value = element_to_obj(child)
        if key in result:
            existing = result[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                result[key] = [existing, value]
        else:
            result[key] = value
    return result


class CapturisStream(Stream):
    """Base stream that talks to the Capturis BillingInfo SOAP endpoint."""

    @property
    def api_url(self) -> str:
        """Return the SOAP endpoint URL (configurable)."""
        return self.config.get("api_url", DEFAULT_API_URL)

    @property
    def username(self) -> str:
        return self.config["username"]

    @property
    def password(self) -> str:
        return self.config["password"]

    @property
    def timeout(self) -> int:
        """Return the request timeout limit in seconds."""
        return self.config.get("request_timeout", 300)

    @property
    def requests_session(self) -> requests.Session:
        """Lazily create a pooled requests session with basic transport retries."""
        session = getattr(self, "_requests_session", None)
        if session is None:
            session = requests.Session()
            adapter = HTTPAdapter(
                pool_connections=10,
                pool_maxsize=10,
                max_retries=Retry(total=3, backoff_factor=0.3),
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self._requests_session = session
        return session

    @staticmethod
    def build_envelope(operation: str, args: List[Any]) -> str:
        """Build a SOAP 1.1 envelope for ``operation`` with positional ``arg0..argN``."""
        if args:
            inner = "".join(
                f"<arg{i}>{escape('' if a is None else str(a))}</arg{i}>"
                for i, a in enumerate(args)
            )
            op = f"<bil:{operation}>{inner}</bil:{operation}>"
        else:
            op = f"<bil:{operation}/>"
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            f'<soapenv:Envelope xmlns:soapenv="{SOAP_ENV_NS}" xmlns:bil="{BILLING_NS}">'
            f"<soapenv:Header/><soapenv:Body>{op}</soapenv:Body></soapenv:Envelope>"
        )

    @staticmethod
    def _redact_args(args: List[Any]) -> List[Any]:
        """Return args with the password positionally redacted for logging."""
        redacted = list(args)
        if len(redacted) > _PASSWORD_ARG_INDEX:
            redacted[_PASSWORD_ARG_INDEX] = "***"
        return redacted

    def call_soap(self, operation: str, args: List[Any]) -> List[Any]:
        """Invoke a SOAP ``operation`` and return its ``<return>`` items as objects."""
        envelope = self.build_envelope(operation, args)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            # Many Java/JAX-WS stacks require a SOAPAction header to be present.
            "SOAPAction": "",
        }
        self.logger.info(
            "Calling Capturis SOAP operation '%s' args=%s",
            operation,
            self._redact_args(args),
        )

        def do_request() -> requests.Response:
            response = self.requests_session.post(
                self.api_url,
                data=envelope.encode("utf-8"),
                headers=headers,
                timeout=self.timeout,
            )
            self.validate_response(response, operation)
            return response

        response = self.request_decorator(do_request)()
        returns = self._parse_returns(response.text, operation)
        self.logger.info(
            "Capturis operation '%s' returned %s item(s)", operation, len(returns)
        )
        return returns

    def validate_response(self, response: requests.Response, operation: str) -> None:
        """Validate an HTTP/SOAP response, raising retriable/non-retriable errors.

        SOAP faults are returned with an HTTP 500, so the body is inspected for a
        ``Fault`` element first: a fault means a genuine application error (e.g.
        bad credentials) and must NOT be retried.
        """
        fault = self._find_fault(response.text)
        if fault:
            raise InvalidCredentialsError(
                f"SOAP Fault in operation '{operation}': {fault}"
            )

        if response.status_code in (401, 403):
            raise InvalidCredentialsError(
                f"Unauthorized ({response.status_code}) calling '{operation}': "
                f"{response.text[:500]}"
            )
        if response.status_code == 429 or 500 <= response.status_code < 600:
            raise RetriableAPIError(
                f"{response.status_code} Server Error calling '{operation}': "
                f"{response.text[:500]}"
            )
        if response.status_code >= 400:
            raise InvalidCredentialsError(
                f"{response.status_code} Client Error calling '{operation}': "
                f"{response.text[:500]}"
            )

    def _parse_xml(self, text: str) -> ET.Element:
        try:
            return ET.fromstring(text)
        except ET.ParseError as e:
            raise RetriableAPIError(f"Invalid XML response: {e}: {text[:500]}")

    @staticmethod
    def _find_first(element: ET.Element, localname: str) -> Optional[ET.Element]:
        for el in element.iter():
            if _localname(el.tag) == localname:
                return el
        return None

    def _find_fault(self, text: str) -> Optional[str]:
        """Return a human-readable fault description if the body contains a SOAP Fault.

        Capturis returns a generic SOAP Fault (HTTP 500, faultstring
        "Fault occurred while processing.") for invalid credentials, so detecting
        the fault lets the caller raise a non-retriable auth error instead of a
        retried 500. If the body isn't well-formed XML we fall back to a raw text
        scan so a slightly malformed fault is still recognized.
        """
        if not text:
            return None
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return self._raw_fault(text)

        fault = self._find_first(root, "Fault")
        if fault is None:
            return None
        parts: List[str] = []
        for el in fault.iter():
            ln = _localname(el.tag)
            if ln in ("faultcode", "faultstring", "faultactor") and el.text:
                parts.append(f"{ln}={el.text.strip()}")
        return "; ".join(parts) or "SOAP Fault"

    @staticmethod
    def _raw_fault(text: str) -> Optional[str]:
        """Detect a SOAP Fault via raw text when the body isn't parseable XML."""
        if "Fault" not in text and "faultstring" not in text:
            return None
        match = re.search(r"<[^>]*faultstring[^>]*>(.*?)</[^>]*faultstring>", text, re.S)
        if match:
            return f"faultstring={match.group(1).strip()}"
        return "SOAP Fault"

    def _parse_returns(self, text: str, operation: str) -> List[Any]:
        """Extract the SOAP response payload as a list of objects.

        Looks for the ``*Response`` wrapper under the SOAP Body, then returns its
        repeated ``<return>`` members converted to objects. If the response has no
        ``<return>`` members, the response element itself is converted and wrapped.
        """
        root = self._parse_xml(text)
        body = self._find_first(root, "Body")
        container = body if body is not None else root

        response_el: Optional[ET.Element] = None
        for child in list(container):
            if _localname(child.tag).lower().endswith("response"):
                response_el = child
                break

        if response_el is None:
            children = list(container)
            response_el = children[0] if children else container

        returns = [
            child
            for child in list(response_el)
            if _localname(child.tag) == "return"
        ]
        if returns:
            return [element_to_obj(r) for r in returns]

        obj = element_to_obj(response_el)
        if obj is None:
            return []
        return obj if isinstance(obj, list) else [obj]

    def request_decorator(self, func: Callable) -> Callable:
        """Wrap a request callable with exponential backoff on transient failures."""
        return backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                ProtocolError,
                RemoteDisconnected,
                ChunkedEncodingError,
            ),
            max_tries=8,
            factor=4,
            on_backoff=self.backoff_handler,
        )(func)

    def backoff_handler(self, details) -> None:
        logging.info(
            "Backing off {wait:0.1f} seconds after {tries} tries "
            "calling function {target}".format(**details)
        )

    # ------------------------------------------------------------------
    # Shared bill-traversal helpers
    #
    # Bills are exposed hierarchically (customers -> sites -> bills), and both
    # the ``bills`` and ``bill_attachments`` streams need to walk that same
    # chain over the same incremental date window, so the traversal lives on the
    # base class.
    # ------------------------------------------------------------------

    @staticmethod
    def _to_utc(dt: datetime) -> datetime:
        """Return a plain (non-pendulum) timezone-aware UTC datetime.

        Naive datetimes are assumed to be UTC. A plain ``datetime`` is returned so
        downstream ``timedelta`` arithmetic doesn't depend on pendulum internals.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return datetime(
            dt.year, dt.month, dt.day, dt.hour, dt.minute,
            dt.second, dt.microsecond, tzinfo=timezone.utc,
        )

    @classmethod
    def _to_iso_z(cls, dt: datetime) -> str:
        """Format a datetime as an ISO-8601 UTC string (e.g. 2026-01-05T00:00:00Z)."""
        return cls._to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _window_bounds(
        self, context: Optional[dict], default_end: datetime
    ) -> Tuple[datetime, datetime]:
        """Compute the overall (start, end) datetimes for this run.

        The start is the incremental high-water mark from state (the previous
        job's ``hg_synced_at``), or the configured ``start_date`` on the first run.
        The end defaults to the job's ``sync_started_at`` (passed in) unless
        ``end_date`` is configured.
        """
        start_dt = self.get_starting_time(context)
        if start_dt is None:
            start_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)
        start_dt = self._to_utc(start_dt)

        end_value = self.config.get("end_date")
        end_dt = (
            self._to_utc(pendulum.parse(end_value))
            if end_value
            else self._to_utc(default_end)
        )
        return start_dt, end_dt

    @staticmethod
    def _extract_id(obj: Any, field: str) -> Optional[Any]:
        """Read ``field`` out of a customer/site object (or the value itself)."""
        if isinstance(obj, dict):
            value = obj.get(field)
        else:
            value = obj
        return value if value not in (None, "") else None

    def _iter_customer_ids(self) -> Iterable[str]:
        """Yield customer ids from config, or discover them via getCustomers."""
        configured = self.config.get("customer_ids")
        if configured:
            for cid in configured:
                yield str(cid)
            return

        customers = self.call_soap("getCustomers", [self.username, self.password])
        for customer in customers:
            cid = self._extract_id(customer, CUSTOMER_ID_FIELD)
            if cid is None:
                self.logger.warning(
                    "Could not determine a customer id from record: %s", customer
                )
                continue
            yield str(cid)

    def _iter_site_ids(self, customer_id: str) -> Iterable[str]:
        """Yield site ids from config, or discover them via getSiteList."""
        configured = self.config.get("site_ids")
        if configured:
            for sid in configured:
                yield str(sid)
            return

        sites = self.call_soap(
            "getSiteList", [self.username, self.password, customer_id]
        )
        for site in sites:
            sid = self._extract_id(site, SITE_ID_FIELD)
            if sid is None:
                self.logger.warning(
                    "Could not determine a site id from record (customer=%s): %s",
                    customer_id,
                    site,
                )
                continue
            yield str(sid)

    def _ping(self) -> None:
        """Best-effort connectivity check; logs but never raises."""
        try:
            self.call_soap("ping", [])
            self.logger.info("Capturis ping successful")
        except Exception as e:  # noqa: BLE001 - ping is best-effort validation
            self.logger.warning("Capturis ping failed (continuing): %s", e)

    def _iter_bill_batches(
        self, context: Optional[dict], default_end: datetime
    ) -> Iterable[Tuple[str, str, List[dict]]]:
        """Walk customers -> sites, yielding ``(customer_id, site_id, bills)`` batches.

        Each site's ``getModifiedBillsBySiteForCustomer`` response is treated as a
        single batch (analogous to a page of records), which lets callers download
        that batch's attachments together before emitting it.

        ``getModifiedBillsBySiteForCustomer`` takes ``xs:dateTime`` window bounds,
        so full ISO timestamps are sent (not date-only) to avoid re-syncing from
        the start of the day on incremental runs.
        """
        start_dt, end_dt = self._window_bounds(context, default_end)
        start_arg = self._to_iso_z(start_dt)
        end_arg = self._to_iso_z(end_dt)
        self.logger.info("Bills window %s..%s", start_arg, end_arg)

        for customer_id in self._iter_customer_ids():
            for site_id in self._iter_site_ids(customer_id):
                raw_bills = self.call_soap(
                    "getModifiedBillsBySiteForCustomer",
                    [
                        self.username,
                        self.password,
                        customer_id,
                        site_id,
                        start_arg,
                        end_arg,
                    ],
                )
                self.logger.info(
                    "customer=%s site=%s bills=%s",
                    customer_id,
                    site_id,
                    len(raw_bills),
                )
                bills = [
                    b if isinstance(b, dict) else {"value": b} for b in raw_bills
                ]
                yield str(customer_id), str(site_id), bills

    def get_sync_output_folder(self) -> str:
        """Return the base folder under which output files (attachments) are written.

        In the hotglue runtime ``JOB_ID`` is set and files are uploaded from
        ``/home/hotglue/{job_id}/sync-output``. Locally we fall back to a
        ``sync-output`` directory (overridable via the ``output_dir`` config).
        """
        job_id = os.environ.get("JOB_ID")
        if job_id:
            return f"/home/hotglue/{job_id}/sync-output"
        return self.config.get("output_dir", "sync-output")
