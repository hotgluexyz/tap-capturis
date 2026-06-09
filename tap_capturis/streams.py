"""Stream type classes for tap-capturis."""

import base64
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from hotglue_singer_sdk import typing as th  # JSON Schema typing helpers

from tap_capturis.client import CapturisStream

logger = logging.getLogger(__name__)

# Fields that Capturis returns as a single object when there is exactly one
# item, but as a list when there are several (standard SOAP repeated-element
# behavior). They are normalized to always be lists to match the schema.
LIST_FIELDS = ("additionalCharges", "invoiceDetails")

# Fixed positional args for the ``getFile`` SOAP operation, taken from the
# working example in the Capturis ticket (HGI-10330):
#   getFile(username, password, customerId, arg3=1, arg4=0, invoiceNbr)
# The semantics of arg3/arg4 are undocumented; these values return the bill PDF.
GETFILE_ARG3 = 1
GETFILE_ARG4 = 0

# Maps leading "magic" bytes of a downloaded attachment to its file extension.
# Bills are PDFs in practice, so that's the fallback.
_FILE_SIGNATURES = (
    (b"%PDF", ".pdf"),
    (b"\x89PNG\r\n\x1a\n", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"II*\x00", ".tiff"),
    (b"MM\x00*", ".tiff"),
    (b"PK\x03\x04", ".zip"),
)
_DEFAULT_EXTENSION = ".pdf"


class BillsStream(CapturisStream):
    """Bills (invoices) stream.

    Capturis exposes bills hierarchically, so this stream walks the documented
    call chain to produce one record per bill::

        ping -> getCustomers -> (per customer) getSiteList
              -> (per site, date-windowed) getModifiedBillsBySiteForCustomer

    ``customer_ids`` and/or ``site_ids`` may be supplied in config to short-circuit
    the discovery calls (useful for testing against a known customer/site).
    """

    name = "bills"
    primary_keys = ["invoiceNbr"]
    # Capturis only supports a server-side *modified-date window* (no per-record
    # modified timestamp is returned), so incremental replication is job-based:
    # a single sync_started_at timestamp is captured at the start of the run and
    # stamped on every record as ``hg_synced_at``, which is used as the replication
    # key / high-water mark. The next run then pulls everything modified since the
    # previous job started.
    replication_key = "hg_synced_at"

    schema = th.PropertiesList(
        th.Property("invoiceNbr", th.StringType),
        th.Property("accountNbr", th.StringType),
        th.Property("accountType", th.StringType),
        th.Property("accountId", th.StringType),
        th.Property("balanceForward", th.StringType),
        th.Property("budgetBilling", th.StringType),
        th.Property("cancelDocId", th.StringType),
        th.Property("currencyCd", th.StringType),
        th.Property("customerName", th.StringType),
        th.Property("dueDt", th.DateTimeType),
        th.Property("invoiceUpdated", th.StringType),
        th.Property("lateFee", th.StringType),
        th.Property("providerFee", th.StringType),
        th.Property("providerFeePct", th.StringType),
        th.Property("totalDue", th.StringType),
        th.Property("utilityInvoiceNbr", th.StringType),
        th.Property("imageKey", th.StringType),
        th.Property("vendorNbr", th.StringType),
        th.Property("vendorNm", th.StringType),
        th.Property("vendorAddr1", th.StringType),
        th.Property("vendorAddr2", th.StringType),
        th.Property("vendorCity", th.StringType),
        th.Property("vendorState", th.StringType),
        th.Property("vendorCountry", th.StringType),
        th.Property("vendorPhoneNbr", th.StringType),
        th.Property("vendorFaxNbr", th.StringType),
        th.Property(
            "additionalCharges",
            th.ArrayType(
                th.ObjectType(
                    th.Property("amount", th.StringType),
                    th.Property("invoiceNbr", th.StringType),
                    th.Property("rateClass", th.StringType),
                    th.Property("rateClassDescription", th.StringType),
                    th.Property("service", th.StringType),
                    th.Property("seqNbr", th.StringType),
                    additional_properties=th.CustomType({}),
                )
            ),
        ),
        th.Property(
            "invoiceDetails",
            th.ArrayType(
                th.ObjectType(
                    th.Property("customerNbr", th.StringType),
                    th.Property("accountNbr", th.StringType),
                    th.Property("accountId", th.StringType),
                    th.Property("invoiceNbr", th.StringType),
                    th.Property("siteId", th.StringType),
                    th.Property("vendorNbr", th.StringType),
                    th.Property("seqNo", th.StringType),
                    th.Property("service", th.StringType),
                    th.Property("subService", th.StringType),
                    th.Property("serviceLocation", th.StringType),
                    th.Property("rateClass", th.StringType),
                    th.Property("rateClassDescription", th.StringType),
                    th.Property("measurement", th.StringType),
                    th.Property("meterNbr", th.StringType),
                    th.Property("multiplier", th.StringType),
                    th.Property("actualUsage", th.StringType),
                    th.Property("amount", th.StringType),
                    th.Property("previousEstimate", th.StringType),
                    th.Property("previousReading", th.StringType),
                    th.Property("currentEstimate", th.StringType),
                    th.Property("currentReading", th.StringType),
                    th.Property("thermFactor", th.StringType),
                    th.Property("btuFactor", th.StringType),
                    th.Property("shouldAccumUsage", th.StringType),
                    th.Property("fromDate", th.DateTimeType),
                    th.Property("toDate", th.DateTimeType),
                    th.Property("checkDate", th.DateTimeType),
                    th.Property("period", th.StringType),
                    th.Property("serviceCost", th.StringType),
                    th.Property("optimizedServiceCost", th.StringType),
                    th.Property("demand", th.StringType),
                    th.Property("timeOfUse", th.StringType),
                    additional_properties=th.CustomType({}),
                )
            ),
        ),
        th.Property("customerId", th.StringType),
        th.Property("siteId", th.StringType),
        # Path (relative to the sync-output folder) of this bill's downloaded
        # document, e.g. "bill_attachments/<invoiceNbr>/<invoiceNbr>.pdf", or ""
        # if no file was written.
        th.Property("bill_attachment_path", th.StringType),
        th.Property("hg_synced_at", th.DateTimeType),
    ).to_dict()
    # Utility bills vary by service type (electric/gas/water), so allow any
    # additional fields the API returns to pass through untouched.
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Walk customers -> sites -> bills and emit one record per bill.

        Each site's bills are treated as a batch: when attachment downloading is
        enabled, every bill's document is fetched (via ``getFile``) and written
        to disk as ``bill_attachments/<invoiceNbr>/<invoiceNbr>.<ext>`` under the
        sync-output folder, and each bill record references its file via
        ``bill_attachment_path``.

        A single ``sync_started_at`` timestamp is captured at the start of the job
        and stamped on every record as ``hg_synced_at``. The SDK uses that field
        (the replication key) as the high-water mark, so the next run pulls
        everything modified since this job started.
        """
        sync_started_at = datetime.now(timezone.utc)
        hg_synced_at = self._to_iso_z(sync_started_at)
        download_attachments = self.config.get("download_attachments", True)
        self._ping()
        logger.info(
            "Incremental bills sync (sync_started_at=%s, download_attachments=%s)",
            hg_synced_at,
            download_attachments,
        )

        for customer_id, site_id, bills in self._iter_bill_batches(
            context, sync_started_at
        ):
            for bill in bills:
                bill.setdefault("customerId", customer_id)
                bill.setdefault("siteId", site_id)
                bill["hg_synced_at"] = hg_synced_at
                self._normalize_list_fields(bill)

            self._annotate_batch_attachments(
                customer_id, bills, download_attachments
            )

            for bill in bills:
                yield bill

    @staticmethod
    def _normalize_list_fields(bill: dict) -> dict:
        """Coerce single-object list fields (e.g. additionalCharges) into lists."""
        for field in LIST_FIELDS:
            value = bill.get(field)
            if isinstance(value, dict):
                bill[field] = [value]
        return bill

    # ------------------------------------------------------------------
    # Attachment downloading: write each bill's file to disk under
    # bill_attachments/<invoiceNbr>/<invoiceNbr>.<ext>
    # ------------------------------------------------------------------

    @staticmethod
    def _extension_from_content(content: bytes) -> str:
        """Return a file extension based on the file's magic bytes (default .pdf)."""
        for signature, ext in _FILE_SIGNATURES:
            if content.startswith(signature):
                return ext
        return _DEFAULT_EXTENSION

    def _download_bill_file(
        self, customer_id: str, invoice_nbr: str
    ) -> Optional[str]:
        """Download one bill's file and write it to disk.

        Returns the path (relative to the sync-output folder) of the written file,
        or ``None`` when no file content is available for the bill.
        """
        try:
            returns = self.call_soap(
                "getFile",
                [
                    self.username,
                    self.password,
                    customer_id,
                    GETFILE_ARG3,
                    GETFILE_ARG4,
                    invoice_nbr,
                ],
            )
        except Exception as e:  # noqa: BLE001 - one bad file shouldn't fail the batch
            logger.warning("Failed to download file for invoice %s: %s", invoice_nbr, e)
            return None

        b64 = returns[0] if returns else None
        if not isinstance(b64, str) or not b64.strip():
            return None
        try:
            content = base64.b64decode(b64)
        except Exception as e:  # noqa: BLE001 - guard against malformed payloads
            logger.warning(
                "Could not base64-decode file for invoice %s: %s", invoice_nbr, e
            )
            return None
        if not content:
            return None

        file_name = f"{invoice_nbr}{self._extension_from_content(content)}"
        out_dir = Path(self.get_sync_output_folder()) / "bill_attachments" / invoice_nbr
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / file_name
        out_path.write_bytes(content)
        logger.info("Saved attachment for invoice %s -> %s", invoice_nbr, out_path)
        return f"bill_attachments/{invoice_nbr}/{file_name}"

    def _annotate_batch_attachments(
        self,
        customer_id: str,
        bills: List[dict],
        download_attachments: bool,
    ) -> None:
        """Download this batch's attachments and stamp ``bill_attachment_path``."""
        if not download_attachments:
            for bill in bills:
                bill["bill_attachment_path"] = ""
            return

        items = [
            (customer_id, str(bill["invoiceNbr"]))
            for bill in bills
            if bill.get("invoiceNbr")
        ]
        paths: Dict[str, str] = {}

        def download(item: Tuple[str, str]) -> None:
            cid, invoice_nbr = item
            rel_path = self._download_bill_file(cid, invoice_nbr)
            if rel_path:
                paths[invoice_nbr] = rel_path

        if items:
            dl_workers = self.config.get("attachment_download_parallelism", 5)
            logger.info("Downloading %s bill attachment(s)...", len(items))
            with ThreadPoolExecutor(max_workers=dl_workers) as executor:
                list(executor.map(download, items))

        for bill in bills:
            invoice_nbr = str(bill.get("invoiceNbr") or "")
            bill["bill_attachment_path"] = paths.get(invoice_nbr, "")
