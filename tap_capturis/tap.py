"""Capturis tap class."""

from typing import List

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_capturis.streams import BillsStream

STREAM_TYPES = [
    BillsStream,
]


class TapCapturis(Tap):
    """Capturis tap class."""

    name = "tap-capturis"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property("username", th.StringType, required=True),
        th.Property("password", th.StringType, required=True),
        th.Property(
            "api_url",
            th.StringType,
            default="https://portal.capturis.com/ubp-ws/v4/BillingInfo",
            description="Capturis BillingInfo SOAP endpoint.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2000-01-01T00:00:00.000Z",
            description="Start of the bill date window (passed to getModifiedBillsBySiteForCustomer).",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="Optional end of the bill date window. Defaults to today (UTC).",
        ),
        th.Property(
            "customer_ids",
            th.ArrayType(th.StringType),
            description="Optional. Restrict to these customer ids and skip the getCustomers discovery call.",
        ),
        th.Property(
            "site_ids",
            th.ArrayType(th.StringType),
            description="Optional. Restrict to these site ids and skip the getSiteList discovery call.",
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=300,
            description="HTTP request timeout in seconds.",
        ),
        th.Property(
            "output_dir",
            th.StringType,
            default="sync-output",
            description=(
                "Local base folder for downloaded bill attachments. Ignored in the "
                "hotglue runtime, where files are written under "
                "/home/hotglue/{JOB_ID}/sync-output."
            ),
        ),
        th.Property(
            "download_attachments",
            th.BooleanType,
            default=True,
            description=(
                "Whether to download each bill's PDF (via getFile) to "
                "bill_attachments/<invoiceNbr>/. Disable to sync bill data only."
            ),
        ),
        th.Property(
            "attachment_download_parallelism",
            th.IntegerType,
            default=5,
            description="Number of concurrent getFile downloads per site batch.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapCapturis.cli()
