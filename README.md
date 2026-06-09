# tap-capturis

`tap-capturis` is a Singer tap for the **Capturis** (NISC) `BillingInfo` SOAP web
service, built with the Meltano SDK for Singer Taps.

Capturis does **not** expose a REST or OAuth API. It is a SOAP 1.1 service where
the username and password are passed as positional arguments on every call.

## Installation

Install using Poetry:

```bash
poetry install
```

Or install from source:

```bash
pip install -e .
```

## Configuration

```json
{
  "username": "you@example.com",
  "password": "your-password",
  "api_url": "https://portal.capturis.com/ubp-ws/v4/BillingInfo",
  "start_date": "2024-01-01T00:00:00.000Z",
  "end_date": "2024-12-31T00:00:00.000Z",
  "customer_ids": ["279"],
  "site_ids": ["6070"]
}
```

### Configuration options

- `username` (required): Capturis API username.
- `password` (required): Capturis API password.
- `api_url` (optional): SOAP endpoint, defaults to `https://portal.capturis.com/ubp-ws/v4/BillingInfo`.
- `start_date` (optional): Start of the bill date window, defaults to `2000-01-01T00:00:00.000Z`.
- `end_date` (optional): End of the bill date window. Defaults to today (UTC).
- `customer_ids` (optional): If set, restricts the sync to these customer ids and
  skips the `getCustomers` discovery call.
- `site_ids` (optional): If set, restricts the sync to these site ids and skips
  the `getSiteList` discovery call.
- `request_timeout` (optional): HTTP timeout in seconds, defaults to `300`.
- `output_dir` (optional): Local base folder for downloaded bill attachments,
  defaults to `sync-output`. Ignored in the hotglue runtime, where files are
  written under `/home/hotglue/{JOB_ID}/sync-output`.

## Streams

### bills

Fetches bills (invoices) by walking the documented Capturis call hierarchy:

```
ping
  -> getCustomers
       -> getSiteList (per customer)
            -> getModifiedBillsBySiteForCustomer (per site, date-windowed)
```

- `ping` checks the service is reachable (it takes no credentials; best-effort —
  a failure is logged but does not abort the sync).
- `getCustomers` discovers customer ids (skipped if `customer_ids` is set).
- `getSiteList` discovers site ids per customer (skipped if `site_ids` is set).
- `getModifiedBillsBySiteForCustomer` returns the bills for each site within the
  `start_date`..`end_date` window.

Each emitted bill record is augmented with `customerId` and `siteId` for context.
When a bill changes, Capturis re-sends the entire bill payload (header + all line
items + charges).

**Replication method**: Incremental (window-based — see below).

**Primary key**: `invoiceNbr`

### Bill attachments (PDFs)

Bill PDFs are downloaded as part of the `bills` stream (not a separate stream).
For every bill, the `getFile` SOAP operation is called:

```
getFile(username, password, customerId, 1, 0, invoiceNbr) -> base64 file
```

(`getFile` is keyed by `customerId` + `invoiceNbr`; `siteId` is not needed. The
two fixed `1`/`0` arguments are undocumented but match the working example from
the integration research and return the bill PDF.)

The base64 payload is decoded and written as a raw file under the sync output
folder:

```
<output_dir>/bill_attachments/<invoiceNbr>/<invoiceNbr>.<ext>
```

The extension is derived from the file's magic bytes (PDF in practice, with
PNG/JPEG/TIFF/ZIP also recognized; falls back to `.pdf`). Downloads within a
site's batch of bills run in parallel.

Every bill record carries a **`bill_attachment_path`** field set to the file's
path relative to the sync output folder (or `""` if no file was written for that
bill), so downstream consumers can locate the document.

Relevant config:

- `download_attachments` (default `true`): set to `false` to sync bill data only.
- `attachment_download_parallelism` (default `5`): concurrent `getFile` calls.

## Incremental replication

Capturis only supports a server-side **modified-date window**
(`getModifiedBillsBySiteForCustomer` takes a start and end date), and the bill
records contain **no per-record modified timestamp** (only `invoiceUpdated`, a
boolean, and business dates like `dueDt`). So per-record incremental bookmarking
isn't possible — instead the tap uses a **job-scoped high-water mark**:

1. At the start of each sync the tap captures a single `sync_started_at`
   timestamp and stamps it on **every** record in the job as `hg_synced_at`. This
   field is the stream's `replication_key`.
2. The SDK advances state to that `hg_synced_at` value, so the next run pulls
   everything modified since the previous job **started** (the window start is the
   previous `hg_synced_at`, falling back to `start_date` on the first run). The
   window end is `sync_started_at`.
Re-pulling overlapping bills is **safe and idempotent** because bills upsert on
the `invoiceNbr` primary key, and Capturis re-sends the entire bill payload
whenever it changes.

Each run issues a single window (`[start, end]`) per customer/site, since that is
the only filter the API supports. The bounds are sent as full ISO-8601 timestamps
(the WSDL types `getModifiedBillsBySiteForCustomer`'s date args as `xs:dateTime`),
so incremental runs resume from the exact `sync_started_at` instant rather than
the start of the day.

To force a full re-sync, run without `--state` (or clear the bookmark).

> Because the window is applied uniformly across all customers/sites in a run,
> the high-water mark is per stream. If you add a new customer/site later, run a
> one-off backfill for it (e.g. with `customer_ids`/`site_ids` and an earlier
> `start_date`) since it will otherwise start from the shared bookmark.

> **Note on the schema:** the full bill payload is not formally documented, so the
> `bills` schema declares the known identifiers (`invoiceNbr`, `customerId`,
> `siteId`) and allows all additional fields returned by the API to pass through
> (`additionalProperties: true`). During discovery, customer/site ids are read
> from `customerNbr` (getCustomers) and `siteId` (getSiteList).

## Usage

### Discover

```bash
tap-capturis --config .secrets/config.json --discover
```

### Sync

```bash
tap-capturis --config .secrets/config.json
```

```bash
tap-capturis --config .secrets/config.json --catalog catalog.json --state state.json
```

## Authentication

Username/password are sent as SOAP arguments on each request (no token exchange).

Capturis has no native 401: invalid credentials come back as an HTTP 500 with a
generic SOAP `<Fault>` (`faultstring="Fault occurred while processing."`). The tap
detects any SOAP Fault (even in a malformed body) and raises a non-retriable
`InvalidCredentialsError` rather than retrying it as a server error. Note that
Capturis reuses this same generic fault for other server-side errors, so it
cannot be distinguished from an auth failure by the fault alone.

## Not yet implemented

This tap implements a single `bills` stream (with bill PDFs downloaded inline —
see above). The research for this integration also identified `customers`,
`sites`, and deleted bills (`getDeletedBills`) as candidate streams for later.
