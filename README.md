# Carfolio support site

Static GitHub Pages source for Carfolio's public privacy policy, subscription
terms, and customer support pages in English, Hebrew, and Dutch.

The intended public repository name is `carfolio-support`. GitHub Pages should
publish from the repository root on the default branch.

Before publishing, verify the repository owner and replace any owner-specific
GitHub Issues URLs if the authenticated account differs from
`sabaaziz991-hash`.

## Israel vehicle-data resilience

`scripts/build_israel_vehicle_snapshot.py` streams both official Ministry of
Transport registries—the private/commercial registry up to 3,500 kg and the
heavy/code-less registry—and publishes only the fields used by Carfolio as
mixed, zlib-compressed plate-prefix JSON shards under
`data/israel-vehicles/v2/`.

The iOS lookup order is:

1. Live `data.gov.il` CKAN lookup.
2. The bill-free official-data asset from the repository's latest GitHub
   Release for the matching two-digit plate prefix when the relevant live
   provider is unavailable.
3. A previously saved vehicle record already stored on the user's device.

Each v2 shard contains separate `passengerRecords` and `heavyRecords` arrays,
so the app performs one static request and can resolve either registry even
when all data.gov.il lookup endpoints are unavailable. Records use ordered
arrays and zlib compression to avoid repeating the Ministry's long field names
millions of times. There is no Cloudflare Worker, database, metered request
service, or Cloudflare billing account in this path.

The snapshot does not replace the Ministry as the source. Its manifest records
the official resource ID, source URL, generation time, record count, and schema
version. Production builds require at least 3,000,000 passenger records and
300,000 heavy records. A download, schema, count, or 900 MB output-ceiling
failure prevents publication, leaving the previous release marked latest.

The data.gov.il license permits copying, redistribution, technical changes,
derivative products, and commercial use. The generated manifest retains the
required Ministry/data.gov.il attribution and license URL. The app must still
label the snapshot by its generation date and must not imply state endorsement.

The data snapshot is deliberately published as GitHub Release assets, not on
GitHub Pages: Pages has a 1 GB site limit and is not intended to be an app data
backend. GitHub documents releases as the distribution path for large files,
with up to 1,000 assets per release and no stated total-size or bandwidth
quota. The repository must stay public and the workflow must stay on a
standard runner so GitHub Actions remains free. Do not commit the generated
dataset to Git history. The workflow publishes a draft transactionally, marks
it latest only after every shard uploads, and retains the newest two releases.
