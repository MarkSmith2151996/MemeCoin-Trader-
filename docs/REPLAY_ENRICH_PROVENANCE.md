# Replay Enrichment Provenance

`scripts/enrich.py` is a byte-for-byte snapshot of the deployed enrichment worker found at `/mnt/d/pumpapi-replay/enrich.py` on 2026-09-11 during MT-738.

Deployed/source SHA256: `bde24fccd7ad199ba54598b46b95319cb5c5cf5f046c0d76f432855d1c0ac8e7`.

No prior repository version of an `enrich.py` worker exists, so there is no repository diff to record. The snapshot is committed solely to make the current 126-file enrichment pipeline reproducible; it does not alter the deployed worker or replay data.
