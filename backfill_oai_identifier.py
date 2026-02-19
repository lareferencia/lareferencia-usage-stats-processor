#!/usr/bin/env python3
"""
Offline backfill helper for Matomo custom_var_v1 (OAI identifier).

Input 1: events CSV/TSV with at least:
  - idsite
  - idaction_url
  - action_url
  - optional custom_var_v1

Input 2: prefix map CSV/TSV with:
  - site_id
  - identifier_prefix

Output:
  - Full audit CSV with status per row
  - Deduplicated updates CSV by (idsite, idaction_url)
  - Unresolved rows CSV
  - Missing-prefix sites CSV (deduplicated by idsite)
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from urllib.parse import unquote


HANDLE_PATTERNS = [
    # e.g. https://hdl.handle.net/10810/68868
    re.compile(r"(?:https?://)?(?:hdl\.handle\.net|handle\.net)/([0-9]+/[A-Za-z0-9._-]+)", re.IGNORECASE),
    # e.g. .../bitstream/handle/10810/68868/file.pdf
    re.compile(r"/bitstream/(?:handle/)?([0-9]+/[A-Za-z0-9._-]+)", re.IGNORECASE),
    # e.g. .../handle/10810/68868
    re.compile(r"/handle/([0-9]+/[A-Za-z0-9._-]+)", re.IGNORECASE),
]

OAI_IDENTIFIER_PATTERN = re.compile(r"^(oai:)(?:https?://)?([^/]+)(?:/[^:]+)*(:.*)$", re.IGNORECASE)


def normalize_site_id(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return str(int(float(text)))
    except ValueError:
        return text


def normalize_prefix(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    text = text.rstrip(":/")
    if text.lower().startswith("oai:"):
        text = "oai:" + text[4:]
    else:
        text = "oai:" + text
    return text


def normalize_oai_identifier(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    decoded = unquote(text)
    match = OAI_IDENTIFIER_PATTERN.match(decoded)
    if match:
        return match.group(1).lower() + match.group(2) + match.group(3)
    return decoded


def extract_handle(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None

    decoded = unquote(text)
    for pattern in HANDLE_PATTERNS:
        match = pattern.search(decoded)
        if match:
            return match.group(1)
    return None


def open_reader(path, delimiter):
    handle = open(path, "r", newline="", encoding="utf-8")
    return handle, csv.DictReader(handle, delimiter=delimiter)


def validate_columns(reader, path, required_columns):
    missing = [column for column in required_columns if column not in reader.fieldnames]
    if missing:
        raise ValueError(
            "Missing required columns in %s: %s. Available columns: %s"
            % (path, ", ".join(missing), ", ".join(reader.fieldnames or []))
        )


def load_prefix_map(path, delimiter, site_column, prefix_column):
    fh, reader = open_reader(path, delimiter)
    try:
        validate_columns(reader, path, [site_column, prefix_column])
        prefixes = {}
        duplicates = 0
        for row in reader:
            site_id = normalize_site_id(row.get(site_column))
            prefix = normalize_prefix(row.get(prefix_column))
            if site_id is None or prefix is None:
                continue
            if site_id in prefixes and prefixes[site_id] != prefix:
                duplicates += 1
            prefixes[site_id] = prefix
        return prefixes, duplicates
    finally:
        fh.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Offline reconstruction of OAI identifiers for Matomo rows with missing custom_var_v1."
    )
    parser.add_argument("--events", required=True, help="events file (CSV/TSV)")
    parser.add_argument("--prefix-map", required=True, help="prefix map file (CSV/TSV)")

    parser.add_argument("--events-sep", default=",", help="events delimiter (default: ,)")
    parser.add_argument("--prefix-sep", default=",", help="prefix map delimiter (default: ,)")

    parser.add_argument("--events-site-col", default="idsite", help="events site column")
    parser.add_argument("--events-idaction-col", default="idaction_url", help="events idaction_url column")
    parser.add_argument("--events-action-url-col", default="action_url", help="events action_url column")
    parser.add_argument(
        "--events-current-oai-col",
        default="custom_var_v1",
        help="events current OAI column (optional, default: custom_var_v1)",
    )

    parser.add_argument("--prefix-site-col", default="site_id", help="prefix map site column")
    parser.add_argument("--prefix-value-col", default="identifier_prefix", help="prefix map identifier prefix column")

    parser.add_argument("--out-all", default="backfill_oai_audit.csv", help="full audit output file")
    parser.add_argument(
        "--out-updates",
        default="backfill_oai_updates_by_action.csv",
        help="deduplicated updates output by (idsite,idaction_url)",
    )
    parser.add_argument("--out-unresolved", default="backfill_oai_unresolved.csv", help="unresolved rows output file")
    parser.add_argument(
        "--out-missing-prefix-sites",
        default="backfill_missing_prefix_sites.csv",
        help="missing prefix sites output file (deduplicated by idsite)",
    )
    parser.add_argument("--dry-run", action="store_true", help="compute and print summary only")
    return parser.parse_args()


def main():
    args = parse_args()

    prefix_map, conflicting_prefixes = load_prefix_map(
        args.prefix_map,
        args.prefix_sep,
        args.prefix_site_col,
        args.prefix_value_col,
    )

    print("Loaded prefix map entries:", len(prefix_map))
    if conflicting_prefixes > 0:
        print("Warning: conflicting prefixes found for same site_id:", conflicting_prefixes)

    events_fh, events_reader = open_reader(args.events, args.events_sep)
    try:
        validate_columns(
            events_reader,
            args.events,
            [args.events_site_col, args.events_idaction_col, args.events_action_url_col],
        )

        action_values = defaultdict(set)
        total_rows = 0
        for row in events_reader:
            total_rows += 1
            site_id = normalize_site_id(row.get(args.events_site_col))
            idaction = str(row.get(args.events_idaction_col, "")).strip()
            action_url = row.get(args.events_action_url_col)
            prefix = prefix_map.get(site_id)
            handle = extract_handle(action_url)
            if site_id and idaction and prefix and handle:
                action_values[(site_id, idaction)].add("%s:%s" % (prefix, handle))
    finally:
        events_fh.close()

    ambiguous_action_keys = {key for key, values in action_values.items() if len(values) > 1}

    print("Events scanned:", total_rows)
    print("Action keys with candidate mapping:", len(action_values))
    print("Ambiguous action keys:", len(ambiguous_action_keys))

    events_fh, events_reader = open_reader(args.events, args.events_sep)
    out_all_fh = None
    out_unresolved_fh = None
    out_updates_fh = None
    out_missing_prefix_sites_fh = None
    all_writer = None
    unresolved_writer = None
    updates_writer = None
    missing_prefix_sites_writer = None
    try:
        validate_columns(
            events_reader,
            args.events,
            [args.events_site_col, args.events_idaction_col, args.events_action_url_col],
        )

        if not args.dry_run:
            all_fieldnames = list(events_reader.fieldnames or []) + [
                "prefix_norm",
                "handle",
                "reconstructed_oai_identifier",
                "status",
            ]
            unresolved_fieldnames = list(events_reader.fieldnames or []) + [
                "prefix_norm",
                "handle",
                "status",
            ]
            updates_fieldnames = ["idsite", "idaction_url", "reconstructed_oai_identifier", "rows_to_update"]
            missing_prefix_sites_fieldnames = [
                "idsite",
                "rows_missing_prefix",
                "distinct_idaction_url",
            ]

            out_all_fh = open(args.out_all, "w", newline="", encoding="utf-8")
            out_unresolved_fh = open(args.out_unresolved, "w", newline="", encoding="utf-8")
            out_updates_fh = open(args.out_updates, "w", newline="", encoding="utf-8")
            out_missing_prefix_sites_fh = open(args.out_missing_prefix_sites, "w", newline="", encoding="utf-8")

            all_writer = csv.DictWriter(out_all_fh, fieldnames=all_fieldnames)
            unresolved_writer = csv.DictWriter(out_unresolved_fh, fieldnames=unresolved_fieldnames)
            updates_writer = csv.DictWriter(out_updates_fh, fieldnames=updates_fieldnames)
            missing_prefix_sites_writer = csv.DictWriter(
                out_missing_prefix_sites_fh, fieldnames=missing_prefix_sites_fieldnames
            )
            all_writer.writeheader()
            unresolved_writer.writeheader()
            updates_writer.writeheader()
            missing_prefix_sites_writer.writeheader()

        summary = defaultdict(int)
        updates_counter = defaultdict(int)
        missing_prefix_sites = defaultdict(lambda: {"rows_missing_prefix": 0, "idactions": set()})

        for row in events_reader:
            site_id = normalize_site_id(row.get(args.events_site_col))
            idaction = str(row.get(args.events_idaction_col, "")).strip()
            action_url = row.get(args.events_action_url_col)
            current_identifier = row.get(args.events_current_oai_col)

            prefix = prefix_map.get(site_id)
            handle = extract_handle(action_url)
            reconstructed = "%s:%s" % (prefix, handle) if prefix and handle else None
            action_key = (site_id, idaction)

            current_norm = normalize_oai_identifier(current_identifier)
            reconstructed_norm = normalize_oai_identifier(reconstructed)

            if current_norm:
                if reconstructed_norm and current_norm != reconstructed_norm:
                    status = "existing_mismatch"
                else:
                    status = "already_present"
            elif prefix is None:
                status = "missing_prefix"
            elif handle is None:
                status = "unresolved_url"
            elif action_key in ambiguous_action_keys:
                status = "ambiguous_action_mapping"
            else:
                status = "reconstructed"

            summary[status] += 1

            if status == "reconstructed":
                updates_counter[(site_id, idaction, reconstructed_norm)] += 1
            elif status == "missing_prefix":
                site_key = site_id if site_id is not None else ""
                missing_prefix_sites[site_key]["rows_missing_prefix"] += 1
                if idaction:
                    missing_prefix_sites[site_key]["idactions"].add(idaction)

            if not args.dry_run:
                all_row = dict(row)
                all_row["prefix_norm"] = prefix or ""
                all_row["handle"] = handle or ""
                all_row["reconstructed_oai_identifier"] = reconstructed_norm or ""
                all_row["status"] = status
                all_writer.writerow(all_row)

                if status not in ("reconstructed", "already_present"):
                    unresolved_row = dict(row)
                    unresolved_row["prefix_norm"] = prefix or ""
                    unresolved_row["handle"] = handle or ""
                    unresolved_row["status"] = status
                    unresolved_writer.writerow(unresolved_row)

        if not args.dry_run:
            for (site_id, idaction, reconstructed_identifier), rows_count in updates_counter.items():
                updates_writer.writerow(
                    {
                        "idsite": site_id,
                        "idaction_url": idaction,
                        "reconstructed_oai_identifier": reconstructed_identifier,
                        "rows_to_update": rows_count,
                    }
                )

            def sort_key(value):
                try:
                    return (0, int(value))
                except Exception:
                    return (1, str(value))

            for site_id in sorted(missing_prefix_sites.keys(), key=sort_key):
                info = missing_prefix_sites[site_id]
                missing_prefix_sites_writer.writerow(
                    {
                        "idsite": site_id,
                        "rows_missing_prefix": info["rows_missing_prefix"],
                        "distinct_idaction_url": len(info["idactions"]),
                    }
                )

        print("Summary:")
        for key in [
            "reconstructed",
            "already_present",
            "existing_mismatch",
            "missing_prefix",
            "unresolved_url",
            "ambiguous_action_mapping",
        ]:
            print("  %s: %s" % (key, summary[key]))
        print("  missing_prefix_sites:", len(missing_prefix_sites))

        if not args.dry_run:
            print("Wrote full audit:", args.out_all)
            print("Wrote unresolved rows:", args.out_unresolved)
            print("Wrote updates by action:", args.out_updates)
            print("Wrote missing-prefix sites:", args.out_missing_prefix_sites)
        else:
            print("Dry run: no output files written.")

    finally:
        events_fh.close()
        if out_all_fh:
            out_all_fh.close()
        if out_unresolved_fh:
            out_unresolved_fh.close()
        if out_updates_fh:
            out_updates_fh.close()
        if out_missing_prefix_sites_fh:
            out_missing_prefix_sites_fh.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
