#!/usr/bin/env python3
import os
import sys
import argparse
import paramiko
from pathlib import Path
from astropy.table import Table

SFTP_HOST = "nillmill.ddns.net"
SFTP_USER = "PRIMVS"
SFTP_PASS = "isitluck"     #whatever store the password like this
REMOTE_LC_DIR = "/light_curves"


def resolve_remote_path(source_id):
    source_str = str(int(source_id))
    subdir1 = source_str[:3]
    subdir2 = source_str[3:6]
    return f"{REMOTE_LC_DIR}/{subdir1}/{subdir2}/{source_str}.csv"


def resolve_local_path(source_id, output_dir):
    source_str = str(int(source_id))
    subdir1 = source_str[:3]
    subdir2 = source_str[3:6]
    return Path(output_dir) / subdir1 / subdir2 / f"{source_str}.csv"


def download_lightcurves(input_fits, id_column="sourceid", output_dir="./light_curves", hdu=1):

    tbl = Table.read(input_fits, hdu=hdu)
    if id_column not in tbl.colnames:
        print(f"Error: Column '{id_column}' not found. Available: {tbl.colnames}")
        sys.exit(1)

    source_ids = tbl[id_column].data
    print(f"Found {len(source_ids)} source IDs in {input_fits}")

    to_download = []
    already_exist = 0
    for sid in source_ids:
        local_path = resolve_local_path(sid, output_dir)
        if local_path.exists():
            already_exist += 1
        else:
            to_download.append(sid)

    print(f"Already downloaded: {already_exist}")
    print(f"To download: {len(to_download)}")

    if not to_download:
        print("Nothing to download.")
        return

    #STFPTPFPT
    print(f"\nConnecting to {SFTP_HOST}...")
    transport = paramiko.Transport((SFTP_HOST, 22))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("Connected.\n")

    downloaded = 0
    failed = 0

    try:
        for i, sid in enumerate(to_download, 1):
            remote_path = resolve_remote_path(sid)
            local_path = resolve_local_path(sid, output_dir)

            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                sftp.get(remote_path, str(local_path))
                downloaded += 1
                print(f"[{i}/{len(to_download)}] {sid} -- DOWNLOADED")
            except FileNotFoundError:
                failed += 1
                print(f"[{i}/{len(to_download)}] !!! {sid} (not found on server)")
            except Exception as e:
                failed += 1
                print(f"[{i}/{len(to_download)}] !!! {sid} ({e})")
    finally:
        sftp.close()
        transport.close()
        print("\nSFTP connection closed.")

    print(f"\nSummary:")
    print(f"  Already existed: {already_exist}")
    print(f"  Downloaded:      {downloaded}")
    print(f"  Failed:          {failed}")
    print(f"  Total sources:   {len(source_ids)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download PRIMVS lightcurves via SFTP from a FITS catalog.")
    parser.add_argument("input_fits", help="Input FITS file with source IDs")
    parser.add_argument("--id-column", default="sourceid", help="Column name for source IDs (default: sourceid)")
    parser.add_argument("--output-dir", default="./light_curves", help="Local output directory (default: ./light_curves)")
    parser.add_argument("--hdu", type=int, default=1, help="HDU index to read (default: 1)")
    args = parser.parse_args()

    download_lightcurves(args.input_fits, args.id_column, args.output_dir, args.hdu)
