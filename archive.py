#!/usr/bin/env python3
import io
import tarfile
import zipfile
from pathlib import Path
from datetime import datetime, timedelta

import dotenv
import click
import zstandard as zstd
from tqdm import tqdm


def zip_day(in_path, out_path):
    with zipfile.ZipFile(str(out_path), "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in tqdm(sorted(in_path.glob("*.json")), desc=out_path.name, disable=None):
            zf.write(str(p), arcname=p.name)


def tarzst_day(in_path, out_path):
    cctx = zstd.ZstdCompressor(level=14, write_checksum=True)
    with out_path.open("wb") as of:
        with cctx.stream_writer(of) as zstd_writer:
            with tarfile.open(mode="w|", fileobj=zstd_writer) as tf:
                for p in tqdm(
                    sorted(in_path.glob("*.json")), desc=out_path.name, disable=None
                ):
                    tf.add(str(p), arcname=p.name)


def archive_day(data_path, archive_path, day, make_zip, make_tar_zst, force=False):
    in_path = data_path / day
    if make_zip:
        zip_path = archive_path / "{}.zip".format(day)
        if force or not zip_path.exists():
            zip_day(in_path, zip_path)
    if make_tar_zst:
        tarzst_path = archive_path / "{}.tar.zst".format(day)
        if force or not tarzst_path.exists():
            tarzst_day(in_path, tarzst_path)


def archive_days(data_path, archive_path, make_zip, make_tar_zst, force):
    today = datetime.now().strftime("%Y%m%d")
    for day in sorted([d for d in data_path.iterdir() if d.is_dir()]):
        if day.name == today:
            continue
        archive_day(data_path, archive_path, day.name, make_zip, make_tar_zst, force)


@click.command()
@click.option(
    "--data-dir",
    envvar="DATA_DIR",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
    help="path to the daily data folders",
)
@click.option(
    "--archive-dir",
    envvar="ARCHIVE_DIR",
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    help="output path",
)
@click.option(
    "--zip/--no-zip",
    default=False,
    show_default=True,
    help="create a .zip archive per day",
)
@click.option(
    "--tar-zst/--no-tar-zst",
    default=True,
    show_default=True,
    help="create a .tar.zst archive per day",
)
@click.option("-f", "--force", is_flag=True, help="override existing archives")
def main(data_dir, archive_dir, zip, tar_zst, force):
    data_path = Path(data_dir)
    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)
    archive_days(data_path, archive_path, zip, tar_zst, force)


if __name__ == "__main__":
    dotenv.load_dotenv()
    main()
