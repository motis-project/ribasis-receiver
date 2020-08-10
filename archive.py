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
            zf.write(str(p))


def tarzst_day(in_path, out_path):
    cctx = zstd.ZstdCompressor(level=14)
    with out_path.open("wb") as of:
        with cctx.stream_writer(of) as zstd_writer:
            with tarfile.open(mode="w|", fileobj=zstd_writer) as tf:
                for p in tqdm(
                    sorted(in_path.glob("*.json")), desc=out_path.name, disable=None
                ):
                    tf.add(str(p), arcname=p.name)


def archive_day(data_path, archive_path, day, make_zip, make_tar_zst):
    in_path = data_path / day
    if make_zip:
        zip_path = archive_path / "{}.zip".format(day)
        zip_day(in_path, zip_path)
    if make_tar_zst:
        tarzst_path = archive_path / "{}.tar.zst".format(day)
        tarzst_day(in_path, tarzst_path)


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
    "--day",
    default=lambda: (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
    show_default="yesterday",
    help="day in YYYYmmdd format",
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
def main(data_dir, archive_dir, day, zip, tar_zst):
    data_path = Path(data_dir)
    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)
    archive_day(data_path, archive_path, day, zip, tar_zst)


if __name__ == "__main__":
    dotenv.load_dotenv()
    main()
