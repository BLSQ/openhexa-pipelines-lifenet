"""Template for newly generated pipelines."""

import os
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.sdk.utils import Environment, get_environment
from queries import QUERIES


@pipeline("moodle-extract", name="Extract Moodle data")
@parameter("output_dir", name="Output directory", type=str, default="moodle/data/raw")
def moodle_extract(output_dir: str):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    con = workspace.custom_connection("moodle-database")
    db = f"mysql://{con.username}:{con.password}@{con.host}:{con.port}/{con.dbname}"
    current_run.log_info(f"Connected to database {con.host}/{con.dbname} with user {con.username}")
    output_dir = Path(workspace.files_path, Path(output_dir))
    os.makedirs(output_dir, exist_ok=True)

    download(db, output_dir)


@moodle_extract.task
def download(db: str, dst_dir: Path):
    """Get data from the Moodle database."""
    for name in ("users", "courses", "grades", "enrollments", "certificates", "completions"):
        dst_file_pqt = Path(dst_dir, f"{name}.parquet")
        dst_file_csv = Path(dst_dir, f"{name}.csv")

        if dst_file_pqt.exists():
            df_old = pl.read_parquet(dst_file_pqt)
            len_old = len(df_old)
        else:
            len_old = 0

        df = pl.read_database_uri(query=QUERIES[name], uri=db)
        current_run.log_info(f"Loaded up-to-date {name} data ({len(df)} rows)")

        try:
            assert len(df) >= len_old
        except AssertionError:
            current_run.log_error(
                f"New version of file {name}.parquet has less row than previous one"
            )
            raise

        df.write_parquet(dst_file_pqt)
        df.write_csv(dst_file_csv)

        if get_environment() == Environment.CLOUD_PIPELINE:
            current_run.add_file_output(dst_file_pqt.as_posix())
            current_run.add_file_output(dst_file_csv.as_posix())

    return True


if __name__ == "__main__":
    moodle_extract()
