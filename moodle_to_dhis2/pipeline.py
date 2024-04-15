"""Template for newly generated pipelines."""

import json
import os
from datetime import datetime
from pathlib import Path

import polars as pl
from openhexa.sdk import parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from tracker import (
    DHIS2_DATE_FORMAT,
    ENROLLMENTS_PROGRAM_STAGE_UID,
    ENROLLMENTS_PROGRAM_UID,
    LEARNING_DATA_VALUES,
    LEARNING_PROGRAM_STAGE_UID,
    LEARNING_PROGRAM_UID,
    TRACKED_ENTITY_ATTRIBUTES,
    TRACKED_ENTITY_TYPE,
    get_enrollments,
    get_tracked_entities,
    join_tracked_entities_uid,
    prepare_course_enrollments_events_payload,
    prepare_enrollments_payload,
    prepare_grade_events_payload,
    prepare_tracked_entities_payload,
    push_to_tracker,
)


@pipeline("moodle-to-dhis2", name="moodle-to-dhis2")
@parameter(
    "import_mode",
    name="Import mode",
    type=str,
    choices=["VALIDATE", "COMMIT"],
    default="VALIDATE",
)
@parameter(
    "import_strategy",
    name="Import strategy",
    type=str,
    choices=["CREATE_AND_UPDATE", "CREATE", "UPDATE"],
    default="CREATE_AND_UPDATE",
)
@parameter("input_dir", name="Input directory", type=str, default="moodle/data/raw")
@parameter("output_dir", name="Output directory", type=str, default="moodle/dhis2")
def moodle_to_dhis2(import_mode: str, import_strategy: str, input_dir: str, output_dir: str):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    con = workspace.dhis2_connection("moodle-lifenet")
    dhis2 = DHIS2(con, Path(workspace.files_path, ".cache"))

    input_dir = Path(workspace.files_path, input_dir)
    output_dir = Path(
        workspace.files_path, output_dir, datetime.now().strftime("%Y-%m-%d_%H:%M:%s")
    )
    if not output_dir.exists():
        os.makedirs(output_dir, exist_ok=True)

    task1 = sync_tracked_entities(dhis2, input_dir, output_dir)
    task2 = sync_grade_enrollments(dhis2, input_dir, output_dir, wait=task1)
    task3 = sync_grade_events(dhis2, input_dir, output_dir, wait=task2)
    task4 = sync_enrollment_events(dhis2, input_dir, output_dir, wait=task3)


@moodle_to_dhis2.task
def sync_tracked_entities(dhis2: DHIS2, input_dir: Path, output_dir: Path):
    """TODO"""
    users = pl.read_parquet(Path(input_dir, "users.parquet"))
    tracked_entities = get_tracked_entities(dhis2, TRACKED_ENTITY_TYPE)

    users = join_tracked_entities_uid(
        dhis2=dhis2,
        users=users,
        tracked_entities=tracked_entities,
        program_uid=LEARNING_PROGRAM_UID,
    )

    payload = prepare_tracked_entities_payload(
        dhis2=dhis2,
        users=users,
        tracked_entity_attributes=TRACKED_ENTITY_ATTRIBUTES,
        tracked_entity_type=TRACKED_ENTITY_TYPE,
    )

    with open(Path(output_dir, "tracked_entities_payload.json"), "w") as f:
        json.dump(payload, f, indent=True)

    report = push_to_tracker(dhis2=dhis2, tracked_entities=payload)

    with open(Path(output_dir, "tracked_entities_report.json"), "w") as f:
        json.dump(
            report,
            f,
            indent=True,
        )

    return True


@moodle_to_dhis2.task
def sync_grade_enrollments(
    dhis2: DHIS2, input_dir: Path, output_dir: Path, wait: bool | None = None
):
    """TODO"""
    users = pl.read_parquet(Path(input_dir, "users.parquet"))
    enrollments = get_enrollments(dhis2, LEARNING_PROGRAM_UID)
    payload = prepare_enrollments_payload(
        existing_enrollments=enrollments, users=users, program_uid=LEARNING_PROGRAM_UID, dhis2=dhis2
    )

    with open(Path(output_dir, "grade_enrollments_payload.json"), "w") as f:
        json.dump(payload, f, indent=True)

    report = push_to_tracker(dhis2=dhis2, enrollments=payload)

    with open(Path(output_dir, "grade_enrollments_report.json"), "w") as f:
        json.dump(report, f, indent=True)

    return True


@moodle_to_dhis2.task
def sync_grade_events(dhis2: DHIS2, input_dir: Path, output_dir: Path, wait: bool | None = None):
    """TODO"""
    grades = pl.read_parquet(Path(input_dir, "grades.parquet"))
    grades = grades.filter(pl.col("score").is_not_null())

    users = pl.read_parquet(Path(input_dir, "users.parquet"))
    tracked_entities = get_tracked_entities(
        dhis2=dhis2,
        tracked_entity_type=TRACKED_ENTITY_TYPE,
    )
    users = join_tracked_entities_uid(
        dhis2=dhis2,
        users=users,
        tracked_entities=tracked_entities,
        program_uid=LEARNING_PROGRAM_UID,
    )

    courses = pl.read_parquet(Path(input_dir, "courses.parquet"))
    courses = courses.rename({"category_id": "module", "category_name": "module_name"})
    courses = courses.filter(
        (pl.col("category").is_not_null())
        & (pl.col("module").is_not_null())
        & (pl.col("course_name").is_not_null())
    )

    grades = grades.join(
        other=users.select(["user_id", "org_unit", "trackedEntity"]), on="user_id", how="left"
    ).join(
        other=courses.select(["course_id", "course_name", "module", "category"]),
        on="course_id",
        how="left",
    )

    grades = grades.with_columns(
        pl.when(pl.col("item_name").str.contains(r"\b(p|P)r(e|é|é)\b"))
        .then(pl.lit("PRE_TEST"))
        .otherwise(
            pl.when(pl.col("item_name").str.contains(r"\b(p|P)ost\b"))
            .then(pl.lit("POST_TEST"))
            .otherwise(None)
        )
        .alias("course_stage")
    )

    grades = grades.with_columns(
        pl.col("time_modified").map_elements(
            lambda x: x.strftime(DHIS2_DATE_FORMAT), skip_nulls=True
        )
    )

    for c in ("trackedEntity", "course_stage", "time_modified"):
        count = len(grades)
        grades = grades.filter(pl.col(c).is_not_null())
        if len(grades) < count:
            print(
                f"Ignored {count - len(grades)} grade events with unexpected null values for column `{c}`"
            )

    payload = prepare_grade_events_payload(
        dhis2=dhis2,
        grades=grades,
        program_uid=LEARNING_PROGRAM_UID,
        program_stage_uid=LEARNING_PROGRAM_STAGE_UID,
        data_values_mapping=LEARNING_DATA_VALUES,
    )

    with open(Path(output_dir, "grade_events_payload.json"), "w") as f:
        json.dump(payload, f, indent=True)

    report = push_to_tracker(dhis2=dhis2, events=payload)

    with open(Path(output_dir, "grade_events_report.json"), "w") as f:
        json.dump(report, f, indent=True)

    return True


@moodle_to_dhis2.task
def sync_enrollment_events(
    dhis2: DHIS2, input_dir: Path, output_dir: Path, wait: bool | None = None
):
    """TODO"""
    enrollments = pl.read_parquet(Path(input_dir, "enrollments.parquet"))

    users = pl.read_parquet(Path(input_dir, "users.parquet"))
    tracked_entities = get_tracked_entities(dhis2=dhis2, tracked_entity_type=TRACKED_ENTITY_TYPE)
    users = join_tracked_entities_uid(
        dhis2=dhis2,
        users=users,
        tracked_entities=tracked_entities,
        program_uid=ENROLLMENTS_PROGRAM_UID,
    )

    courses = pl.read_parquet(Path(input_dir, "courses.parquet"))
    courses = courses.rename({"category_id": "module", "category_name": "module_name"})
    courses = courses.filter(
        (pl.col("category").is_not_null())
        & (pl.col("module").is_not_null())
        & (pl.col("course_name").is_not_null())
    )

    certificates = pl.read_parquet(Path(input_dir, "certificates.parquet"))

    enrollments = enrollments.with_columns(
        pl.col("enrollment_date").map_elements(
            lambda x: x.strftime(DHIS2_DATE_FORMAT), skip_nulls=True
        )
    )

    payload = prepare_course_enrollments_events_payload(
        enrollments=enrollments,
        certificates=certificates,
        courses=courses,
        users=users,
        dhis2=dhis2,
        program_uid=ENROLLMENTS_PROGRAM_UID,
        program_stage_uid=ENROLLMENTS_PROGRAM_STAGE_UID,
    )

    with open(Path(output_dir, "enrollments_events_payload.json"), "w") as f:
        json.dump(payload, f, indent=True)

    report = push_to_tracker(dhis2=dhis2, events=payload)

    with open(Path(output_dir, "enrollments_events_report.json"), "w") as f:
        json.dump(report, f, indent=True)

    return True


if __name__ == "__main__":
    moodle_to_dhis2()
