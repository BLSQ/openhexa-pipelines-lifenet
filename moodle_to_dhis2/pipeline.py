"""Template for newly generated pipelines."""

import json
import os
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import List

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from tracker import (
    TRACKED_ENTITY_TYPE,
    generate_uid,
    get_enrollments,
    get_events,
    get_program_org_units,
    get_tracked_entities,
)

STORED_BY = "Bluesquare"
DHIS2_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%3f"

TRACKED_ENTITY_ATTRIBUTES = {
    "first_name": "BHg8LLaoolq",
    "last_name": "srPxZ5neKN8",
    "user_id": "CgURgDLJJUi",
    "gender": "InLMldiUBio",
    "position": "MdUwUhekwRQ",
    "phase": "BhEi9vjZxzH",
}

POSITION_OPTION_SET = {
    "Nurse": "NURSE",
    "Midwife": "MIDWIFE",
    "Doctor": "DOCTOR",
    "Lab Technician": "LAB_TECHNICIAN",
    "Clinical Officer": "CLINICAL_OFFICER",
    "Cashier": "CASHIER",
    "Accountant": "ACCOUNTANT",
    "Records": "RECORDS",
    "Stores": "STORES",
    "Pharmacist": "PHARMACIST",
    "Counsellor": "COUNSELLOR",
    "Administrator": "ADMINISTRATOR",
    "In-Charge": "IN_CHARGE",
    "HUMC": "HUMC",
}

GENDER_OPTION_SET = {
    "Female": "FEMALE",
    "Male": "MALE",
    "Unknown": "UNKNOWN",
}  # default: "UNKNOWN"

PROGRAM_PHASE_OPTION_SET = {"Active": "ACTIVE", "Alumni": "ALUMNI"}

LEARNING_DATA_VALUES = {
    "completion_status": "ZYRGnmPtQZl",
    "course_id": "cLaBzltcQ8f",
    "course_stage": "q891hzw1n4D",
    "module": "mdzeD21jLmA",
    "score": "L9hy9NckqQm",
    "category": "jN22FmJuCaC",
}

ENROLLMENTS_DATA_VALUES = {
    "user_id": "lg5Yv6NDl6U",
    "first_name": "V8K8O44Rm15",
    "last_name": "laKnsyMWTLb",
    "course_id": "cLaBzltcQ8f",
    "module": "mdzeD21jLmA",
    "category": "jN22FmJuCaC",
    "certificate_issued": "fFK83fzYTRL",
    "completion_status": "ZYRGnmPtQZl",
}

LEARNING_PROGRAM_UID = "NYY8hzVYv8f"
LEARNING_PROGRAM_STAGE_UID = "iW8k954zTRK"

ENROLLMENTS_PROGRAM_UID = "kOWbqri5tY2"
ENROLLMENTS_PROGRAM_STAGE_UID = "mHBassGTx51"


@pipeline("moodle-to-dhis2", name="moodle-to-dhis2")
@parameter(
    "import_mode",
    name="Import mode",
    type=str,
    choices=["VALIDATE", "COMMIT"],
    default="COMMIT",
)
@parameter(
    "import_strategy",
    name="Import strategy",
    type=str,
    choices=["CREATE_AND_UPDATE", "CREATE", "UPDATE"],
    default="CREATE_AND_UPDATE",
)
@parameter(
    "validation_mode", name="Validation mode", type=str, choices=["FULL", "FAIL_FAST", "SKIP"], default="FAIL_FAST"
)
@parameter("input_dir", name="Input directory", type=str, default="moodle/data/raw")
@parameter("output_dir", name="Output directory", type=str, default="moodle/dhis2")
def moodle_to_dhis2(import_mode: str, import_strategy: str, validation_mode: str, input_dir: str, output_dir: str):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    input_dir = Path(workspace.files_path, input_dir)
    output_dir = Path(workspace.files_path, output_dir, datetime.now().strftime("%Y-%m-%d_%H:%M:%s"))
    if not output_dir.exists():
        os.makedirs(output_dir, exist_ok=True)

    sync(
        import_mode=import_mode,
        import_strategy=import_strategy,
        validation_mode=validation_mode,
        input_dir=input_dir,
        output_dir=output_dir,
    )


def transform_users(dhis2: DHIS2, users: pl.DataFrame, tracked_entities: pl.DataFrame) -> pl.DataFrame:
    """Prepare moodle user data to be pushed as tracked entities in DHIS2."""
    n = len(users)
    current_run.log_info(f"Processing {n} Moodle users")

    # ignore users with null org unit
    users = users.filter(pl.col("org_unit").is_not_null())
    if (n - len(users)) > 0:
        current_run.log_info(f"Ignoring {n - len(users)} Moodle users without org unit")
    n = len(users)

    # ignore users with org units that are not included in both programs
    for program_name, program_uid in zip(
        ["LifeNet Digital Learning", "Moodle Course Enrollments"], [LEARNING_PROGRAM_UID, ENROLLMENTS_PROGRAM_UID]
    ):
        program_org_units = get_program_org_units(dhis2, program_uid)
        users = users.filter(pl.col("org_unit").is_in(program_org_units))
        if (n - len(users)) > 0:
            current_run.log_info(
                f"Ignoring {n - len(users)} Moodle users because their org unit do not belong to the {program_name} program"
            )
        n = len(users)

    # join existing trackedEntity id if user_id already exists in Tracker
    if not tracked_entities.is_empty():
        tracked_entities = tracked_entities.unique(subset="user_id", keep="last")
        users = users.join(
            other=tracked_entities.select(["user_id", "trackedEntity"]),
            on="user_id",
            how="left",
        )
    else:
        users = users.with_columns(pl.lit(None).alias("trackedEntity"))

    # replace source values with option sets
    users = users.with_columns(
        [
            pl.col("gender").replace(GENDER_OPTION_SET, default="UNKNOWN"),
            pl.col("position").replace(POSITION_OPTION_SET, default=None),
            pl.col("phase").replace(PROGRAM_PHASE_OPTION_SET, default=None),
        ]
    )

    # ignore tracked entities with duplicated user IDs (forbidden by program)
    duplicates = users.filter(pl.col("user_id").is_duplicated())
    if not duplicates.is_empty():
        for user_id in duplicates["user_id"].unique():
            current_run.log_info(f"Ignoring user {user_id} because they are linked to multiple entities")
            users = users.filter(pl.col("user_id") != user_id)

    # convert datetimes to string as expected by DHIS2
    users = users.with_columns(
        [pl.col("time_created").dt.to_string(DHIS2_DATE_FORMAT), pl.col("last_access").dt.to_string(DHIS2_DATE_FORMAT)]
    )

    return users


def build_tracked_entities_payload(dhis2: DHIS2, users: pl.DataFrame, tracked_entities: pl.DataFrame) -> List[dict]:
    """Build JSON payload for tracked entities import."""
    uids = generate_uid(dhis2, n=len(users))
    payload = []

    for user in users.iter_rows(named=True):
        attributes = []

        # transform entity attributes into a list of dict
        for column, value in user.items():
            if column in TRACKED_ENTITY_ATTRIBUTES and value is not None:
                attributes.append({"attribute": TRACKED_ENTITY_ATTRIBUTES[column], "value": value})

        # check if the user alreay belongs to a tracked entity in DHIS2
        # if they do, get current data associated with the tracked entity for comparison
        # with the new data extracted from Moodle
        if user.get("trackedEntity"):
            uid = user["trackedEntity"]
            dst_entity = tracked_entities.row(by_predicate=pl.col("trackedEntity") == uid, named=True)

            # rebuild the payload from the data extracted from DHIS2
            dst_entity["attributes"] = []
            for column in dst_entity:
                if column in TRACKED_ENTITY_ATTRIBUTES:
                    dst_entity["attributes"].append(
                        {"attribute": TRACKED_ENTITY_ATTRIBUTES[column], "value": dst_entity[column]}
                    )

        # the user do not belong to an existing tracked entity in DHIS2
        # therefore we use a new UID
        else:
            uid = uids.pop()
            dst_entity = None

        src_entity = {
            "trackedEntity": uid,
            "trackedEntityType": TRACKED_ENTITY_TYPE,
            "orgUnit": user["org_unit"],
            "attributes": attributes,
        }

        def _get_attribute_value(attributes: List[dict], dx_uid: str) -> str | int | None:
            """Get attribute value from list of dict format in payload."""
            for attr in attributes:
                if attr["attribute"] == dx_uid:
                    return attr["value"]
            return None

        # check for changes before adding to payload
        changed = False
        if not dst_entity:
            changed = True
        else:
            for key in ["trackedEntity", "trackedEntityType", "orgUnit"]:
                if src_entity[key] != dst_entity[key]:
                    changed = True
            for name, dx in TRACKED_ENTITY_ATTRIBUTES.items():
                src = _get_attribute_value(src_entity["attributes"], dx)
                dst = _get_attribute_value(dst_entity["attributes"], dx)
                if src != dst:
                    changed = True

        # add entity to payload only if changes are detected
        if changed:
            payload.append(src_entity)

    return payload


def build_enrollments_payload(
    dhis2: DHIS2, users: pl.DataFrame, enrollments: pl.DataFrame, program_uid: str
) -> List[dict]:
    """Build JSON payload for program enrollments."""
    uids = generate_uid(dhis2, n=len(users))
    payload = []

    for user in users.iter_rows(named=True):
        if not user.get("trackedEntity") or not user.get("org_unit"):
            continue

        if "trackedEntity" in enrollments.columns and "orgUnit" in enrollments.columns:
            try:
                dst_enrol = enrollments.row(
                    by_predicate=(pl.col("trackedEntity") == user["trackedEntity"])
                    & (pl.col("orgUnit") == user["org_unit"]),
                    named=True,
                )
                uid = dst_enrol["enrollment"]
            except pl.exceptions.NoRowsReturnedError:
                dst_enrol = {}
                uid = uids.pop()
        else:
            dst_enrol = {}
            uid = uids.pop()

        src_enrol = {
            "enrolledAt": user["time_created"],
            "enrollment": uid,
            "occurredAt": user["time_created"],
            "orgUnit": user["org_unit"],
            "program": program_uid,
            "status": "ACTIVE",
            "trackedEntity": user["trackedEntity"],
            "trackedEntityType": TRACKED_ENTITY_TYPE,
        }

        dst_enrol = {k: v for k, v in dst_enrol.items() if k in src_enrol}

        same = True
        for key in ["enrollment", "trackedEntity", "program", "status", "orgUnit", "enrolledAt"]:
            if src_enrol.get(key) != dst_enrol.get(key):
                same = False

        if not same:
            payload.append(src_enrol)

    return payload


def transform_grades(
    grades: pl.DataFrame,
    enrollments: pl.DataFrame,
    tracked_entities: pl.DataFrame,
    courses: pl.DataFrame,
    events: pl.DataFrame,
) -> pl.DataFrame:
    """Prepare grade events for LifeNet Digital Learning program."""
    current_run.log_info(f"Processing {len(grades)} grades")
    # rename columns from moodle extract
    courses = courses.rename({"category_id": "module", "category_name": "module_name"})

    # ignore incomplete course entries
    courses = courses.filter(
        (pl.col("category").is_not_null()) & (pl.col("module").is_not_null()) & (pl.col("course_name").is_not_null())
    )

    # join course data to grades
    grades = grades.join(
        other=courses.select(["course_id", "course_name", "module", "category"]),
        on="course_id",
        how="left",
    )

    # join existing trackedEntity UID if they already exist
    grades = grades.join(
        other=tracked_entities.select(["trackedEntity", "user_id"]),
        on="user_id",
        how="left",
    )
    n = len(grades)

    # ignore grade events without trackedEntity UID
    grades = grades.filter(pl.col("trackedEntity").is_not_null())
    if n - len(grades) > 0:
        current_run.log_info(f"Skipping {n - len(grades)} grade events not linked to any tracked entity")
    n = len(grades)

    # join enrollment UID if they already exist
    grades = grades.join(
        other=enrollments.select(["enrollment", "trackedEntity", "orgUnit"]),
        on="trackedEntity",
        how="left",
    )
    if n - len(grades) > 0:
        current_run.log_info(f"Skipping {n - len(grades)} grade events not linked to any enrollment")
    n = len(grades)

    # transform the course_stage column into 2 values: PRE_TEST and POST_TEST
    grades = grades.with_columns(
        pl.when(pl.col("item_name").str.contains(r"\b(p|P)r(e|é|é)\b"))
        .then(pl.lit("PRE_TEST"))
        .otherwise(pl.when(pl.col("item_name").str.contains(r"\b(p|P)ost\b")).then(pl.lit("POST_TEST")).otherwise(None))
        .alias("course_stage")
    )

    # convert datetime to string as expected by DHIS2
    grades = grades.with_columns(pl.col("time_modified").dt.to_string(DHIS2_DATE_FORMAT))

    # join exist event UID if they already exist
    grades = grades.join(
        other=events.select(["trackedEntity", "orgUnit", "course_id", "course_stage", "event"]),
        on=["trackedEntity", "orgUnit", "course_id", "course_stage"],
        how="left",
    )

    return grades


def build_grade_events_payload(dhis2: DHIS2, grades: pl.DataFrame, events: pl.DataFrame) -> List[dict]:
    """Build JSON payload for events (LifeNet Digital Learning program)."""
    payload = []
    uids = generate_uid(dhis2, n=len(grades))

    # make sure data types are correct in events data
    events = events.with_columns(
        [pl.col("module").cast(int), pl.col("score").cast(float), pl.col("course_id").cast(int)]
    )

    for grade in grades.iter_rows(named=True):
        if grade.get("event"):
            uid = grade["event"]
            dst = events.row(by_predicate=pl.col("event") == uid, named=True)
        else:
            uid = uids.pop()
            dst = {}

        # format event data values
        data_values = []
        for column, value in grade.items():
            if column in LEARNING_DATA_VALUES and value is not None:
                data_values.append(
                    {
                        "storedBy": STORED_BY,
                        "dataElement": LEARNING_DATA_VALUES[column],
                        "value": value,
                    }
                )

        # for each data value, compare source and destination
        same = True
        for col in LEARNING_DATA_VALUES:
            if grade.get(col) != dst.get(col):
                same = False

        if not same:
            payload.append(
                {
                    "program": LEARNING_PROGRAM_UID,
                    "event": uid,
                    "programStage": LEARNING_PROGRAM_STAGE_UID,
                    "orgUnit": grade["orgUnit"],
                    "trackedEntity": grade["trackedEntity"],
                    "enrollment": grade["enrollment"],
                    "enrollmentStatus": "ACTIVE",
                    "occurredAt": grade["time_modified"],
                    "dataValues": data_values,
                }
            )

    return payload


def transform_enrollments(
    enrollments: pl.DataFrame,
    users: pl.DataFrame,
    certificates: pl.DataFrame,
    courses: pl.DataFrame,
    completions: pl.DataFrame,
    events: pl.DataFrame,
) -> pl.DataFrame:
    """Transform moodle course enrollments to be pushed as Tracker events."""
    # join user data to course enrollments
    enrollments = enrollments.join(other=users, on="user_id", how="left").join(
        other=courses, on="course_id", how="left"
    )

    # check if a certificate has been issued for a given user & course
    enrollments = enrollments.with_columns(
        pl.struct(["user_id", "course_id"])
        .map_elements(
            lambda x: not certificates.filter(
                (pl.col("user_id") == x["user_id"]) & (pl.col("course_id") == x["course_id"])
            ).is_empty()
        )
        .alias("certificate_issued")
    )

    # convert datetime to string as expected by DHIS2
    enrollments = enrollments.with_columns(pl.col("enrollment_date").dt.to_string(DHIS2_DATE_FORMAT))

    # get maximum completion state for a given user & course
    completions = (
        completions.group_by(by=["user_id", "course_id"]).max().select(["user_id", "course_id", "completion_state"])
    )
    enrollments = enrollments.join(other=completions, on=["user_id", "course_id"], how="left")

    # ignore course enrollments without any associated trackedEntity
    enrollments = enrollments.filter(pl.col("trackedEntity").is_not_null())

    enrollments = enrollments.rename({"category_id": "module", "completion_state": "completion_status"})

    # join existing event uid if they already exist
    enrollments = enrollments.join(
        other=events.select(["orgUnit", "course_id", "user_id", "event"]),
        left_on=["org_unit", "course_id", "user_id"],
        right_on=["orgUnit", "course_id", "user_id"],
        how="left",
    )

    return enrollments


def build_enrollment_events_payload(dhis2: DHIS2, enrollments: pl.DataFrame, events: pl.DataFrame) -> List[dict]:
    """Build JSON payload for events (LifeNet Digital Learning program)."""
    payload = []
    uids = generate_uid(dhis2, n=len(enrollments))

    # make sure data types are correct in events data
    enrollments = enrollments.with_columns([pl.col("course_id").cast(int), pl.col("user_id").cast(int)])

    for enrol in enrollments.iter_rows(named=True):
        if enrol.get("event"):
            uid = enrol["event"]
            dst = events.row(by_predicate=pl.col("event") == uid, named=True)
        else:
            uid = uids.pop()
            dst = {}

        # format event data values
        data_values = []
        for column, value in enrol.items():
            if column in ENROLLMENTS_DATA_VALUES and value is not None:
                data_values.append(
                    {
                        "storedBy": STORED_BY,
                        "dataElement": ENROLLMENTS_DATA_VALUES[column],
                        "value": value,
                    }
                )

        # for each data value, compare source and destination
        same = True
        for col in ENROLLMENTS_DATA_VALUES:
            if enrol.get(col) != dst.get(col):
                same = False

        if not same:
            payload.append(
                {
                    "program": ENROLLMENTS_PROGRAM_UID,
                    "event": uid,
                    "programStage": ENROLLMENTS_PROGRAM_STAGE_UID,
                    "orgUnit": enrol["org_unit"],
                    "trackedEntity": enrol["trackedEntity"],
                    "occurredAt": enrol["enrollment_date"],
                    "dataValues": data_values,
                }
            )

    return payload


def post(dhis2: DHIS2, payload: dict, import_mode: str, import_strategy: str, validation_mode: str) -> bool:
    """Push tracked entities, program enrollments or events to DHIS2."""
    params = {"importMode": import_mode, "importStrategy": import_strategy, "validationMode": validation_mode}
    return
    # check if payload is empty before starting import job
    empty = True
    for payload_type in ["events", "trackedEntities", "enrollments"]:
        if payload.get(payload_type):
            empty = False
    if empty:
        current_run.log_info("No change detected, skipping import job")
        return True

    # start import job
    r = dhis2.api.post(
        endpoint="tracker",
        json=payload,
        params=params,
    )
    job_uid = r.json()["response"]["id"]
    current_run.log_info(f"Started tracker import job {job_uid}. Waiting for completion...")
    current_run.log_info(f"Import job progress available at {dhis2.api.url}/tracker/jobs{job_uid}")

    # request job status and wait for completion
    completed = False
    while not completed:
        r = dhis2.api.get(f"tracker/jobs/{job_uid}")
        progress = r.json()
        for task in progress:
            if task.get("completed"):
                completed = True
        sleep(10)

    # check job failure/success
    r = dhis2.api.get(f"tracker/jobs/{job_uid}/report")
    report = r.json()
    if report["status"] == "OK":
        created = report["stats"]["created"]
        updated = report["stats"]["updated"]
        deleted = report["stats"]["deleted"]
        ignored = report["stats"]["ignored"]
        current_run.log_info(
            f"Import job {job_uid} completed (created: {created}, updated: {updated}, deleted: {deleted}, ignored: {ignored})"
        )
        current_run.log_info(f"Full report available at {dhis2.api.url}/tracker/jobs/{job_uid}/report")
    else:
        current_run.log_error(
            f"Import job {job_uid} failed. Full report available at {dhis2.api.url}/tracker/jobs/{job_uid}/report"
        )
        raise ValueError(
            f"Import job {job_uid} failed. Full report available at {dhis2.api.url}/tracker/jobs/{job_uid}/report"
        )

    return True


@moodle_to_dhis2.task
def sync(import_mode: str, import_strategy: str, validation_mode: str, input_dir: Path, output_dir: Path):
    dhis2 = DHIS2(workspace.dhis2_connection("lifenet"))

    # load and transform moodle users data
    users = pl.read_parquet(input_dir / "users.parquet")
    tracked_entities = get_tracked_entities(dhis2, TRACKED_ENTITY_TYPE)
    users = transform_users(dhis2, users, tracked_entities)

    # push users as tracked entities
    payload = build_tracked_entities_payload(dhis2, users, tracked_entities)
    with open(output_dir / "tracked_entities.json", "w") as f:
        json.dump(payload, f)
    post(
        dhis2,
        payload={"trackedEntities": payload},
        import_mode=import_mode,
        import_strategy=import_strategy,
        validation_mode=validation_mode,
    )

    # reload users with up-to-date trackedEntities uid
    users = pl.read_parquet(input_dir / "users.parquet")
    tracked_entities = get_tracked_entities(dhis2, TRACKED_ENTITY_TYPE)
    users = transform_users(dhis2, users, tracked_entities)
    users.write_parquet(output_dir / "users.parquet")

    # push program enrollments
    enrollments = get_enrollments(dhis2, LEARNING_PROGRAM_UID)
    payload = build_enrollments_payload(dhis2, users, enrollments, LEARNING_PROGRAM_UID)
    post(
        dhis2,
        payload={"enrollments": payload},
        import_mode=import_mode,
        import_strategy=import_strategy,
        validation_mode=validation_mode,
    )
    with open(output_dir / "enrollments.json", "w") as f:
        json.dump(payload, f)

    # reload program enrollments with up-to-date uid
    enrollments = get_enrollments(dhis2, LEARNING_PROGRAM_UID)

    # load and transform moodle grades data
    grades = pl.read_parquet(input_dir / "grades.parquet")
    courses = pl.read_parquet(input_dir / "courses.parquet")
    events = get_events(dhis2, LEARNING_PROGRAM_UID, LEARNING_DATA_VALUES, include_deleted=False)
    events = events.with_columns(
        [
            # pl.col("completion_status").cast(int),
            pl.col("course_id").cast(int),
            pl.col("module").cast(int),
            pl.col("score").cast(float),
        ]
    )
    grades = transform_grades(grades, enrollments, tracked_entities, courses, events)
    grades.write_parquet(output_dir / "grades.parquet")

    # push moodle grades events
    payload = build_grade_events_payload(dhis2, grades, events)
    post(
        dhis2,
        payload={"events": payload},
        import_mode=import_mode,
        import_strategy=import_strategy,
        validation_mode=validation_mode,
    )
    with open(output_dir / "events_grades.json", "w") as f:
        json.dump(payload, f)

    # load and transform moodle course enrollments data
    enrollments = pl.read_parquet(input_dir / "enrollments.parquet")
    certificates = pl.read_parquet(input_dir / "certificates.parquet")
    courses = pl.read_parquet(input_dir / "courses.parquet")
    completions = pl.read_parquet(input_dir / "completions.parquet")
    events = get_events(dhis2, ENROLLMENTS_PROGRAM_UID, ENROLLMENTS_DATA_VALUES, include_deleted=False)

    if "completion_status" not in events.columns:
        events = events.with_columns(pl.lit(None).alias("completion_status"))

    events = events.with_columns(
        [
            pl.col("user_id").cast(int),
            pl.col("course_id").cast(int),
            pl.col("module").cast(int),
            (pl.col("certificate_issued") == "true").alias("certificate_issued"),
            pl.col("completion_status").cast(int),
        ]
    )
    enrollments = transform_enrollments(enrollments, users, certificates, courses, completions, events)
    enrollments.write_parquet(output_dir / "enrollments.parquet")

    # push moodle course enrollments events
    payload = build_enrollment_events_payload(dhis2, enrollments, events)
    post(
        dhis2,
        payload={"events": payload},
        import_mode=import_mode,
        import_strategy=import_strategy,
        validation_mode=validation_mode,
    )
    with open(output_dir / "events_enrollments.json", "w") as f:
        json.dump(payload, f)

    return


if __name__ == "__main__":
    moodle_to_dhis2()
