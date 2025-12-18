from time import sleep
from typing import List

import polars as pl
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2

DHIS2_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%3f"
STORED_BY = "Bluesquare"

LEARNING_PROGRAM_UID = "NYY8hzVYv8f"
LEARNING_PROGRAM_STAGE_UID = "iW8k954zTRK"

ENROLLMENTS_PROGRAM_UID = "kOWbqri5tY2"
ENROLLMENTS_PROGRAM_STAGE_UID = "mHBassGTx51"

TRACKED_ENTITY_TYPE = "ZDSBUOcHKV2"

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

LEARNING_DATA_VALUES = {
    "completion_status": "ZYRGnmPtQZl",
    "course_id": "cLaBzltcQ8f",
    "course_stage": "q891hzw1n4D",
    "module": "mdzeD21jLmA",
    "score": "L9hy9NckqQm",
    "category": "jN22FmJuCaC",
}


def get_tracked_entities(dhis2: DHIS2, tracked_entity_type: str) -> pl.DataFrame:
    """Get all tracked entities from DHIS2 for a given tracked entity type."""
    mapping = {v: k for k, v in TRACKED_ENTITY_ATTRIBUTES.items()}
    entities = []

    r = dhis2.api.get(
        "tracker/trackedEntities",
        params={
            "ouMode": "ALL",
            "trackedEntityType": tracked_entity_type,
            "filter": f"{TRACKED_ENTITY_ATTRIBUTES['user_id']}:gt:0",
            "skipPaging": True,
        },
    )

    for entity in r["trackedEntities"]:
        for attr in entity["attributes"]:
            entity[mapping[attr["attribute"]]] = attr["value"]
        entities.append(entity)

    COLUMNS = [
        "trackedEntity",
        "trackedEntityType",
        "createdAt",
        "updatedAt",
        "orgUnit",
        "inactive",
        "deleted",
    ]
    COLUMNS += list(mapping.values())

    df = pl.DataFrame(entities)

    if df.is_empty():
        return df

    df = df.select([c for c in COLUMNS if c in df.columns])

    df = df.with_columns(
        [
            pl.col("createdAt").str.to_datetime(format=DHIS2_DATE_FORMAT),
            pl.col("updatedAt").str.to_datetime(format=DHIS2_DATE_FORMAT),
            pl.col("user_id").cast(int),
        ]
    )

    return df


def get_enrollments(dhis2: DHIS2, program_uid: str) -> pl.DataFrame:
    """Get existing enrollments for a given Tracker program."""
    enrollments = []

    r = dhis2.api.get(
        "tracker/enrollments",
        params={"ouMode": "ALL", "program": program_uid, "skipPaging": True},
    )

    for enrol in r["enrollments"]:
        enrollments.append(enrol)

    df = pl.DataFrame(enrollments)
    return df


def get_events(
    dhis2: DHIS2, program_uid: str, data_values: dict, include_deleted: bool = False
) -> pl.DataFrame:
    """Get existing enrollments for a given Tracker program."""
    events = []

    params = {
        "ouMode": "ALL",
        "program": program_uid,
        "skipPaging": True,
        "includeDeleted": include_deleted,
    }

    r = dhis2.api.get(
        "tracker/events",
        params=params,
    )

    mapping = {v: k for k, v in data_values.items()}
    for event in r["events"]:
        if "dataValues" in event:
            for data_value in event["dataValues"]:
                event[mapping[data_value["dataElement"]]] = data_value["value"]
        events.append(event)

    COLUMNS = [
        "event",
        "status",
        "program",
        "programStage",
        "enrollment",
        "trackedEntity",
        "orgUnit",
        "orgUnitName",
        "occurredAt",
        "createdAt",
        "updatedAt",
        "deleted",
        "attributeOptionCombo",
        "attributeCategoryOptions",
    ]
    COLUMNS += list(mapping.values())

    df = pl.DataFrame(events)
    df = df.select([c for c in COLUMNS if c in df.columns])
    return df


def get_program_org_units(dhis2: DHIS2, program_uid: str) -> List[str]:
    """Get org units for a given DHIS2 Tracker program."""
    r = dhis2.api.get(f"programs/{program_uid}")
    org_units = [ou["id"] for ou in r["organisationUnits"]]
    return org_units


def generate_uid(dhis2: DHIS2, n: int = 1) -> List[str]:
    """Generate valid UID using DHIS2 API."""
    return dhis2.api.get("system/id", params={"limit": n})["codes"]


def join_tracked_entities_uid(
    dhis2: DHIS2, users: pl.DataFrame, tracked_entities: pl.DataFrame, program_uid: str
) -> pl.DataFrame:
    """Join tracked entity uid of users if they already exists in DHIS2 Tracker."""
    # make sure user_id is an int, as dhis2 response is always str
    tracked_entities = tracked_entities.with_columns(pl.col("user_id").cast(int))

    # warn user about users associated with an org unit not included in the program
    org_units = get_program_org_units(dhis2, program_uid)
    for ou in users["org_unit"].unique():
        if ou:
            if ou not in org_units:
                current_run.log_warning(
                    f"Org unit '{ou}' from Moodle not found in Tracker program"
                )

    users = users.filter(pl.col("org_unit").is_in(org_units))

    # join trackedEntity uid from existing DHIS2 Tracker data
    users = users.join(
        other=tracked_entities.select(["user_id", "trackedEntity", "orgUnit"]),
        left_on=["user_id", "org_unit"],
        right_on=["user_id", "orgUnit"],
        how="left",
    )

    return users


def prepare_tracked_entities_payload(
    dhis2: DHIS2,
    users: pl.DataFrame,
    tracked_entity_attributes: dict,
    tracked_entity_type: str,
) -> List[dict]:
    """Prepare JSON payload for DHIS2 Tracker tracked entities."""
    # generate valid DHIS2 uids, we will need len(users) at most
    uids = generate_uid(dhis2, n=len(users))

    entities = []

    for entity in users.iter_rows(named=True):
        attributes = []

        for column, value in entity.items():
            if column in tracked_entity_attributes and value is not None:
                attributes.append(
                    {"attribute": tracked_entity_attributes[column], "value": value}
                )

        if entity.get("trackedEntity"):
            tracked_entity_uid = entity["trackedEntity"]
        else:
            tracked_entity_uid = uids.pop()

        entities.append(
            {
                "orgUnit": entity["org_unit"],
                "trackedEntityType": tracked_entity_type,
                "trackedEntity": tracked_entity_uid,
                "attributes": attributes,
            }
        )

    return entities


def prepare_enrollments_payload(
    existing_enrollments: pl.DataFrame,
    users: pl.DataFrame,
    program_uid: str,
    dhis2: DHIS2,
) -> List[dict]:
    """Prepare JSON payload DHIS2 Tracker enrollments."""
    enrollments = []
    uids = generate_uid(dhis2, n=len(users))

    for user in users.iter_rows(named=True):
        if not user.get("trackedEntity") or not user.get("org_unit"):
            continue

        if (
            "trackedEntity" in existing_enrollments.columns
            and "orgUnit" in existing_enrollments.columns
        ):
            df_ = existing_enrollments.filter(
                (pl.col("trackedEntity") == user["trackedEntity"])
                & (pl.col("orgUnit") == user["org_unit"])
            )
            if df_.is_empty():
                enrollment_uid = uids.pop()
            else:
                enrollment_uid = df_["enrollment"][0]
        else:
            enrollment_uid = uids.pop()

        enrollments.append(
            {
                "enrolledAt": user["time_created"],
                "enrollment": enrollment_uid,
                "occuredAt": user["time_created"],
                "orgUnit": user["org_unit"],
                "program": program_uid,
                "status": "ACTIVE",
                "trackedEntity": user["trackedEntity"],
                "trackedEntityType": TRACKED_ENTITY_TYPE,
            }
        )

    return enrollments


def prepare_grade_events_payload(
    dhis2: DHIS2,
    grades: pl.DataFrame,
    program_uid: str,
    program_stage_uid: str,
    data_values_mapping: dict,
):
    payload = []
    uids = generate_uid(dhis2, n=len(grades))

    # join existing events uid
    events = get_events(
        dhis2=dhis2, program_uid=LEARNING_PROGRAM_UID, data_values=LEARNING_DATA_VALUES
    )
    events = events.with_columns(
        [
            pl.col("completion_status").cast(int),
            pl.col("course_id").cast(int),
            pl.col("module").cast(int),
            pl.col("score").cast(float),
        ]
    )
    grades = grades.join(
        other=events.select(["event", "trackedEntity", "course_id", "course_stage"]),
        on=["trackedEntity", "course_id", "course_stage"],
        how="left",
    )

    # join existing enrollments uid
    enrollments = get_enrollments(dhis2, LEARNING_PROGRAM_UID)

    grades = grades.join(
        other=enrollments.select(["enrollment", "orgUnit", "trackedEntity"]),
        left_on=["org_unit", "trackedEntity"],
        right_on=["orgUnit", "trackedEntity"],
        how="left",
    )

    count = len(grades)
    grades = grades.filter(pl.col("enrollment").is_not_null())
    if len(grades) < count:
        current_run.log_warning(
            f"Ignored {count - len(grades)} grade events with unexpected null value for column `enrollment`"
        )

    for grade in grades.iter_rows(named=True):
        # assign new event uid if it does not exist
        if not grade.get("event"):
            grade["event"] = uids.pop()

        data_values = []
        for column, value in grade.items():
            if column in data_values_mapping and value is not None:
                data_values.append(
                    {
                        "storedBy": STORED_BY,
                        "dataElement": data_values_mapping[column],
                        "value": value,
                    }
                )

        payload.append(
            {
                "program": program_uid,
                "event": grade["event"],
                "programStage": program_stage_uid,
                "orgUnit": grade["org_unit"],
                "trackedEntity": grade["trackedEntity"],
                "enrollment": grade["enrollment"],
                "enrollmentStatus": "ACTIVE",
                "occurredAt": grade["time_modified"],
                "dataValues": data_values,
            }
        )

    return payload


def prepare_course_enrollments_events_payload(
    dhis2: DHIS2,
    enrollments: pl.DataFrame,
    certificates: pl.DataFrame,
    courses: pl.DataFrame,
    users: pl.DataFrame,
    program_uid: str,
    program_stage_uid: str,
):
    payload = []
    uids = generate_uid(dhis2, n=len(enrollments))

    # join user data
    enrollments = enrollments.join(other=users, on="user_id", how="left").join(
        other=courses, on="course_id", how="left"
    )

    # join existing events uid
    events = get_events(
        dhis2=dhis2,
        program_uid=ENROLLMENTS_PROGRAM_UID,
        data_values=ENROLLMENTS_DATA_VALUES,
    )
    if (
        "user_id" in events.columns
        and "course_id" in events.columns
        and "course_stage" in events.columns
    ):
        events = events.with_columns(
            [pl.col("user_id").cast(int), pl.col("course_id").cast(int)]
        )
        enrollments = enrollments.join(
            other=events.select(
                ["event", "trackedEntity", "course_id", "course_stage"]
            ),
            on=["trackedEntity", "course_id", "course_stage"],
            how="left",
        )

    # certificate issued?
    enrollments = enrollments.with_columns(
        pl.struct(["user_id", "course_id"])
        .map_elements(
            lambda x: not certificates.filter(
                (pl.col("user_id") == x["user_id"])
                & (pl.col("course_id") == x["course_id"])
            ).is_empty()
        )
        .alias("certificate_issued")
    )

    for enrol in enrollments.iter_rows(named=True):
        if not enrol.get("trackedEntity") or not enrol.get("org_unit"):
            continue

        # assign new event uid if it does not exist
        if not enrol.get("event"):
            enrol["event"] = uids.pop()

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

        payload.append(
            {
                "program": program_uid,
                "event": enrol["event"],
                "programStage": program_stage_uid,
                "orgUnit": enrol["org_unit"],
                "trackedEntity": enrol["trackedEntity"],
                "occurredAt": enrol["enrollment_date"],
                "dataValues": data_values,
            }
        )

    return payload


def post(dhis2: DHIS2, payload: dict) -> dict:
    """Push tracked entities, program enrollments or events to DHIS2."""
    for key in payload:
        if key not in ["enrollments", "trackedEntities", "events"]:
            msg = f"Unrecognized object `{key}` in payload"
            current_run.log_error(msg)
            raise ValueError(msg)

    r = dhis2.api.post(
        endpoint="tracker",
        json=payload,
        params={
            "importMode": "COMMIT",
            "importStrategy": "CREATE_AND_UPDATE",
            "validationMode": "FAIL_FAST",
        },
    )

    r.raise_for_status()
    job = r.json()["response"]["id"]

    completed = False
    current_run.log_info(f"Waiting for completion of DHIS2 Tracker import job {job}")
    messages = []

    i = 0
    while not completed:
        r = dhis2.api.get(endpoint=f"tracker/jobs/{job}")
        for step in r:
            if step["message"] not in messages:
                messages.append(step["message"])
                current_run.log_info(step["message"])
            if step["completed"]:
                completed = True

        # wait 1 mn max
        if i > 10:
            sleep(60)
        elif i > 5:
            sleep(30)
        else:
            sleep(3)

        i += 1

    # collect import report when job is done
    r = dhis2.api.get(endpoint=f"tracker/jobs/{job}/report")

    return r


def push_to_tracker(
    dhis2: DHIS2,
    tracked_entities: dict = None,
    enrollments: dict = None,
    events: dict = None,
) -> str:
    """Push tracked entities, program enrollments or events to DHIS2."""
    if not tracked_entities and not enrollments and not events:
        current_run.log_error("No payload provided")
        return

    payload = {}
    if tracked_entities:
        payload["trackedEntities"] = tracked_entities[0:10]
    if enrollments:
        payload["enrollments"] = enrollments[0:10]
    if events:
        payload["events"] = events[0:10]

    # start import job

    r = dhis2.api.post(
        endpoint="tracker",
        json=payload,
        params={
            "async": True,
            "reportMode": "FULL",
            "importMode": "VALIDATE",
            "importStrategy": "CREATE_AND_UPDATE",
        },
    )

    job_uid = r.json()["response"]["id"]

    # regularly fetch import job status

    completed = False
    current_run.log_info(
        f"Waiting for completion of DHIS2 Tracker import job {job_uid}"
    )
    messages = []

    i = 0
    while not completed:
        r = dhis2.api.get(endpoint=f"tracker/jobs/{job_uid}")
        for step in r:
            if step["message"] not in messages:
                messages.append(step["message"])
                current_run.log_info(step["message"])
            if step["completed"]:
                completed = True

        if i > 10:
            sleep(60)
        elif i > 5:
            sleep(30)
        else:
            sleep(3)

        i += 1

    # collect import report when job is done

    r = dhis2.api.get(endpoint=f"tracker/jobs/{job_uid}/report")
    return r
