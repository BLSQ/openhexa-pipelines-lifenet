import json
import polars as pl
from datetime import datetime
from openhexa.toolbox.dhis2 import DHIS2


#parameters
TRACKED_ENTITY_TYPE = "ZDSBUOcHKV2"

DHIS2_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

TRACKED_ENTITY_ATTRIBUTES = { # Tracker Program
    "first_name": "BHg8LLaoolq",
    "last_name": "srPxZ5neKN8",
    "user_id": "CgURgDLJJUi",
    "gender": "InLMldiUBio",
    "position": "MdUwUhekwRQ",
    "phase": "BhEi9vjZxzH",
}


def get_org_units(ous: str):
    ou_ids = []
    for ou in ous:
        ou_ids.append(ou["id"])
    return ou_ids

def get_ds_elements(dse: str):
    de_ids = []
    for de in dse:
        de_ids.append(de["dataElement"]["id"])
    return de_ids

#Process data values
def process_datavalues(datavalues: list[dict]) -> dict:
    """Transform event data values.

    In the request response, event data values are stored as a list of dict
    for each event. Each data value dict has the following keys: createdAt,
    updatedAt, dataElement, and value.

    We transform this list of dict into a single dict, with data element id as
    dict key, and data element value as dict value.

    Args:
        datavalues (list[dict]): raw event data values (one dict per data value).

    Returns:
        dict: output event data values as a dict (one key: value per data value).
    """
    data = {}

    for datavalue in datavalues:

        dx_id = datavalue["dataElement"]
        dx_value = datavalue["value"]

        # convert to integer if possible
        if dx_value.isnumeric():
            dx_value = int(dx_value)

        data[dx_id] = dx_value

    return data

#Process single events
def process_simple_event(event: dict, dhis2_date_format: str) -> dict:
    """Transform raw event responses."""
    
    row = {
        "event_id": event["event"],
        "program_id": event["program"],
        "ou_id": event["orgUnit"],
        "ou_name": event["orgUnitName"],
        "occurred_at": datetime.strptime(event["occurredAt"], dhis2_date_format),
        "updated_at": datetime.strptime(event["updatedAt"], dhis2_date_format)
    }

    if event.get("dataValues"):
        data_values = process_datavalues(event["dataValues"])
        row.update(data_values)

    return row

#Process events
def process_tracker_event(event: dict, dhis2_date_format: str) -> dict:
    """Transform raw event responses."""
    
    row = {
        "event_id": event["event"],
        "tracked_entity": event["trackedEntity"],
        "program_id": event["program"],
        "ou_id": event["orgUnit"],
        "ou_name": event["orgUnitName"],
        "occurred_at": datetime.strptime(event["occurredAt"], dhis2_date_format),
        "updated_at": datetime.strptime(event["updatedAt"], dhis2_date_format)
    }

    if event.get("dataValues"):
        data_values = process_datavalues(event["dataValues"])
        row.update(data_values)

    return row

def get_tracked_entities(dhis2: DHIS2, tracked_entity_type: str) -> pl.DataFrame:
    """Get all tracked entities from DHIS2 for a given tracked entity type."""
    mapping = {v: k for k, v in TRACKED_ENTITY_ATTRIBUTES.items()}
    entities = []

    r = dhis2.api.get(
        "tracker/trackedEntities",
        params={
            "ouMode": "ACCESSIBLE",
            "trackedEntityType": tracked_entity_type,
            "skipPaging": True,
        },
    )

    for entity in json.dumps(r)["instances"]:
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

    for enrol in json.dumps(r)["instances"]:
        enrollments.append(enrol)

    df = pl.DataFrame(enrollments)
    return df

def process_dx_names(dhis2: DHIS2, events: pl.DataFrame) -> pl.DataFrame:
    identifiers = [
        column
        for column in events.columns
        if column
        not in ["event_id", "program_id", "ou_id", "ou_name", "occurred_at", "updated_at"]
    ]

    filter = f"id:in:[{','.join(identifiers)}]"
    data_elements = dhis2.meta.data_elements(filter=filter)
    data_elements = pl.DataFrame(data_elements)
    dx_length = len(data_elements)
    if dx_length > 0:
        dx_names = data_elements.select(["id","name"])
        dx_dict = dx_names.to_dict()
        column_names = dict(zip(dx_names['id'], dx_names['name']))
        events = events.rename(column_names)

    return events
    
