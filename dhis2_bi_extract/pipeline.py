"""Template for newly generated pipelines."""
import json
import os
from openhexa.sdk import current_run, pipeline
import requests
import polars as pl
from datetime import datetime, date
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.sdk import workspace

from sqlalchemy import create_engine, Integer, String

from helper import (
    TRACKED_ENTITY_TYPE,
    TRACKED_ENTITY_TYPE,
    DHIS2_DATE_FORMAT,
    get_org_units,
    process_simple_event,
    process_tracker_event,
    get_tracked_entities,
    process_dx_names

)


@pipeline("dhis2-bi-extract", name="Dhis2 Bi extract")
def dhis2_bi_extract():
    
    con = workspace.dhis2_connection("lifenet")
    monitrac = DHIS2(con)
    get_events(monitrac)

    get_aggregate(monitrac)


@dhis2_bi_extract.task
def get_events(monitrac: DHIS2):
    current_run.log_info("Fetching events data")
    PROG_PARAMS = {
        "fields": "name,id,code,programType,organisationUnits,programStages",
        "paging": False
    }
    r = monitrac.api.get(
        endpoint="programs",
        params=PROG_PARAMS
    )

    programs = []
    for prog in r["programs"]:
        programs.append(prog)

    #Get events from DHIS2
    for program in programs:
        enrollments = pl.DataFrame()
        tracked_entities = pl.DataFrame()
        events = pl.DataFrame()    
        rows = []

        PARAMS = {
            "program": program["id"],
            "ouMode": "ACCESSIBLE",
            "occurredAfter": "2021-01-11",
            "occurredBefore": "2024-11-10",
            "skipPaging": True
        }
        
        r = monitrac.api.get(
            endpoint="tracker/events",
            params=PARAMS
        )
        events_extract = []
        for event in r["instances"]:
            events_extract.append(event)

        code = program["code"]
        current_run.log_info(f"Fetched {len(events_extract)}  {code} Events from DHIS2")
        
        if program["programType"] == "WITH_REGISTRATION":        
            tracked_entities = get_tracked_entities(monitrac, TRACKED_ENTITY_TYPE)
            for event in events_extract:
                row = process_tracker_event(event,DHIS2_DATE_FORMAT)
                rows.append(row)
            events = pl.DataFrame(rows)
            events = process_dx_names(monitrac, events)
            events = events.join(
                other=tracked_entities,
                left_on=["tracked_entity", "ou_id"],
                right_on=["trackedEntity", "orgUnit"],
                how="left",
            )
        else:
            for event in events_extract:
                row = process_simple_event(event,DHIS2_DATE_FORMAT)
                rows.append(row)
            events = pl.DataFrame(rows)
            events = process_dx_names(monitrac, events)

        engine = create_engine(workspace.database_url)
        data = events.to_pandas()

        # Write data into db
        data.to_sql(program["code"], con=engine, if_exists="replace", index_label="id", chunksize=100)
        current_run.log_info(f"Successflly oaded {len(data)} {code} Events into database")


@dhis2_bi_extract.task
def get_aggregate(monitrac: DHIS2):
    current_run.log_info(f"Fetching aggregate data")

    start_date = "2020-01-01"
    today = date.today()
    end_date = today.strftime("%Y-%m-%d")

    r = monitrac.api.get(
        "dataSets",
        params={
            "paging": False,
            "fields": "name,id,dataSetElements,organisationUnits"
        }
    )

    data_sets = []
    for s in r["dataSets"]:
        data_sets.append(s)
    current_run.log_info(f"Processing {len(data_sets)} datasets")

    for ds in data_sets:
        orgUnits = get_org_units(ds["organisationUnits"])
        ds_name = ds["name"]
        current_run.log_info(f"Processing the {ds_name} dataset")
        ds_ids = []
        ds_ids.append(ds["id"])
        ou_count = len(orgUnits)
        if ou_count > 0: 
            data_values = monitrac.data_value_sets.get(
                datasets=ds_ids,
                org_units=orgUnits,
                start_date=start_date,
                end_date=end_date
            )

            df = pl.DataFrame(data_values) 
            values_count = len(df)
            if values_count > 0: 
                df = monitrac.meta.add_dx_name_column(df, dx_id_column="dataElement")
                df = monitrac.meta.add_coc_name_column(df, coc_column="categoryOptionCombo")
                df = monitrac.meta.add_org_unit_name_column(df, org_unit_id_column="orgUnit")
                
                # Write data into OX db
                engine = create_engine(workspace.database_url)
                data = df.to_pandas()
                data.to_sql(ds_name, con=engine, if_exists="replace", index_label="id", chunksize=100)
                current_run.log_info(f"Loading {len(data)} {ds_name} aggregate records into database")        
        else :
            current_run.log_info(f"Dataset {ds_name} has no assigned org units")

if __name__ == "__main__":
    dhis2_bi_extract()