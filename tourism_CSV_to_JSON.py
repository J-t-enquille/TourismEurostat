import pandas as pd
import json
from datetime import datetime
import pytz

# ==============================
# Data from main mode of transport
# ==============================

cols_wanted = [
    "geo",              # Geopolitical entity (reporting)
    "TIME_PERIOD",      # Year
    "c_dest",           # Country of destination
    "purpose",
    "duration",
    "tra_mode",         # Mode of transport
    "OBS_VALUE"
]

df = pd.read_csv("data/Trips_by_main_mode_of_transport.csv", usecols=cols_wanted)

group_cols = [
    "geo",
    "TIME_PERIOD",
    "c_dest",
    "purpose",
    "duration",
    "tra_mode"
]
field_names = [
    "geo",
    "TIME_PERIOD",
    "c_dest",
    "purpose",
    "duration",
    "tra_mode",
    "month"
]

with open("data/Trips_by_main_mode_of_transport.json", encoding="utf-8") as jf:
    meta_transport = json.load(jf)

extension_transport = meta_transport.get("extension", {})
annotations_transport = extension_transport.get("annotation", [])

title_transport = meta_transport.get("label")
author_transport = None
description_transport = None
creation_date_transport = None
last_update_transport = None
last_update_structure_transport = None
doi_transport = None
id_transport = extension_transport.get("id")
link_transport = "https://ec.europa.eu/eurostat/databrowser/view/tour_dem_tttr/default/table?lang=en"

for ann in annotations_transport:
    ann_type = ann.get("type")
    if ann_type == "CREATED":
        creation_date_transport = ann.get("date")
    elif ann_type == "UPDATE_DATA":
        last_update_transport = ann.get("date")
    elif ann_type == "UPDATE_STRUCTURE":
        last_update_structure_transport = ann.get("date")
    elif ann_type == "DISSEMINATION_DOI_XML":
        import re
        match = re.search(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', ann.get("title", ""), re.I)
        if match:
            doi_transport = match.group()
    elif ann_type == "SOURCE_INSTITUTIONS":
        author_transport = ann.get("text")

time_zone = pytz.timezone("Europe/Paris")
date_local = datetime.now(time_zone).isoformat()


def create_json(df, group_cols, field_names, value_col, level=0):
    if level >= len(group_cols):
        vals = df[value_col].tolist()
        return vals[0] if len(vals) == 1 else vals
    col = group_cols[level]
    label = field_names[level]
    out = {}
    for key, sub in df.groupby(col):
        out[key] = create_json(sub, group_cols, field_names, value_col, level + 1)
    return {label: out}


df_json = {
    "metadata": {
        "source_transport": {
            "title": title_transport,
            "author": author_transport,
            "description": description_transport,
            "DOI": doi_transport,
            "online_id": id_transport,
            "link": link_transport,
            "creation_date": creation_date_transport,
            "last_update": last_update_transport,
            "last_update_structure": last_update_structure_transport,
        }
    },
    "data": create_json(
        df,
        group_cols,
        field_names,
        value_col="OBS_VALUE"
    )
}

# ==============================
# Data from month of departure
# ==============================

month_cols = [
    "geo",
    "TIME_PERIOD",
    "c_dest",
    "purpose",
    "duration",
    "month",
    "OBS_VALUE"
]
df_month = pd.read_csv("data/Trips_by_month_of_departure.csv", usecols=month_cols)

with open("data/Trips_by_month_of_departure.json", encoding="utf-8") as jf:
    meta_month = json.load(jf)

extension_month = meta_month.get("extension", {})
annotations_month = extension_month.get("annotation", [])

title_month = meta_month.get("label")
author_month = None
description_month = None
creation_date_month = None
last_update_month = None
last_update_structure_month = None
doi_month = None
id_month = extension_month.get("id")
link_month = "https://ec.europa.eu/eurostat/databrowser/view/tour_dem_ttmd/default/table?lang=en"

for ann in annotations_month:
    ann_type = ann.get("type")
    if ann_type == "CREATED":
        creation_date_month = ann.get("date")
    elif ann_type == "UPDATE_DATA":
        last_update_month = ann.get("date")
    elif ann_type == "UPDATE_STRUCTURE":
        last_update_structure_month = ann.get("date")
    elif ann_type == "DISSEMINATION_DOI_XML":
        import re
        match = re.search(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', ann.get("title", ""), re.I)
        if match:
            doi_month = match.group()
    elif ann_type == "SOURCE_INSTITUTIONS":
        author_month = ann.get("text")

for _, row in df_month.iterrows():
    country_of_departure = row["geo"]
    year = row["TIME_PERIOD"]
    country_of_destination = row["c_dest"]
    purpose = row["purpose"]
    duration = row["duration"]
    month = row["month"]
    obs_value = row["OBS_VALUE"]

    navigation = (
        df_json
        ["data"]
        ["geo"][country_of_departure]
        ["TIME_PERIOD"][year]
        ["c_dest"][country_of_destination]
        ["purpose"][purpose]
        ["duration"][duration]
    )

    if "month" not in navigation:
        navigation["month"] = {}
    navigation["month"][month] = obs_value

df_json["metadata"]["source_month"] = {
    "title": title_month,
    "author": author_month,
    "description": description_month,
    "DOI": doi_month,
    "online_id": id_month,
    "link": link_month,
    "creation_date": creation_date_month,
    "last_update": last_update_month,
    "last_update_structure": last_update_structure_month,
}

# ==============================
# Data from income quartile
# ==============================

income_cols = [
    "geo",
    "TIME_PERIOD",
    "c_dest",
    "purpose",
    "duration",
    "quant_inc",
    "OBS_VALUE"
]
df_income = pd.read_csv("data/Trips_by_household_income_quartile_of_the_tourist.csv", usecols=income_cols)

with open("data/Trips_by_household_income_quartile_of_the_tourist.json", encoding="utf-8") as jf:
    meta_income = json.load(jf)

extension_income = meta_income.get("extension", {})
annotations = extension_income.get("annotation", [])

title_income = meta_income.get("label")
author_income = None
description_income = None
creation_date_income = None
last_update_income = None
last_update_structure_income = None
doi_income = None
id_income = extension_income.get("id")
link_income = "https://ec.europa.eu/eurostat/databrowser/view/tour_dem_ttinc/default/table?lang=en"

for ann in annotations:
    ann_type = ann.get("type")
    if ann_type == "CREATED":
        creation_date_income = ann.get("date")
    elif ann_type == "UPDATE_DATA":
        last_update_income = ann.get("date")
    elif ann_type == "UPDATE_STRUCTURE":
        last_update_structure_income = ann.get("date")
    elif ann_type == "DISSEMINATION_DOI_XML":
        import re
        match = re.search(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', ann.get("title", ""), re.I)
        if match:
            doi_income = match.group()
    elif ann_type == "SOURCE_INSTITUTIONS":
        author_income = ann.get("text")

for _, row in df_income.iterrows():
    country_of_departure = row["geo"]
    year = row["TIME_PERIOD"]
    country_of_destination = row["c_dest"]
    purpose = row["purpose"]
    duration = row["duration"]
    income_quantile = row["quant_inc"]
    obs_value = row["OBS_VALUE"]

    navigation = (
        df_json
        ["data"]
        ["geo"][country_of_departure]
        ["TIME_PERIOD"][year]
        ["c_dest"][country_of_destination]
        ["purpose"][purpose]
        ["duration"][duration]
    )

    if "quant_inc" not in navigation:
        navigation["quant_inc"] = {}
    navigation["quant_inc"][income_quantile] = obs_value

df_json["metadata"]["source_income"] = {
    "title": title_income,
    "author": author_income,
    "description": description_income,
    "DOI": doi_income,
    "online_id": id_income,
    "link": link_income,
    "creation_date": creation_date_income,
    "last_update": last_update_income,
    "last_update_structure": last_update_structure_income,
}

# ==============================
# Last JSON parameters
# ==============================

with open("tourism_eurostat.json", "w", encoding="utf-8") as f:
    json.dump(df_json, f, ensure_ascii=False, indent=2)

# df_json["data"]["Country of departure"].keys() = list all departure country
# df_json["data"]["Country of departure"]["Albania"]["Year"].keys() = list all years for Albania
# df_json["data"]
#        ["Country of departure"]["Netherlands"]
#        ["Year"][2017]
#        ["Country of destination"]["All countries of the world"]
#        ["Purpose"]["Personal reasons"]
#        ["Duration"]["1 night or over"]
#        ["Income quantile"].keys()

