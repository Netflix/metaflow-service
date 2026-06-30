"""Filter-condition grammar shared by the metadata service and ui_backend.

Pure functions that parse query params (a MultiDict) into parameterised SQL
(conditions, values). No web/aiohttp dependency, so either service can import this
without pulling in the other's web layer. Conditions use %s placeholders with a
separate values list, so user input is always bound, never interpolated.

The functions are lifted verbatim from services/ui_backend_service/api/utils.py so
the two services parse filters identically. Once the shared-module location is
confirmed, ui_backend should import these from here instead of keeping its own copy.
"""

from typing import List
from multidict import MultiDict


# Built-in conditions, always prefixed with '_' (currently just _tags).
def builtin_conditions_query_dict(query: MultiDict):
    conditions = []
    values = []

    for key, val in query.items():
        if not key.startswith("_"):
            continue

        deconstruct = key.split(":", 1)
        if len(deconstruct) > 1:
            field = deconstruct[0]
            operator = deconstruct[1]
        else:
            field = key
            operator = None

        # Tags
        if field == "_tags":
            tags = val.split(",")
            if operator == "likeany" or operator == "likeall":
                # ?_tags:likeany => LIKE ANY (OR), ?_tags:likeall => LIKE ALL (AND)
                compare = "ANY" if operator == "likeany" else "ALL"
                conditions.append(
                    "tags||system_tags::text LIKE {}(array[{}])".format(
                        compare, ",".join(["%s"] * len(tags))
                    )
                )
                values += map(lambda t: "%{}%".format(t), tags)
            else:
                # ?_tags:any => ?| (OR), ?_tags:all => ?& (AND, the default)
                compare = "?|" if operator == "any" else "?&"
                conditions.append(
                    "tags||system_tags {} array[{}]".format(
                        compare, ",".join(["%s"] * len(tags))
                    )
                )
                values += tags

    return conditions, values


operators_to_sql = {
    "eq": '"{}" = %s',  # equals
    "ne": '"{}" != %s',  # not equals
    "lt": '"{}" < %s',  # less than
    "le": '"{}" <= %s',  # less than or equals
    "gt": '"{}" > %s',  # greater than
    "ge": '"{}" >= %s',  # greater than or equals
    "co": '"{}" ILIKE %s',  # contains
    "sw": '"{}" ILIKE %s',  # starts with
    "ew": '"{}" ILIKE %s',  # ends with
    "li": '"{}" ILIKE %s',  # ILIKE (caller supplies % in the value)
    "is": '"{}" IS %s',  # IS
}

operators_to_sql_values = {
    "eq": "{}",
    "ne": "{}",
    "lt": "{}",
    "le": "{}",
    "gt": "{}",
    "ge": "{}",
    "co": "%{}%",
    "sw": "{}%",
    "ew": "%{}",
    "li": "{}",
    "is": "{}",
}


# Custom conditions on table columns, never prefixed with '_'.
def custom_conditions_query_dict(query: MultiDict, allowed_keys: List[str] = []):
    conditions = []
    values = []

    for key, val in query.items():
        if key.startswith("_"):
            continue

        deconstruct = key.split(":", 1)
        if len(deconstruct) > 1:
            field = deconstruct[0]
            operator = deconstruct[1]
        else:
            field = key
            operator = "eq"

        if allowed_keys is not None and field not in allowed_keys:
            continue

        if operator not in operators_to_sql:
            continue

        vals = val.split(",")

        conditions.append(
            "({})".format(
                " OR ".join(
                    map(
                        lambda v: operators_to_sql[
                            "is" if v == "null" else operator
                        ].format(field),
                        vals,
                    )
                )
            )
        )
        values += map(
            lambda v: (
                None if v == "null" else operators_to_sql_values[operator].format(v)
            ),
            vals,
        )

    return conditions, values
