import datetime
from django.db import models

def normalize_row_for_model(row: dict, model_class: models.Model) -> dict:
    """
    Normalize a row dictionary so that:
    - Keys not in model fields are ignored
    - Date strings are converted to date objects
    - Empty strings are treated as None
    """
    normalized = {}
    field_names = {f.name: f for f in model_class._meta.fields}

    for key, value in row.items():
        if key not in field_names:
            continue

        field = field_names[key]

        if value == "" or value is None:
            normalized[key] = None
        elif isinstance(field, (models.DateField, models.DateTimeField)):
            # Try to parse dates in common formats
            if isinstance(value, str):
                try:
                    normalized[key] = datetime.datetime.strptime(value, "%Y-%m-%d").date()
                except ValueError:
                    try:
                        normalized[key] = datetime.datetime.strptime(value, "%d/%m/%Y").date()
                    except ValueError:
                        normalized[key] = None
            else:
                normalized[key] = value
        else:
            normalized[key] = value

    return normalized
