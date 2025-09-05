import datetime
from django.db import models, transaction

def normalize_row_for_model(row: dict, model) -> dict:
    """
    Normalize row values automatically based on model field definitions.
    Handles nulls, booleans, integers, floats, and dates.
    """
    normalized = {}

    for field in model._meta.fields:
        name = field.name
        val = row.get(name)

        # Skip if field not in row
        if name not in row:
            continue

        # Handle null-like values
        if val in ["", None, "NULL", "null"]:
            normalized[name] = None
            continue

        # Boolean normalization
        if isinstance(field, models.BooleanField):
            if isinstance(val, str):
                normalized[name] = val.strip().lower() in ["sim", "true", "1", "yes"]
            else:
                normalized[name] = bool(val)
            continue

        # Integer normalization
        if isinstance(field, models.IntegerField):
            try:
                normalized[name] = int(val)
            except (TypeError, ValueError):
                normalized[name] = None
            continue

        # Float normalization
        if isinstance(field, models.FloatField):
            try:
                if isinstance(val, str):
                    val = val.replace(",", ".")
                normalized[name] = float(val)
            except (TypeError, ValueError):
                normalized[name] = None
            continue

        # Date normalization
        if isinstance(field, models.DateField):
            try:
                if isinstance(val, str):
                    try:
                        # First try ISO (YYYY-MM-DD)
                        normalized[name] = datetime.date.fromisoformat(val)
                    except ValueError:
                        # Then try DD/MM/YYYY
                        normalized[name] = datetime.datetime.strptime(val, "%d/%m/%Y").date()
                else:
                    normalized[name] = val
            except Exception:
                normalized[name] = None
            continue

        # Default: keep value as-is
        normalized[name] = val

    return normalized


def upsert(model, rows: list[dict], unique_by: str, update_fields: list[str]):
    """
    Generic bulk upsert utility.
    Splits rows into new vs existing, then bulk_creates and bulk_updates.

    Args:
        model: Django model class
        rows: list of dicts with data
        unique_by: field name to check uniqueness (e.g. "codigo_if")
        update_fields: fields to update if object already exists

    Returns:
        (created_count, updated_count)
    """
    if not rows:
        return (0, 0)

    # 1. Extract all unique keys from payload
    keys = [r.get(unique_by) for r in rows if r.get(unique_by) is not None]

    # 2. Query DB for existing records
    existing = model.objects.filter(**{f"{unique_by}__in": keys})
    existing_map = {getattr(obj, unique_by): obj for obj in existing}

    new_objects = []
    update_objects = []

    for row in rows:
        key = row.get(unique_by)
        if not key:
            continue

        # 🔄 Normalize according to model field types
        row = normalize_row_for_model(row, model)

        if key in existing_map:
            # Update existing object
            obj = existing_map[key]
            for field in update_fields:
                if field in row:
                    setattr(obj, field, row[field])
            update_objects.append(obj)
        else:
            # Prepare new object
            new_objects.append(model(**row))

    created = updated = 0

    # 3. Save in bulk
    with transaction.atomic():
        if new_objects:
            model.objects.bulk_create(new_objects, batch_size=1000)
            created = len(new_objects)
        if update_objects:
            model.objects.bulk_update(update_objects, update_fields, batch_size=1000)
            updated = len(update_objects)

    return (created, updated)
