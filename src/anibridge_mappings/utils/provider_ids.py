"""Provider-specific ID normalization helpers."""


def normalize_imdb_id(raw_value: str | None) -> str | None:
    """Normalize an IMDb ID into standard 'tt...' format.

    Args:
        raw_value (str | None): The raw IMDb ID value to normalize.

    Returns:
        str | None: The normalized IMDb ID.
    """
    if not raw_value:
        return None
    lowered = raw_value.strip().lower()
    suffix = lowered.removeprefix("tt")

    if not suffix.isdigit():
        return None
    # Some sources convert IMDb IDs to integers, which can lose leading zeros.
    # This is a little unsafe since we can't be sure of the padding length.
    suffix = suffix.zfill(7)

    # IMDb currently uses up to 8 digits, but might expand in the future.
    if len(suffix) > 9:
        return None
    return f"tt{suffix}"
