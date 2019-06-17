


class CheapoDBException(Exception):
    pass


def normalize_table_name(name):
    """Check if the table name is obviously invalid."""
    if not isinstance(name, str):
        raise ValueError()
    name = name.strip()
    if not len(name):
        raise ValueError(f'Invalid table name: {name}')
    return name
