
def sanitize_type(type):
    """
    Given a potentially non-array type instance, return an array type instance
    :param type: str, array
    :return: [type ...]
    """

    if isinstance(type, str):
        return [type]

    return type
