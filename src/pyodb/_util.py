import secrets


def generate_uid(length: int = 32) -> str:
    """Generates a UID consisting of {length} ASCII letters and digits

    Args:
        length (int): Length of the resulting UID. Defaults to 32. Must be even!

    Returns:
        str: ASCII UID
    """
    return secrets.token_urlsafe(length)[:length]
