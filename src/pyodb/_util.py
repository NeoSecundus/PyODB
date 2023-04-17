import secrets


UID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
def generate_uid(length: int = 32) -> str:
    """Generates a UID consisting of 30 ASCII letters and digits

    Args:
        length (int): Length of the resulting UID. Defaults to 32.

    Returns:
        str: ASCII UID
    """
    return "".join([secrets.choice(UID_CHARS) for _ in range(length)])
