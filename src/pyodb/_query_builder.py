"""The main module which processes python primitives (and lists and dicts) and generates sql
statements.
Including create and remove table statements in case the fields contained by a class were changed.
"""

# SQLITE3 Available Data Types:
# TEXT = str, list, dict
# NUMERIC = int, float, date, datetime
#   ^Converts float to int if float does not have decimal places (10.01 => float, 10.0 = int)
# INTEGER = int
# REAL = float
# BLOB = binary or undefined type
