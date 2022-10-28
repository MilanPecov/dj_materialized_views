from django.db import connection


def execute_raw_sql(*sql):
    """
    Execute SQL query and close the connection

    :param sql: sql string
    :return: None
    """

    with connection.cursor() as cursor:
        cursor.execute(*sql)
