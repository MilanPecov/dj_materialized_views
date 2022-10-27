from django.db import connection


def run_custom_sql(*sql):
    """
    Run SQL query and close the connection

    :param sql: sql string
    :return: None
    """

    with connection.cursor() as cursor:
        cursor.execute(*sql)
