import psycopg2

def get_connection(dbname):

    connect_str = "dbname={} host='localhost' user='xxx' password='xxx'".format(dbname)

    return psycopg2.connect(connect_str)
