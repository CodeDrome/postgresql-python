from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2

import pgconnection


def main():

    """
    Demonstrate creation of PostgreSQL database, tables and views using psycopg2
    """

    print("------------------")
    print("| codedrome.com  |")
    print("| PostgreSQL DDL |")
    print("------------------\n")

    # Create a tuple of dictionaries containing the SQL to create database, tables and views
    queries = ({"Description": "Create database",
                "Database": "postgres",
                "SQL": "CREATE DATABASE codeinpython"},

               {"Description": "Create galleries table ",
                "Database": "codeinpython",
                "SQL": "CREATE TABLE galleries(galleryid serial PRIMARY KEY, name varchar(64) NOT NULL, description varchar(256))"},

               {"Description": "Create photos table ",
                "Database": "codeinpython",
                "SQL": "CREATE TABLE photos(photoid serial PRIMARY KEY, galleryid smallint REFERENCES galleries(galleryid) NOT NULL, title varchar(64) NOT NULL, description varchar(256) NOT NULL, photographer varchar(64) NOT NULL, datetaken date)"},

               {"Description": "Create typesdemo table ",
                "Database": "codeinpython",
                "SQL": "CREATE TABLE typesdemo(serialid serial PRIMARY KEY, intcolumn integer, realcolumn real, varcharcolumn varchar(64), datecolumn date, booleancolumn boolean)"},

               {"Description": "Create view galleriesphotos ",
                "Database": "codeinpython",
                "SQL": "CREATE VIEW galleriesphotos AS SELECT galleries.name AS galleryname, galleries.description AS gallerydescription, photos.title AS phototitle, photos.description AS photodescription FROM galleries LEFT JOIN photos ON photos.galleryid = galleries.galleryid"})

    # iterate and run queries
    try:

        for query in queries:

            conn = pgconnection.get_connection(query["Database"])
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute(query["SQL"])

            print("Executed {}".format(query["Description"]))

            cursor.close()
            conn.close()

    except psycopg2.ProgrammingError as e:

        print(e)


main()
