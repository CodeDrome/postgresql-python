import datetime

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import pgconnection


def main():

    print("------------------")
    print("| codedrome.com  |")
    print("| PostgreSQL DML |")
    print("------------------\n")

    insert_galleries()
    insert_photos()
    insert_typesdemo()

    #update_photos()

    #delete_photos()

    #select_photos()

    #select_galleriesphotos()

    #insert_photo_invalid_fk()

    #delete_fk()

    #reset_serial()


def insert_galleries():

    galleries = ({"name": "London 2018", "description": "Photos of London in 2018"},
                 {"name": "Paris 2016", "description": "Photos of Paris in 2016"},
                 {"name": "Oslo 2018", "description": "Photos of Oslo in 2018"},
                 {"name": "Copenhagen 2017", "description": "Photos of Copenhagen in 2017"},
                 {"name": "Edinburgh 2015", "description": "Photos of Edinburgh in 2015"})

    try:

        conn = pgconnection.get_connection("codeinpython")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        for gallery in galleries:

            cursor.execute("""INSERT INTO galleries(name, description)
                              VALUES (%(name)s, %(description)s);""",
                              {'name': gallery["name"], 'description': gallery["description"]})

            print("Gallery inserted")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def insert_photos():

    photos = ({"galleryid": 1, "title": "London Photo 1", "description": "London Photo 1", "photographer": "Chris Webb", "datetaken": datetime.date(2018, 5, 17)},
              {"galleryid": 1, "title": "London Photo 2", "description": "London Photo 2", "photographer": "Chris Webb", "datetaken": datetime.date(2018, 5, 18)},
              {"galleryid": 2, "title": "Paris Photo 1", "description": "Paris Photo 1", "photographer": "Chris Webb", "datetaken": datetime.date(2016, 9, 1)},
              {"galleryid": 2, "title": "Paris Photo 2", "description": "Paris Photo 2", "photographer": "Chris Webb", "datetaken": datetime.date(2016, 9, 1)},
              {"galleryid": 3, "title": "Oslo Photo 1", "description": "Oslo Photo 1", "photographer": "Chris Webb", "datetaken": datetime.date(2018, 7, 5)},
              {"galleryid": 3, "title": "Oslo Photo 2", "description": "Oslo Photo 2", "photographer": "Chris Webb", "datetaken": datetime.date(2018, 7, 5)},
              {"galleryid": 4, "title": "Copenhagen Photo 1", "description": "Copenhagen Photo 1", "photographer": "Chris Webb", "datetaken": datetime.date(2017, 4, 12)},
              {"galleryid": 4, "title": "Copenhagen Photo 2", "description": "Copenhagen Photo 2", "photographer": "Chris Webb", "datetaken": datetime.date(2017, 4, 13)},
              {"galleryid": 5, "title": "Edinburgh Photo 1", "description": "Edinburgh Photo 1", "photographer": "Chris Webb", "datetaken": datetime.date(2015, 8, 21)},
              {"galleryid": 5, "title": "Edinburgh Photo 2", "description": "Edinburg Photo 2", "photographer": "Chris Webb", "datetaken": datetime.date(2015, 8, 21)})

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        for photo in photos:

            cursor.execute("""INSERT INTO photos(galleryid, title, description, photographer, datetaken)
                              VALUES (%(galleryid)s, %(title)s, %(description)s, %(photographer)s, %(datetaken)s);""",
                              photo)

            conn.commit()

            print("Photo inserted")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def insert_typesdemo():

    row = {"intcolumn": 123, "realcolumn": 456.789, "varcharcolumn": "Now is the winter of our discontent", "datecolumn": datetime.date(2018, 2, 17), "booleancolumn": True}

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        cursor.execute("""INSERT INTO typesdemo(intcolumn, realcolumn, varcharcolumn, datecolumn, booleancolumn)
                          VALUES (%(intcolumn)s, %(realcolumn)s, %(varcharcolumn)s, %(datecolumn)s, %(booleancolumn)s);""",
                          row)

        conn.commit()

        print("typesdemo row inserted")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def update_photos():

    update_dict = {"description": "Edinburgh Photo 2", "photoid": 10}

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        cursor.execute("UPDATE photos SET description = %(description)s WHERE photoid = %(photoid)s;", update_dict)

        conn.commit()

        print("Photo updated")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def delete_photos():

    delete_dict = {"photoid": 10}

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        cursor.execute("DELETE FROM photos WHERE photoid = %(photoid)s;", delete_dict)

        conn.commit()

        print("Photo deleted")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def select_photos():

    # Wildcards:
    # percent sign % for 0 or more characters
    # underscore _ for exactly 1 character
    select_dict = {"where_like": '%Oslo%'}

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        # To make LIKE case insensitive use ILIKE
        cursor.execute("SELECT galleryid, title, description, photographer, datetaken FROM photos WHERE description LIKE %(where_like)s;", select_dict)

        # get a list of tuples containing the data
        data = cursor.fetchall()

        cursor.close()
        conn.close()

        for row in data:

            print(row)

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def select_galleriesphotos():

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        cursor.execute("SELECT galleryname, gallerydescription, phototitle, photodescription FROM galleriesphotos")

        # get a list of tuples containing the data
        data = cursor.fetchall()

        cursor.close()
        conn.close()

        for row in data:

            print(row)

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def insert_photo_invalid_fk():

    invalid_photo = {"galleryid": 6, "title": "Hong Kong Photo 1", "description": "Hong Kong Photo 1", "photographer": "Chris Webb", "datetaken": datetime.date(2018, 8, 27)}

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        cursor.execute("""INSERT INTO photos(galleryid, title, description, photographer, datetaken)
                          VALUES (%(galleryid)s, %(title)s, %(description)s, %(photographer)s, %(datetaken)s);""",
                          invalid_photo)

        conn.commit()

        print("Photo inserted")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def delete_fk():

    delete_dict = {"galleryid": 1}

    try:

        conn = pgconnection.get_connection("codeinpython")
        cursor = conn.cursor()

        cursor.execute("DELETE FROM galleries WHERE galleryid = %(galleryid)s;", delete_dict)

        conn.commit()

        print("Gallery deleted")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


def reset_serial():

    try:

        conn = pgconnection.get_connection("codeinpython")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute("ALTER SEQUENCE photos_photoid_seq RESTART WITH 1")

        print("photo table serial reset")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:

        print(type(e))

        print(e)


main()
