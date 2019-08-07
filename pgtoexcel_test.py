import psycopg2

import pgconnection
import pgtoexcel


def main():

    print("-----------------------")
    print("| codedrome.com       |")
    print("| PostgreSQL to Excel |")
    print("-----------------------")

    try:

        conn = pgconnection.get_connection("codeinpython")
        query_string = "SELECT galleryname, gallerydescription, phototitle, photodescription FROM galleriesphotos"
        filepath = "galleriesphotos.xlsx"

        pgtoexcel.export_to_excel(conn,
                                  query_string,
                                  ("Gallery Name", "Gallery Description", "Photo Title", "Photo Description"),
                                  "filepath")

    except Exception as e:
        print(type(e))
        print(e)


main()
