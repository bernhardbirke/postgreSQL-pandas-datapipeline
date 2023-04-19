import psycopg2
from postgresql_config import config


def read_current_watt():
    """ query momentanleistung_p from the smartmeter table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(
            "SELECT data_id, created_on, momentanleistung_p FROM smartmeter ORDER BY data_id DESC LIMIT 5")
        watt_row = cur.fetchone()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        return watt_row


def create_table_smartmeter():
    """ create table smartmeter in the PostgreSQL database (specified in config.py)"""
    command = (
        """
        CREATE TABLE smartmeter (
            data_id SERIAL PRIMARY KEY,
            time TIMESTAMP NOT NULL,
            wirkenergie_p FLOAT4,
            wirkenergie_n FLOAT4,
            momentanleistung_p FLOAT4,
            momentanleistung_n FLOAT4,
            spannung_l1 FLOAT4,
            spannung_l2 FLOAT4,
            spannung_l3 FLOAT4,
            strom_l1 FLOAT4,
            strom_l2 FLOAT4,
            strom_l3 FLOAT4,
            leistungsfaktor FLOAT4
        )
        """)
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_table_smartmeter_diff():
    """ create table smartmeter in the PostgreSQL database (specified in config.py). Units:  wirkenergie_p_diff [wh],  wirkenergie_n_diff [wh]"""
    command = (
        """
        CREATE TABLE smartmeter_diff (
            data_id INT PRIMARY KEY,
            time TIMESTAMP NOT NULL,
            wirkenergie_p_diff FLOAT4,
            wirkenergie_n_diff FLOAT4,
            CONSTRAINT fk_data_id
                FOREIGN KEY(data_id)
                    REFERENCES smartmeter(data_id)
                    ON DELETE SET NULL
                    ON UPDATE CASCADE
        )
        """)
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_smartmeter(WirkenergieP, WirkenergieN, MomentanleistungP, MomentanleistungN, SpannungL1, SpannungL2, SpannungL3, StromL1, StromL2, StromL3, Leistungsfaktor):
    """ insert a new data row into the smartmeter table """
    sql = """INSERT INTO smartmeter(created_on, wirkenergie_p, wirkenergie_n, momentanleistung_p, momentanleistung_n, spannung_l1, spannung_l2, spannung_l3, strom_l1, strom_l2, strom_l3, leistungsfaktor)
            VALUES(NOW()::TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING data_id;"""
    conn = None
    data_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (WirkenergieP, WirkenergieN, MomentanleistungP, MomentanleistungN,
                    SpannungL1, SpannungL2, SpannungL3, StromL1, StromL2, StromL3, Leistungsfaktor))
        # get the generated id back
        data_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return data_id

    # to test a specific function via "python postgresql_tasks.py" in the powershell
if __name__ == '__main__':
  #  create_table_smartmeter()
   # insert_smartmeter(1234.1339491293948, 45.2, 0.023, 2.39, 230,
   #                   240.3, 222.23, 50, 51.4, 49.3, 0.56)
    create_table_smartmeter_diff()
