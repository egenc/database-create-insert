"""Description:
This script will create a table in a PostgreSQL database and
insert data from a CSV file.
"""
import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    handlers=[
        logging.FileHandler('database.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': '123'
}
FILE_NAME = 'data/data.csv'
COL_NAMES = [
    'ID', 'id', 'seq', 'origin', 'Asn - deamidation risk Cnt', 'Cys Cnt', 'Isomerization Cnt', 'Met Cnt', 'N-Glycosylation Cnt',
    'Pro Cnt', 'Strong Deamidation Cnt', 'Weak Deamidation Cnt', 'SEQUENCE_TYPE', 'STOICHIOMETRY', 'Format', 'Isotype'
]


def create_conn(DB_CONFIG):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def create_table(cur):
    # cur.execute('DROP TABLE IF EXISTS BioSequenceData')
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS BioSequenceData (
            Sequence_ID VARCHAR(255),
            Nucleotide_Sequence TEXT,
            Sequence_Origin TEXT,
            Asn_Deamidation_Risk_Count FLOAT,
            Cysteine_Count FLOAT,
            Isomerization_Count FLOAT,
            Methionine_Count FLOAT,
            N_Glycosylation_Count FLOAT,
            Proline_Count FLOAT,
            Strong_Deamidation_Count FLOAT,
            Weak_Deamidation_Count FLOAT,
            Sequence_Type VARCHAR(255),
            Stoichiometry FLOAT,
            Antibody_Format VARCHAR(255),
            Antibody_Isotype VARCHAR(255)
        );
        """
    )
    if cur.rowcount == -1: # checks if table exists
        logging.info('Table is present - No action needed')
    else:
        logging.info('Table created successfully')

def single_insert(conn, cur, row_data):
    """ Execute a single INSERT request """
    try:
        cur.execute(
            "INSERT INTO BioSequenceData(Sequence_ID, Nucleotide_Sequence, Sequence_Origin, Asn_Deamidation_Risk_Count, Cysteine_Count, Isomerization_Count, Methionine_Count, N_Glycosylation_Count, Proline_Count, Strong_Deamidation_Count, Weak_Deamidation_Count, Sequence_Type, Stoichiometry, Antibody_Format, Antibody_Isotype) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
            row_data
        )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()


def insert_data(conn, cur, dataframe):
    for i, row in dataframe.iterrows():
        single_insert(
            conn,
            cur,
            (
                row['id'],
                row['seq'],
                row['origin'],
                row.get('Asn - deamidation risk Cnt', None),
                row.get('Cys Cnt', None),
                row.get('Isomerization Cnt', None),
                row.get('Met Cnt', None),
                row.get('N-Glycosylation Cnt', None),
                row.get('Pro Cnt', None),
                row.get('Strong Deamidation Cnt', None),
                row.get('Weak Deamidation Cnt', None),
                row.get('SEQUENCE_TYPE', None),
                row.get('STOICHIOMETRY', None),
                row.get('Format', None),
                row.get('Isotype', None)
            )
        )
    logger.info('Data has been successfully inserted to table')


def read_and_transform_df(dataframe, columns):
    dataframe = pd.read_csv(FILE_NAME, usecols=columns, low_memory=False)
    # # Assuming ID is not a unique identifier and just an incrementation of the index
    dataframe = dataframe[dataframe['ID'].notna()].set_index('ID', drop=True)
    return dataframe

def feed_csv(dataframe):
    conn = create_conn(DB_CONFIG)
    if conn is not None:
        cur = conn.cursor()
        create_table(cur)
        df = read_and_transform_df(dataframe, COL_NAMES)
        # Assume df is the DataFrame you want to insert
        insert_data(conn, cur, df)
        cur.close()
        conn.close()

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        # Check if the created file is a CSV file
        if event.src_path.endswith(".csv"):
            # Process the newly created CSV file
            feed_csv(event.src_path)
            

if __name__ == "__main__":
    feed_csv(FILE_NAME)
    # Initialize the watchdog observer
    observer = Observer()
    event_handler = CSVHandler()

    # Schedule the observer to watch the input folder for new CSV files
    observer.schedule(event_handler, path='data', recursive=False)

    # Start the observer in the background
    observer.start()

    try:
        while True:
            time.sleep(1)  # Keep the script running

    except KeyboardInterrupt:
        observer.stop()

    observer.join()
