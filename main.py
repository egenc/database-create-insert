"""Description:
This script will create a table in a PostgreSQL database and
insert data from a CSV file.
"""
import time
import logging
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

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
    'Pro Cnt', 'Strong Deamidation Cnt', 'Weak Deamidation Cnt', 'SEQUENCE_TYPE', 'STOICHIOMETRY', 'Format', 'Isotype', 'DATE_CREATED', 'Project'
]


def create_conn(DB_CONFIG):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def create_table(conn):
    # cur.execute('DROP TABLE IF EXISTS BioSequenceData')
    cur = conn.cursor()
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
            Antibody_Isotype VARCHAR(255),
            project_name VARCHAR(255),
            Date_Created DATE 
        );
        """
    )
    cur.close()
    logging.info('Table created successfully')


def single_insert(conn, cur, row_data):
    """ Execute a single INSERT request """
    try:
        cur.execute(
            "INSERT INTO BioSequenceData(Sequence_ID, Nucleotide_Sequence, Sequence_Origin, Asn_Deamidation_Risk_Count, Cysteine_Count, Isomerization_Count, Methionine_Count, N_Glycosylation_Count, Proline_Count, Strong_Deamidation_Count, Weak_Deamidation_Count, Sequence_Type, Stoichiometry, Antibody_Format, Antibody_Isotype, project_name, Date_Created) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
            row_data
        )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()


def insert_data(conn, cur, dataframe):
    """
    Inserts data from a DataFrame into a PostgreSQL database table using the provided database connection and cursor.

    Args:
        conn (psycopg2.extensions.connection): The PostgreSQL database connection.
        cur (psycopg2.extensions.cursor): The database cursor.
        dataframe (pandas.DataFrame): The DataFrame containing data to be inserted.

    Returns:
        None

    Notes:
        This function iterates through the rows of the DataFrame and inserts each row into the specified database table.
        It uses the `single_insert` function to execute individual INSERT requests for each row.
    """

    try:
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
                    row.get('Isotype', None),
                    row.get('Project', None),
                    row.get('DATE_CREATED', None),
                )
            )
        logger.info('Data has been successfully inserted to table')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()


def checkTableExists(conn, tablename):
    """
    Checks if a table exists in the PostgreSQL database connected through the provided connection.

    Args:
        conn (psycopg2.extensions.connection): The PostgreSQL database connection.
        tablename (str): The name of the table to check for existence.

    Returns:
        bool: True if the table exists, False otherwise.

    Notes:
        This function queries the database's information schema to determine if the specified table exists in the 'public' schema.
    """

    dbcur = conn.cursor()
    try:
        dbcur.execute("""
            SELECT * FROM information_schema.tables WHERE table_schema = 'public';
        """)
        if dbcur.fetchone()[2] == tablename.lower():
            dbcur.close()
            return True
        else:
            dbcur.close()
            return False
    except:
        dbcur.close()
        return False


def transform_df(dataframe):
    """
    Transforms a DataFrame by filtering and normalizing its columns.

    Args:
        dataframe (pandas.DataFrame): The DataFrame to be transformed.

    Returns:
        pandas.DataFrame: The transformed DataFrame.

    Notes:
        This function assumes that the DataFrame contains a column named 'ID' that is not a unique identifier but
        represents an incrementing index. It also assumes the presence of a 'DATE_CREATED' column that is normalized
        to the 'YYYY-MM-DD' format for database compatibility.
    """

    # Assuming ID is not a unique identifier and just an incrementation of the index
    dataframe = dataframe[dataframe['ID'].notna()].set_index('ID', drop=True)

    # Normalize Date column for database (SQL) language
    dataframe['DATE_CREATED'] = dataframe['DATE_CREATED'].apply(lambda x: pd.to_datetime(
        x, format='%d/%m/%Y', errors='coerce').strftime('%Y-%m-%d') if isinstance(x, str) and x.strip() else None)

    return dataframe


def feed_csv(csv_file, columns):
    """
    Reads data from a CSV file, processes it, and inserts it into a PostgreSQL database table.

    Args:
        csv_file (str): The path to the CSV file containing the data to be inserted.
        columns (list): A list of column names to be used when processing the CSV file.

    Returns:
        None

    Notes:
        This function assumes that a PostgreSQL database connection has been established using the `create_conn`
        function and a table named 'BioSequenceData' may or may not exist in the database.
    """

    conn = create_conn(DB_CONFIG)
    if conn is not None:
        cur = conn.cursor()
        if checkTableExists(conn, 'BioSequenceData'):
            print('----inserting table----')
        else:
            print('----creating table----')
            create_table(conn)

        dataframe = pd.read_csv(
            csv_file, usecols=columns, low_memory=False)
        df = transform_df(dataframe)
        # Assume df is the DataFrame you want to insert
        insert_data(conn, cur, df)
        cur.close()
        conn.close()


class CSVHandler(FileSystemEventHandler):
    """
    A custom event handler class to monitor the creation of CSV files and process them.

    Args:
        columns (list): A list of column names to be used when processing the CSV file.

    Attributes:
        column_names (list): The list of column names to be used when processing the CSV file.

    Methods:
        on_created(event): Overrides the on_created method of the base class to process newly created CSV files.
    """

    def __init__(self, columns):
        self.column_names = columns

    def on_created(self, event):
        """
        Overrides the on_created method of the base class to process newly created CSV files.

        Args:
            event (FileSystemEvent): The event object representing the creation of a file.

        Returns:
            None
        """
        if event.is_directory:
            return

        # Check if the created file is a CSV file
        if event.src_path.endswith(".csv"):
            # Process the newly created CSV file
            feed_csv(event.src_path, columns=self.column_names)


if __name__ == "__main__":
    feed_csv(FILE_NAME, COL_NAMES)
    # Initialize the watchdog observer
    observer = Observer()
    event_handler = CSVHandler(COL_NAMES)

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
