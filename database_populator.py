import sys
from os import path, listdir

from sqlalchemy import MetaData, create_engine
from sqlalchemy_utils import database_exists, create_database

from settings import DEFAULT_DB_TABLE_NAME, DEFAULT_DB
from table_manager import PrimaryTableManager, SecondaryTableManager


class DatabasePopulatorException(Exception):
    pass


class DatabasePopulator(object):

    OPTIONS = {
        '1': 'create_and_populate_primary_table',
        '2': 'add_non_primary_dataset',
        '3': 'add_datasets_from_directory',
        '4': 'exit',
    }

    def __init__(self, database=DEFAULT_DB):
        self.database = database
        self.engine = self._get_engine()
        self.metadata = self._get_metadata()

    def run(self):
        choice = self._get_choice()
        try:
            getattr(self, self.OPTIONS[choice])()
        except Exception as e:
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
        self.run()

    def _get_choice(self):
        return raw_input("""
        1. Add primary '{primary_table}' table from csv
        2. Add dataset that will populate a secondary table as csv
        3. Add datasets from directory to create secondary tables
        4. exit
        """.format(primary_table=DEFAULT_DB_TABLE_NAME))

    def _get_engine(self):
        return create_engine(self.database, echo=True)

    def _get_metadata(self):
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        metadata = MetaData(bind=self.engine)
        metadata.reflect(self.engine)
        return metadata

    def create_and_populate_primary_table(self):
        file_name = self.ask_for_csv()
        PrimaryTableManager(file=file_name, metadata=self.metadata).populate_table()

    def ask_for_csv(self):
        return raw_input('Type the path name of the .csv file: ')

    def add_non_primary_dataset(self):
        self._raise_exc_if_primary_table_not_present()
        file_name = self.ask_for_csv()
        self._name_table_and_populate(file_name)
        print('Success!')

    def add_datasets_from_directory(self):
        self._raise_exc_if_primary_table_not_present()
        dir = self.ask_for_directory()
        for f in listdir(dir):
            if path.isfile(path.join(dir, f)) and path.join(dir, f).endswith('.csv'):
                self._name_table_and_populate(path.join(dir, f))

    def ask_for_directory(self):
        return raw_input('Type the name of the directory: ')

    def _name_table_and_populate(self, file_name):
        table_name = self.ask_for_table_name(file_name)
        SecondaryTableManager(file=file_name, table_name=table_name, metadata=self.metadata).populate_table()
        print('Success!')

    def ask_for_table_name(self, file_name):
        return raw_input('Type a table name for {}: '.format(file_name))

    def _raise_exc_if_primary_table_not_present(self):
        if not self.engine.dialect.has_table(self.engine, DEFAULT_DB_TABLE_NAME):
            raise DatabasePopulatorException('Need to create and populate {} table first!'.format(DEFAULT_DB_TABLE_NAME))

    def exit(self):
        sys.exit()
