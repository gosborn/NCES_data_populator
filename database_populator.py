import unicodecsv
from collections import OrderedDict
from sqlalchemy import Integer, ForeignKey, Float, String, Table, Column, MetaData, create_engine
from sqlalchemy_utils import database_exists, create_database
from table_creator import TableCreator, PrimaryTableCreator
from os import path, listdir


database_name = 'test_db'

database= '{}'

engine = create_engine('mysql+pymysql://{}/{}?charset=utf8'.format(database, database_name), echo=True)
metadata = MetaData(bind=engine)
metadata.reflect(engine)


class DatabasePopulator(object):

    OPTIONS = {
        '1': 'populate_primary_school_table',
        '2': 'add_non_primary_dataset',
        '3': 'add_datasets_from_directory',
        '4': 'exit'
    }

    def __init__(self):
        # this seems pretty fishy to me
        if not database_exists(engine.url):
            create_database(engine.url)

    # create db if needed
    # let user select datasource for primary table or skip if already done
    def choose_path(self):
        var = raw_input("""
        1. Add primary 'school' table from csv
        2. Add dataset that will populate a separate table as csv
        3. Add datasets from directory that will not populate primary 'school' table
        4. exit
        """)
        if var not in self.OPTIONS.keys():
            return self.choose_path()
        getattr(self, self.OPTIONS[var])()
        self.choose_path()

    def populate_primary_school_table(self):
        file_name = raw_input('Type the path name of the .csv file')
        if path.isfile(file_name):
            PrimaryTableCreator(file_name=file_name, metadata=metadata).populate_table()
        else:
            print('Couldn\'t find file!')

    def add_non_primary_dataset(self):
        file_name = raw_input('Type the path name of the .csv file')
        if path.isfile(file_name):
            if engine.dialect.has_table(engine, 'school'):
                table_name = raw_input('Type a table name')
                TableCreator(file_name=file_name, table_name=table_name, metadata=metadata).populate_table()

            else:
                print('Need to populate school table first')
        else:
            print('Couldn\'t find file!')

    def add_datasets_from_directory(self):
        if engine.dialect.has_table(engine, 'school'):
            tables_to_populate = []
            dir = raw_input('Type the name of the directory')
            if path.exists(dir):
                for f in listdir(dir):
                    if path.isfile(path.join(dir, f)) and path.join(dir, f).endswith('.csv'):
                        table_name = raw_input('Type a table name for file {}'.format(f))
                        creator = TableCreator(file_name=path.join(dir, f), table_name=table_name, metadata=metadata)
                        creator.create_table()
                        tables_to_populate.append(creator)
            for table in tables_to_populate:
                table.populate_table()
        else:
            print('Need to populate school table first')

    def exit(self):
        import sys
        sys.exit()
