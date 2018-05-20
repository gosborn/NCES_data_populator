import unicodecsv
from collections import OrderedDict
from sqlalchemy import Integer, ForeignKey, Float, String, Table, Column, MetaData, create_engine
from settings import DEFAULT_DB_TABLE_NAME, DEFAULT_ID, DEFAULT_FK

# TODO, some warning about assuming that first table is going to have primary key of UnitID and this will be referenced by other tables

DEFAULT_FOREIGN_KEY = 'school.UnitID'

class ColumnData(object):

    def __init__(self, descriptor, *column_args, **kwargs):
        self.descriptor = descriptor
        self.column_args = column_args
        self.kwargs = kwargs

    def question(self, key):
        return '{key}. {question}? (Select {key})'.format(key=key, question=self.descriptor)

    def as_column(self, name):
        return Column(*self.column_args, name=name[:64], **self.kwargs)


class TableCreator(object):
    INTEGER = '1'
    FLOAT = '2'
    STRING = '3'
    INTEGER_WITH_INDEX = '4'
    FLOAT_WITH_INDEX = '5'
    STRING_WITH_INDEX = '6'

    COLUMN_MAPPINGS = OrderedDict([
        (INTEGER, ColumnData('Integer', Integer)),
        (FLOAT, ColumnData('Float', Float)),
        (STRING, ColumnData('String', String(256))),
        (INTEGER_WITH_INDEX, ColumnData('Integer with index', Integer, index=True)),
        (FLOAT_WITH_INDEX, ColumnData('Float with index', Float, index=True)),
        (STRING_WITH_INDEX, ColumnData('String with index', String(256), index=True)),
    ])

    FOREIGN_KEY = ColumnData('Foreign Key (Use for UnitID on tables that reference the Primary Table)',
                             Integer, ForeignKey(DEFAULT_FK), nullable=False)

    def __init__(self, file_name, table_name, metadata):
        self.table_name = table_name
        self.file = file_name
        self.metadata = metadata
        self.table = None

    def choices(self):
        choices = ''
        for key, value in self.COLUMN_MAPPINGS.iteritems():
            choices += '{}\n'.format(value.question(key))
        return choices

    def foreign_key(self, name):
        return Column(Integer, ForeignKey('school.UnitID'), name=self.format_column_name(name))

    def primary_key(self):
        return Column('id', Integer, primary_key=True)

    def get_columns(self):
        columns = []
        with open(self.file) as csvfile:
            file = unicodecsv.reader(csvfile)
            headers = next(file)
            for header in headers:
                if header:
                    if header == DEFAULT_ID:
                        columns.append(self.foreign_key(header))
                    else:
                        column_type = self.determine_column_type(header)
                        columns.append(column_type.as_column(self.format_column_name(header)))
            columns.append(self.primary_key())
        return columns

    def format_column_name(self, name):
        return name[:64].replace(' ', '_').replace('(', '').replace(')', '')

    def create_table(self):
        self.table = Table(
            self.table_name,
            self.metadata,
            *self.get_columns()
        )
        self.table.create()

    def populate_table(self):
        if self.table is None:
            self.create_table()
        with open(self.file) as csvfile:
            file = unicodecsv.DictReader(csvfile)
            for row in file:
                if row.get(u'') is not None:
                    del row[u'']
                for key in row.keys():
                    if row[key] == u'':
                        del row[key]
                for key in row.keys():
                    row[self.format_column_name(key)] = row.pop(key)
                self.table.insert().values(**row).execute()

    def determine_column_type(self, header):
        print('What kind of column is {}?').format(header)
        key = raw_input(self.choices())
        if key not in self.COLUMN_MAPPINGS.keys():
            return self.determine_column_type(header)
        return self.COLUMN_MAPPINGS[key]


class PrimaryTableCreator(TableCreator):

    PRIMARY_KEY = ColumnData('Primary Key (Use for default id on Primary Table)', Integer, primary_key=True)

    def __init__(self, file_name, metadata):
        super(PrimaryTableCreator, self).__init__(file_name, DEFAULT_DB_TABLE_NAME, metadata)

    def get_columns(self):
        columns = []
        with open(self.file) as csvfile:
            file = unicodecsv.reader(csvfile)
            headers = next(file)
            for header in headers:
                if header:
                    if header == DEFAULT_ID:
                        column_type = self.PRIMARY_KEY
                    else:
                        column_type = self.determine_column_type(header)
                    columns.append(column_type.as_column(header))
        return columns
