from collections import OrderedDict

import unicodecsv
from sqlalchemy import Integer, ForeignKey, Float, String, Table, Column

from settings import DEFAULT_DB_TABLE_NAME, DEFAULT_ID, DEFAULT_FK


class ColumnFactory(object):

    def __init__(self, descriptor, *column_args, **kwargs):
        self.descriptor = descriptor
        self.column_args = column_args
        self.kwargs = kwargs

    def question(self, key):
        return '{key}. {question}? (Select {key})'.format(key=key, question=self.descriptor)

    def as_column(self, name):
        return Column(*self.column_args, name=name, **self.kwargs)


class TableManager(object):
    INTEGER = '1'
    FLOAT = '2'
    STRING = '3'
    INTEGER_WITH_INDEX = '4'
    FLOAT_WITH_INDEX = '5'
    STRING_WITH_INDEX = '6'

    COLUMN_MAPPINGS = OrderedDict([
        (INTEGER, ColumnFactory('Integer', Integer)),
        (FLOAT, ColumnFactory('Float', Float)),
        (STRING, ColumnFactory('String', String(256))),
        (INTEGER_WITH_INDEX, ColumnFactory('Integer with index', Integer, index=True)),
        (FLOAT_WITH_INDEX, ColumnFactory('Float with index', Float, index=True)),
        (STRING_WITH_INDEX, ColumnFactory('String with index', String(256), index=True)),
    ])

    def __init__(self, file, table_name, metadata):
        self.table_name = table_name
        self.file = file
        self.metadata = metadata
        self.table = None

    def populate_table(self):
        if self.table is None:
            self.create_table()
        data = self.get_data()
        self.table.insert().values(data).execute()

    def create_table(self):
        self.table = self.get_table()
        self.table.create()

    def get_table(self):
        return Table(
            self.table_name,
            self.metadata,
            *self.get_columns()
        )

    def get_data(self):
        data = []
        with open(self.file) as csvfile:
            file = unicodecsv.DictReader(csvfile)
            for row in file:
                self.clean_row(row)
                data.append(row)
        return data

    def get_columns(self):
        columns = []
        with open(self.file) as csvfile:
            file = unicodecsv.reader(csvfile)
            headers = next(file)
            for header in headers:
                if header:
                    columns.append(self.get_column(header))
        return columns

    def get_column(self, header):
        column_type = self.get_column_type_from_user_input(header)
        return column_type.as_column(self.format_column_name(header))

    def get_column_type_from_user_input(self, header):
        print 'What kind of column is {}?'.format(header)
        choice = raw_input(self.choices())
        if choice not in self.COLUMN_MAPPINGS.keys():
            return self.get_column_type_from_user_input(header)
        return self.COLUMN_MAPPINGS[choice]

    def choices(self):
        choices = ''
        for key, value in self.COLUMN_MAPPINGS.iteritems():
            choices += '{}\n'.format(value.question(key))
        return choices

    def format_column_name(self, name):
        return name[:64].replace(' ', '_').replace('(', '').replace(')', '')

    def primary_key(self, id_name='id'):
        return Column(id_name, Integer, primary_key=True)

    def clean_row(self, row):
        if row.get(u'') is not None:
            del row[u'']
        for key in row.keys():
            if row[key] == u'':
                row[key] = None
            row[self.format_column_name(key)] = row.pop(key)


class PrimaryTableManager(TableManager):

    def __init__(self, file, metadata):
        super(PrimaryTableManager, self).__init__(file, DEFAULT_DB_TABLE_NAME, metadata)

    def get_column(self, header):
        if header == DEFAULT_ID:
            return self.primary_key(DEFAULT_ID)
        return super(PrimaryTableManager, self).get_column(header)


class SecondaryTableManager(TableManager):

    def get_columns(self):
        columns = super(SecondaryTableManager, self).get_columns()
        columns.insert(0, self.primary_key())
        return columns

    def get_column(self, header):
        if header == DEFAULT_ID:
            return self.foreign_key(header)
        return super(SecondaryTableManager, self).get_column(header)

    def foreign_key(self, name):
        return Column(Integer, ForeignKey(DEFAULT_FK), name=self.format_column_name(name))
