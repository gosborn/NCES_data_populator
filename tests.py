from unittest import TestCase

from mock import patch
from sqlalchemy import create_engine
from sqlalchemy import select, func, Integer, Table, Column, MetaData

from database_populator import DatabasePopulator, DatabasePopulatorException
from settings import DEFAULT_ID, DEFAULT_DB_TABLE_NAME
from table_manager import ColumnFactory, TableManager, PrimaryTableManager, SecondaryTableManager


class ColumnFactoryTestCase(TestCase):

    def test_question_includes_descriptor(self):
        descriptor = 'descriptor'
        column_factory = ColumnFactory(descriptor)
        question = column_factory.question('3')
        self.assertTrue('{}?'.format(descriptor) in question)

    def test_as_column_returns_column_of_appropriate_type(self):
        column_factory = ColumnFactory('_', Integer, index=True)
        name = 'test_column'
        column = column_factory.as_column(name)
        self.assertTrue(type(column) is Column)
        self.assertEqual(column.name, name)
        self.assertEqual(column.index, True)
        self.assertTrue(type(column.type) is Integer)


class TableManagerBaseTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('sqlite:///:memory:')
        cls.metadata = MetaData(bind=cls.engine)
        cls.metadata.reflect(cls.engine)


class TableManagerTestCase(TableManagerBaseTestCase):

    @patch('table_manager.TableManager.get_column_type_from_user_input')
    def test_get_column_returns_a_column(self, mock_column_type):
        mock_column_type.return_value = TableManager.COLUMN_MAPPINGS.get('3')
        table_manager = TableManager('./test_fixture.csv', 'test_table', self.metadata)
        column = table_manager.get_column('test_column')
        self.assertTrue(type(column) is Column)
        self.assertEqual(column.name, 'test_column')

    @patch('table_manager.TableManager.get_column_type_from_user_input')
    def test_get_columns_returns_columns_equal_to_columns_in_csv(self, mock_column_type):
        mock_column_type.return_value = TableManager.COLUMN_MAPPINGS.get('3')
        table_manager = TableManager('./test_fixture.csv', 'test_table', self.metadata)
        columns = table_manager.get_columns()
        self.assertEqual(len(columns), 4)

    @patch('table_manager.TableManager.get_column_type_from_user_input')
    def test_get_table_returns_table_with_four_columns(self, mock_column_type):
        mock_column_type.return_value = TableManager.COLUMN_MAPPINGS.get('3')
        table_manager = TableManager('./test_fixture.csv', 'test_table', self.metadata)
        table = table_manager.get_table()
        self.assertTrue(type(table) is Table)
        self.assertEqual(table.name, 'test_table')
        self.assertEqual(len(table.columns), 4)

    def test_format_column_name_removes_white_space_and_parentheses(self):
        column_name = 'this is an entirely too long column name ' \
                      '(mySQL only supports up to 64 characters (but aliases can be longer))'
        table_manager = TableManager('./test_fixture.csv', 'test_table', self.metadata)
        new_name = table_manager.format_column_name(column_name)
        self.assertTrue(len(new_name) <= 64)
        self.assertTrue(' ' not in new_name)
        self.assertTrue(')' not in new_name)
        self.assertTrue('(' not in new_name)

    def test_primary_key_returns_a_primary_key_column(self):
        table_manager = TableManager('./test_fixture.csv', 'test_table', self.metadata)
        pk_column = table_manager.primary_key()
        self.assertTrue(type(pk_column) is Column)
        self.assertEqual(pk_column.name, 'id')
        self.assertTrue(type(pk_column.type) is Integer)
        self.assertEqual(pk_column.primary_key, True)

    def test_clean_row_strips_any_empty_keys_and_empty_values(self):
        table_manager = TableManager('./test_fixture.csv', 'test_table', self.metadata)
        uncleaned_row = {
            u'': 'data',
            u'column': u''
        }
        table_manager.clean_row(uncleaned_row)
        self.assertEqual(uncleaned_row, {u'column': None})

    def test_get_data_should_return_a_list_of_dicts(self):
        table_manager = TableManager('./test_fixture.csv', 'test_table', self.metadata)
        data = table_manager.get_data()
        self.assertEqual(data, [
            {
                u'Total_price_for_in-state_students_living_on_campus_2016-17_DRVI': u'100000',
                u'Institution_Name': u'A University',
                u'Total_price_for_in-district_students_living_on_campus__2016-17_': u'100000', u'UnitID': u'1'
            },
            {
                u'Total_price_for_in-state_students_living_on_campus_2016-17_DRVI': u'3000',
                u'Institution_Name': u'B University',
                u'Total_price_for_in-district_students_living_on_campus__2016-17_': u'3000', u'UnitID': u'2'
            },
            {
                u'Total_price_for_in-state_students_living_on_campus_2016-17_DRVI': u'30',
                u'Institution_Name': u'C University',
                u'Total_price_for_in-district_students_living_on_campus__2016-17_': u'30', u'UnitID': u'3'
            }
        ])


class PrimaryTableManagerTestCase(TableManagerBaseTestCase):

    def test_if_column_name_is_default_return_primary_key(self):
        table_manager = PrimaryTableManager('./test_fixture.csv', self.metadata)
        column = table_manager.get_column(DEFAULT_ID)
        self.assertTrue(type(column) is Column)
        self.assertEqual(column.name, DEFAULT_ID)
        self.assertTrue(type(column.type) is Integer)
        self.assertEqual(column.primary_key, True)


class SecondaryTableManagerTestCase(TableManagerBaseTestCase):

    @patch('table_manager.TableManager.get_column_type_from_user_input')
    def test_get_columns_includes_a_primary_key(self, mock_column_type):
        mock_column_type.return_value = TableManager.COLUMN_MAPPINGS.get('3')
        table_manager = SecondaryTableManager('./test_fixture.csv', 'test_table', self.metadata)
        columns = table_manager.get_columns()
        pk_column = columns[0]
        self.assertTrue(type(pk_column) is Column)
        self.assertEqual(pk_column.name, 'id')
        self.assertTrue(type(pk_column.type) is Integer)
        self.assertEqual(pk_column.primary_key, True)

    def test_if_column_name_is_default_return_foreign_key(self):
        table_manager = SecondaryTableManager('./test_fixture.csv', 'test_table', self.metadata)
        column = table_manager.get_column(DEFAULT_ID)
        self.assertTrue(type(column) is Column)
        self.assertEqual(column.name, DEFAULT_ID)
        self.assertTrue(type(column.type) is Integer)
        self.assertEqual(len(column.foreign_keys), 1)


class DatabasePopulatorTestCase(TestCase):

    @patch('database_populator.DatabasePopulator.ask_for_csv', return_value='./test_fixture.csv')
    @patch('table_manager.TableManager.get_column_type_from_user_input')
    def test_create_and_populate_primary_table_calls_populate_table(self, mock_column_type, _):
        mock_column_type.return_value = TableManager.COLUMN_MAPPINGS.get('3')
        db_populator = DatabasePopulator('sqlite:///:memory:')
        db_populator.create_and_populate_primary_table()
        table = db_populator.metadata.tables.get(DEFAULT_DB_TABLE_NAME)
        self.assertTrue(table is not None)
        count_query = db_populator.engine.execute(select([func.count()]).select_from(table))
        self.assertEqual(count_query.fetchall()[0][0], 3)

    @patch('database_populator.DatabasePopulator.ask_for_table_name', return_value='test_secondary')
    @patch('database_populator.DatabasePopulator.ask_for_csv', return_value='./test_fixture.csv')
    @patch('table_manager.TableManager.get_column_type_from_user_input')
    def test_add_non_primary_dataset_raise_exc_if_no_primary_table(self, mock_column_type, _, __):
        mock_column_type.return_value = TableManager.COLUMN_MAPPINGS.get('3')
        db_populator = DatabasePopulator('sqlite:///:memory:')
        self.assertRaises(DatabasePopulatorException, db_populator.add_non_primary_dataset)

    @patch('database_populator.DatabasePopulator.ask_for_table_name', return_value='test_secondary')
    @patch('database_populator.DatabasePopulator.ask_for_csv', return_value='./test_fixture.csv')
    @patch('table_manager.TableManager.get_column_type_from_user_input')
    def test_add_non_primary_dataset_raise_exc_if_no_primary_table(self, mock_column_type, _, __):
        mock_column_type.return_value = TableManager.COLUMN_MAPPINGS.get('3')
        db_populator = DatabasePopulator('sqlite:///:memory:')
        db_populator.create_and_populate_primary_table()
        db_populator.add_non_primary_dataset()
        table = db_populator.metadata.tables.get('test_secondary')
        self.assertTrue(table is not None)
        count_query = db_populator.engine.execute(select([func.count()]).select_from(table))
        self.assertEqual(count_query.fetchall()[0][0], 3)
