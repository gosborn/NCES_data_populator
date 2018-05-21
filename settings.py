DEFAULT_DB_TABLE_NAME = 'school'
DEFAULT_ID = 'UnitID'
DEFAULT_FK = '{}.{}'.format(DEFAULT_DB_TABLE_NAME, DEFAULT_ID)

DATABASE_NAME = 'school_data'
DATABASE = None
DATABASE_USER = None
DATABASE_PW = None

DEFAULT_DB = 'mysql+pymysql://{user}:{pw}@{database}/{database_name}?charset=utf8'.format(
    user=DATABASE_USER, pw=DATABASE_PW, database=DATABASE, database_name=DATABASE_NAME
)
