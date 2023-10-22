from utils.config_parser import ConfigReader
from data_utils.db_utils import DbUtils

SQL_FILE = "sql_definitions/user_table.sql"


def init_db():
    config_reader = ConfigReader('config.ini')
    config_dict = config_reader.get_config_dict()['db']

    db_utils = DbUtils(config_dict)
    db_utils.create_table_if_not_exists(SQL_FILE)
