# util/db_connection.py
from sqlalchemy import create_engine
from util.db_config import DatabaseConfig

class Db_Connection:

    def __init__(self, db_type):
        config = DatabaseConfig.DATABASES.get(db_type)

        if config is None:
            raise Exception(f"Invalid database type: {db_type}")

        self.connection = None
        self.type = config['type']
        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']

    def start(self):
        try:
            if self.type == 'mysql':
                db_connection_str = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
                self.connection = create_engine(db_connection_str)
                return self.connection
            else:
                raise Exception(f"Unsupported database type: {self.type}")

        except Exception as e:
            print('Error in connection\n' + str(e))
            return None

    def stop(self):
        self.connection.dispose()
