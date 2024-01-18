# db_config.py

class DatabaseConfig:
    DATABASES = {
        'oltp': {
            'type': 'mysql',
            'host': '10.10.10.2',
            'port': '3306',
            'user': 'dwh',
            'password': 'elcaro_4U',
            'database': 'oltp'
        },
        'staging': {
            'type': 'mysql',
            'host': '10.10.10.2',
            'port': '3306',
            'user': 'dwh',
            'password': 'elcaro_4U',
            'database': 'staging'
        },
        'sor': {
            'type': 'mysql',
            'host': '10.10.10.2',
            'port': '3306',
            'user': 'dwh',
            'password': 'elcaro_4U',
            'database': 'sor'
        }
    }
