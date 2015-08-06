import psycopg2
import pandas as pd

class RedshiftReader(object):
    '''Write to a postgres database'''

    required_config = ['DB_USER', 'DB_PWD', 'DB_HOST', 'DB_PORT', 'DB_NAME']

    def __init__(self, config, module, custom_settings=None):
        for var in self.required_config:
            if var not in config:
                raise ValueError("missing config var: %s" % var)

        self.config = config

        rs_conn_str = " dbname='{}' user='{}' host='{}' port='{}' password='{}'".format(
                self.config.get('DB_NAME'), self.config.get('DB_USER'),
                self.config.get('DB_HOST'), self.config.get('DB_PORT')
                self.config.get('DB_PWD'))

        self.module = module

        print('INIT POSTGRES READER FOR MODULE {}'.format(module))
        self.conn = psycopg2.connect(rs_conn_str)

    def read(self, query):
        df =  pd.io.sql.read_sql(query, self.conn)
        return df

    def run(self, query):
        print('REDSHIFT READER RUN QUERY:{}'.format(query))
        cur = self.conn.cursor()
        cur.execute(query)
