import os
import sys

from sqlalchemy import create_engine


class PostgresWriter(object):
    '''Write to a postgres database'''

    required_config = ['DB_USER', 'DB_PWD', 'DB_HOST', 'DB_NAME', 'DB_PORT',]

    def __init__(self, config, module):

        for var in self.required_config:
            if var not in config:
                raise ValueError("missing config var: %s" % var)

        self.config = config

        self.module = module
        pg_conn_str = "postgresql://{}:{}@{}:{}/{}".format(
                self.config.get('DB_USER'),
                self.config.get('DB_PWD'),
                self.config.get('DB_HOST'),
                self.config.get('DB_PORT'),
                self.config.get('DB_NAME'),
                )
        sys.stdout.write('INIT POSTGRES WRITER FOR MODULE {}:\nDATABASE: {} HOST: {}'.format(module,
            self.config.get('DB_NAME'), self.config.get('DB_HOST')))
        self.engine = create_engine(pg_conn_str)

    def write(self, df, table, if_exists='append'):
        sys.stdout.write('{}:WRITE CALL.Table:,\
                    {} Rows: {} Columns: {}'.format(
            __name__, table, df.shape[0], df.shape[1]))

        if os.environ.get('DEBUG_MODE') == 'true':
            sys.stdout.write('DEBUG MODE, NOT WRITING')
            return None
        else:
            sys.stdout.write('WRITING')
            df.to_sql(table, self.engine,if_exists=if_exists, index=False)
