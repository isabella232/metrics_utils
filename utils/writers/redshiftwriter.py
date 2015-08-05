import os
import sys

import psycopg2
from sqlalchemy import create_engine
from db import DB

from utils.config import get_io_config

env = get_io_config('redshift')
DB_USER, DB_PWD, DB_HOST, DB_NAME, S3_BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY = env.DB_USER, env.DB_PWD, env.DB_HOST, env.DB_NAME, env.S3_BUCKET, env.AWS_ACCESS_KEY, env.AWS_SECRET_KEY

class RedshiftWriter(object):
    '''Write to a postgres database'''

    def __init__(self, module):
        self.module = module

        sys.stdout.write('{} INIT REDSHIFT WRITER FOR MODULE {}:\nDATABASE: {} HOST: {}\n'.format(__name__,
            module, DB_NAME, DB_HOST))

        rs_settings =  {
            'username':DB_USER,
            'password':DB_PWD,
            'hostname':DB_HOST,
            'dbname':DB_NAME,
                }

        rs_conn_str = " dbname='{dbname}' user='{username}' host='{hostname}' port='5439' password='{password}'".format(
                **rs_settings)

        rs_sqlalchemy_str = 'postgresql://{username}:{password}@{hostname}:5439/{dbname}'.format(
                **rs_settings)

        self.conn = psycopg2.connect(rs_conn_str)
        self.cur = self.conn.cursor()
        self.engine = create_engine(rs_sqlalchemy_str)

        sys.stdout.write('{} INIT DB.PY READER FOR MODULE {}\n'.format(__name__, module))
        rs_settings['dbtype'] = 'redshift'
        rs_settings['schemas'] = ['']
        self.db =  DB(**rs_settings)

    def _try_command(self, cmd):
        try:
            self.cur.execute(cmd)
            self.conn.commit()
        except Exception as e:
            sys.stderr.write("Error executing command:")
            sys.stderr.write("\t '{0}'".format(cmd))
            sys.stderr.write("Exception: {0}".format(e))
            self.conn.rollback()

    def clean_table(self, table):
        sys.stdout.write('{}: ANALYZE AND VACUUM TABLE {}\n\n'.format(__name__, table))
        #psycopg2 creates automatic transactions and VACUUM must be done outside, booo
        self.conn.autocommit = True
        sql = 'VACUUM {};'.format(table)
        self._try_command(sql)
        self.conn.autocommit = False
        sql = 'ANALYZE {};'.format(table)
        self._try_command(sql)

    def write(self, df, table, drop_if_exists=False, s3_copy=False, if_exists='append',**kw):
        sys.stdout.write('{}:WRITE CALL.Table:,\
                    {} Rows: {} Columns: {}\n'.format(
            __name__, table, df.shape[0], df.shape[1]))

        if os.environ.get('DEBUG_MODE') == 'true':
            sys.stdout.write('{}: DEBUG MODE, NOT WRITING\n'.format(__name__))
            return None
        if drop_if_exists:
            sql = 'BEGIN;DELETE FROM {};COMMIT;END;'.format(table)
            sys.stdout.write(sql + '\n')
            self._try_command(sql)
            if s3_copy:
                sys.stdout.write('{}: WRITING S3 COPY\n'.format(__name__))
                self._write_s3_copy(table, df, drop_if_exists, **kw)
            else:
                sys.stdout.write('{}: WRITING BATCH\n'.format(__name__))
                df.to_sql(table, self.engine,if_exists='append',index=False)
            self.clean_table(table)
        else:
            if s3_copy:
                sys.stdout.write('{}: WRITING S3 COPY\n'.format(__name__))
                self._write_s3_copy(table, df, drop_if_exists, **kw)
            else:
                sys.stdout.write('{}: WRITING BATCH\n'.format(__name__))
                df.to_sql(table, self.engine,if_exists=if_exists, index=False)

    def _write_s3_copy(self, name, df, drop_if_exists=False, chunk_size=10000,
                    s3=None, print_sql=True, bucket_location=None):
        try:
            from boto.s3.connection import S3Connection
            from boto.s3.key import Key
            from boto.s3.connection import Location
            import threading
            import gzip
            from StringIO import StringIO
            if bucket_location is None:
                bucket_location = Location.DEFAULT

        except ImportError:
            raise Exception("Couldn't find boto library. Please ensure it is installed")
        s3_bucket = S3_BUCKET
        conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = conn.get_bucket(s3_bucket)
        bucket_name = s3_bucket

        # we're going to chunk the file into pieces. according to amazon, this is
        # much faster when it comes time to run the \COPY statment.
        #
        # see http://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html
        sys.stdout.write("Transfering {0} to s3 in chunks\n".format(name))
        len_df = len(df)
        chunks = range(0, len_df, chunk_size)
        def upload_chunk(i):
            chunk = df[i:(i+chunk_size)]
            k = Key(bucket)
            k.key = 'data-{}-{}-{}.csv.gz'.format(name, i, i + chunk_size)
            k.set_metadata('parent', 'db.py')
            out = StringIO()
            with gzip.GzipFile(fileobj=out, mode="w") as f:
                  f.write(chunk.to_csv(index=False, encoding='utf-8'))
            k.set_contents_from_string(out.getvalue())
            sys.stdout.write(".")
            return i

        threads = []
        for i in chunks:
            t = threading.Thread(target=upload_chunk, args=(i, ))
            t.start()
            threads.append(t)

        # join all threads
        for t in threads:
            t.join()
        sys.stdout.write("done\n")

        # perform the \COPY here. the s3 argument is a prefix, so it'll pick up
        # all of the data-{tablename}*.gz files we've created
        sys.stdout.write("Copying data from s3 to redshfit...\n")
        columns = ','.join(df.columns)
        columns = ', '.join([ '"{}"'.format(col) for col in df.columns])
        sql = '''
        COPY {name}({columns})
        FROM 's3://{bucket_name}/data-{name}'
        CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY};aws_secret_access_key={AWS_SECRET_KEY}'
        CSV IGNOREHEADER 1 delimiter ',' gzip  TRUNCATECOLUMNS;
        '''.format(name=name, columns=columns, bucket_name=bucket_name,
                   AWS_ACCESS_KEY=AWS_ACCESS_KEY, AWS_SECRET_KEY=AWS_SECRET_KEY)

        if print_sql:
            sys.stdout.write(sql + "\n")
        self._try_command(sql)
        self.conn.commit()
        sys.stdout.write("done!\n")
        # tear down the bucket's data
        sys.stdout.write("Tearing down bucket...\n")
        for key in bucket.list():
            if key.key.startswith('data-{}'.format(name)):
                sys.stdout.write('Deleting key {}\n'.format(key.key))
                key.delete()
        sys.stdout.write("done!\n")
