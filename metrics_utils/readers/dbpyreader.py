from db import DB


class DBPYReader(object):
    '''Uses db.py as a generic interface to a database'''


    required_config = ['DB_USER', 'DB_PWD', 'DB_HOST', 'DB_NAME', 'DB_PORT',]

    def __init__(self, config, module, custom_settings=None):

        for var in self.required_config:
            if var not in config:
                raise ValueError("missing config var: %s" % var)

        self.config = config
        reader_settings = {'username':self.config.get('DB_USER'),
                           'password':self.config.get('DB_PWD'),
                           'hostname':self.config.get('DB_HOST'),
                           'dbname':self.config.get('DB_NAME'),
                           'dbtype':'redshift',
                           'schemas':['']
                               }
        if custom_settings:
            reader_settings.update(custom_settings)

        self.module = module
        print('INIT DB.PY READER FOR MODULE {}'.format(module))
        self.db =  DB(**reader_settings)

    def read(self, query):
        df = self.db.query(query)
        return df
