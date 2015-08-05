import os


class CsvWriter(object):
    '''Write to a csv file'''
    def __init__(self, module):
        self.module = module
        if not os.path.exists('csv'):
            os.makedirs('csv')

    def write(self, df, table, **kw):
        print('{}:WRITE CALL.Table:,\
                    {} Rows: {} Columns: {}'.format(
            __name__, table, df.shape[0], df.shape[1]))

        if os.environ.get('DEBUG_MODE') == 'true':
            print('DEBUG MODE, NON WRITING')
            return None
        else:
            print('WRITING')
            print(os.getcwd())
            df.to_csv('./csv/' + table + '.csv', index=False, encoding='utf-8')
