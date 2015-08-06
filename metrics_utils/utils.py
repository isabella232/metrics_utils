#from __future__ import unicode_literals, absolute_import, division, print_function
from datetime import datetime
import time
import os
import sys
import json
from dateutil import parser
import functools
from contextlib import contextmanager

import pandas as pd

def normalize_text(text):
    '''returns normalized text'''
    return ''.join([char for char in text.lower().split() if char.isalnum()])

def is_since(date, since_date):
    #returns if date is a newer date than since_date
    date_list = [date, since_date]
    for i,d in enumerate(date_list):
        if type(d) in (str,unicode,int):
            date_list[i] = datetime.fromtimestamp(d)
        elif not type(d) is datetime:
            raise TypeError("{} NOT A VALID DATE: TYPE:{}".format(d, type(d)))
    date, since_date = date_list
    return since_date < date

def get_task_specs(task_name, get_period=False):
    '''get the task arguments from the task file'''
    with open(os.path.dirname(__file__)+'/../tasks.json') as file:
        tasks_specs = json.load(file)
        specs = tasks_specs['tasks_specs']
        task_spec = specs[task_name]
        if get_period:
            return parse_period(task_spec)
        return task_spec
        raise Exception("CAN'T FIND TASK SPECS")

def timestamp_to_utc_epoch(timestamp):
    timestamp = datetime.fromtimestamp(timestamp)
    epoch_time = int((timestamp  - datetime(1970,1,1)).total_seconds())
    epoch_time = epoch_time# + (5* 3600)
    return epoch_time

def parse_period(task_spec):
    task_spec = task_spec['schedule']
    task_keys = task_spec.keys()
    cron_periods = ['week', 'day','hour', 'minute','second']
    interval_periods = ['weeks', 'days','hours', 'minutes','seconds']
    if task_spec['trigger'] == 'cron':
        for i, period in enumerate(cron_periods):
            if period in task_keys:
                return {cron_periods[i - 1] + 's' : 1}
    elif task_spec['trigger'] == 'interval':
        for period in interval_periods:
            if period in task_keys:
                return {period: eval(task_spec[period])}
    raise Exception("NO FUNCTION TYPE FOUND")

def df_exists(df):
    '''Works with None and dataframes'''
    if hasattr(df,'shape') and df.shape[0]:
        return True
    else:
        return False

def datetime_to_timestamp(date):
    try:
        return time.mktime(date.timetuple())
    except:
        return None

def convert_dates(date, date_format):
    try:
        return date.strftime(date_format)
    except:
        return None

def df_format_timestamp(df, column='time', input_format=None, unit=None):
    if input_format:
        df[column] = df[column].apply(lambda x:
                                      datetime.strptime(str(x),input_format))
    elif unit:
        df[column] = pd.to_datetime(df[column], unit=unit)
    else:
        try:
            df[column] = df[column].map(lambda x:parser.parse(str(x)))
        except AttributeError:
            try:
                df[column] = pd.to_datetime(df[column], coerce=True)
            except ValueError:
                assert False
    df[column] = df[column].apply(datetime_to_timestamp_string)
    return df

def add_timestamp(timestamp, df):
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    df['time'] = timestamp
    return df

def datetime_to_timestamp_string(date, output_format='%Y-%m-%d %H:%M:%S'):
    try:
        return date.strftime(output_format)
    except:
        return None

def df_column_count(df, column_name, normalize):
    '''return counts of a Pandas Dataframe by a column'''
    df = df[column_name].value_counts().reset_index()
    df.columns = [column_name, 'count']
    return df

def memoize(obj):
    '''memoizes a function (caches the results)'''
    cache = obj.cache = {}
    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer

class memoizeLimited(object):

    def __init__(self, max_size=999999):
        """
        If there are decorator arguments, the function
        to be decorated is not passed to the constructor!
        """
        self.max_size = max_size
        self.cache = {}
    def __call__(self, f):
        """
        If there are decorator arguments, __call__() is only called
        once, as part of the decoration process! You can only give
        it a single argument, which is the function object.
        """
        def memoizer(*args, **kwargs):
            key = str(args) + str(kwargs)
            if key not in self.cache:
                self.cache[key] = f(*args, **kwargs)
            output = self.cache[key]

            if len(self.cache) > self.max_size:
                del self.cache
                setattr(self, 'cache',{})
            return output
        return memoizer

@contextmanager
def stdout_redirector(stream_file):
    old_stdout = sys.stdout
    sys.stdout = stream_file
    try:
        yield
    finally:
        sys.stdout = old_stdout

@contextmanager
def crab_task(task_name, secrets=None):
    from crab.client import CrabClient
    import traceback
    from StringIO import StringIO

    stdout_stream = StringIO()
    crab = CrabClient(command=task_name)
    crab.start()
    task_status = 0
    task_traceback = ''
    #stdout hijacking context
    with stdout_redirector(stdout_stream):
        try:
            yield
        except:
            task_status = 1
            task_traceback=traceback.format_exc()
            if secrets:
                for secret in secrets:
                    task_traceback = task_traceback.replace(
                        str(secret), '*' * 4)
    task_stdout = stdout_stream.getvalue()
    try:
        task_stdout = task_stdout.encode('ascii', 'replace')
    except UnicodeDecodeError:
        task_stdout = task_stdout.decode('utf-8').encode('ascii','replace')
    #anonymize secrets
    if secrets:
        for secret in secrets:
            task_stdout = task_stdout.replace(str(secret), '*' * 4)

    crab.finish(status=task_status, stdoutdata=task_stdout,
                stderrdata=task_traceback)
    #we print the stdout so cron can pick it up
    print(task_stdout)
