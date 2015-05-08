import ConfigParser
import sys
import MySQLdb as mdb
import pandas as pd
import numpy as np

def get_configuration(conf_file='db.conf'):
    new_config = ConfigParser.SafeConfigParser()
    try:
        with open(conf_file, 'r') as dbconf_file:
            new_config.readfp(dbconf_file)
    except ConfigParser.ParsingError, parse_err:
        print parse_err

    return new_config

def make_connection(cf):
    
    dialect = cf.get('Database', 'type')
    user = cf.get('Database', 'db_user')
    pw = cf.get('Database', 'password')
    host = cf.get('Database', 'host')
    db_name = cf.get('Database', 'dbname')
    port = cf.get('Database', 'port')
    driver = cf.get('Database', 'driver')
    
    con = mdb.connect(host,user,pw,db_name)
    return con

def query_db(connection,query):
    df = pd.read_sql_query(query,connection)

    return df



if __name__ == '__main__':
    
    try:
        dbconfig = get_configuration()
        connection = make_connection(dbconfig)
        
        segment_2 = "SELECT distinct t1.*, t2.item_name, t3.item_price, t3.dealer, t3.action_type, t3.time_stamp FROM datasci.devices as t1 Inner Join products_lookup as t2 on t1.item_id = t2.item_id Inner join actions as t3 on t1.user_id = t3.user_id;"

        segment2 = query_db(connection,segment_2)
        print len(segment2)
        print 'hello'

    except Exception, e:
        print 'Error:'
        print e.message









#def new_engine(cf):
#    """
#    Creates an engine from the config cf

#    :param cf: data from ConfigParser
#    """
#    try:
#        dialect = cf.get('Database', 'type')
#        user = cf.get('Database', 'db_user')
#        pw = cf.get('Database', 'password')
#        host = cf.get('Database', 'host')
#        db_name = cf.get('Database', 'dbname')
#        port = cf.get('Database', 'port')
#        driver = cf.get('Database', 'driver')
#    except ConfigParser.NoOptionError, importantInfoMissing:
#        try: #every necessary information present?
#            assert(dialect)
#            assert(user)
#            assert(pw)
#            assert(host)
#            assert(db_name)
#            try:
#                assert(port)
#            except NameError, portNotFound:
#                print('Missing port configuration. Trying to connect with default values.')
#                port = ''
#            try:
#                assert(driver)
#            except NameError, driverNotFound:
#                print 'Missing driver configuration. Trying to connect with default values.'
#                driver = ''

#        except AssertionError:
#            print('Database connection information incomplete!')
#            print(importantInfoMissing)
#            sys.exit(DBCONNECT)

#    if driver:
#        driver = '+' + driver
#    if port:
#        port = ':' + port
#    url = dialect + driver + '://' + user + ':' + pw + '@' + host + port + '/' + db_name
#    try:
#        engine = create_engine(str(url + '?charset=utf8'), encoding=str('utf-8'))
#    except ImportError, i:
#        print('Could not find database driver. Make sure it is installed or specify the driver you want to use in the config file!')
#        print(i)
#    return engine