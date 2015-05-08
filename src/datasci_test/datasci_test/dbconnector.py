
def get_configuration(conf_file='db.conf'):
    import ConfigParser
    new_config = ConfigParser.SafeConfigParser()
    try:
        with open(conf_file, 'r') as dbconf_file:
            new_config.readfp(dbconf_file)
    except ConfigParser.ParsingError, parse_err:
        print parse_err

    return new_config

def make_connection(cf):
    import MySQLdb as mdb
    
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
    import pandas as pd
    df = pd.read_sql_query(query,connection)

    return df