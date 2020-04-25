from create_tables_sql_queries import drop_tables_list, create_tables_list
import psycopg2
import configparser

def drop_tables(cur, conn):
    '''
    Drops all tables from Redshift
    '''
    print ('Attempting dropping of tables')
    for query in drop_tables_list:    
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print (e)
            print (f'Error while trying to drop table: \n{query}')
            
    print ('Tables dropped successfully')
    
def create_tables(cur, conn):
    '''
    Creates all staging, fact and dimension tables in Redshift
    '''
    print ('Attempting creating of tables')
    for query in create_tables_list:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print (e)
            print (f'Error while trying to create table: \n{query}')
    
    print ('Tables successfully created')
                   
def main():
    """
    Drops all tables and creates the necessary staging, fact & dimension tables
    in Redshift
    """
    config = configparser.ConfigParser()
    config.read('/home/workspace/setup.cfg')
    
    
    host = config['DB']['HOST']
    dbname = config['DB']['DBNAME']
    user = config['DB']['USER']
    password = config['DB']['PASSWORD']
    port = config['DB']['PORT']
    print (f'Connecting to {dbname} on {host}')
    conn = psycopg2.connect(host=host, 
                           dbname=dbname, 
                           user=user, 
                           password=password, 
                           port=port
                          )
    print (f'Connected to {dbname} on {host}')
    cur = conn.cursor()
                   
    drop_tables(cur, conn)
    create_tables(cur, conn)
                   
    conn.close()
                   
if __name__ == '__main__':
    main()