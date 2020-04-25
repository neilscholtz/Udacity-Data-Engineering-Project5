from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):
    select_sql='''
        SELECT COUNT(*) 
        FROM {}'''
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table

    def execute(self, context):
        '''
        - Creat Redshift (Postgres) Hook
        - Checks number of rows in table
        - Checks number of null values in table
        - Returns erros if 0 rows or more than 0 null values
        '''
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Check number of rows
        self.log.info(f'Checking "{self.table}" table for number of rows')
        row_count_sql_query = DataQualityOperator.select_sql.format(self.table)
        self.log.info(f'SQL row count query: {row_count_sql_query}')
        row_count = redshift.get_records(row_count_sql_query)
        if row_count[0][0] < 1:
            return ValueError(f'Data validation failed on table "{self.table}" with 0 rows returned')
            self.log.info(f'Data validation failed on table "{self.table}" with 0 rows returned')
        else:
            self.log.info(f'Data validation passed on table "{self.table}" with {row_count[0][0]} rows returned')
    
        # Check null values
        ## Assign null_sql_query
        self.log.info(f'Checking "{self.table}" table for null values')
        if self.table == 'songplays':
            null_sql_query = SqlQueries().songplays_table_nulls
        elif self.table == 'users':
            null_sql_query = SqlQueries().user_table_nulls
        elif self.table == 'songs':
            null_sql_query = SqlQueries().song_table_nulls
        elif self.table == 'artists':
            null_sql_query = SqlQueries().artist_table_nulls
        elif self.table == 'time':
            null_sql_query = SqlQueries().time_table_nulls
        
        ## Run null check
        self.log.info(f'SQL null query: {null_sql_query}')
        null_result = redshift.run(null_sql_query)
        if null_result == None or null_result[0][0] < 0:
            self.log.info(f'Data validation passed on table {self.table} with 0 nulls found')
        else:
            return ValueError(f'Data validation failed on table {self.table} with {null_result[0][0]} nulls found')
            self.log.info(f'Data validation failed on table {self.table} with {null_result[0][0]} nulls found')