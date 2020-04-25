from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    sql = """
        INSERT INTO songplays
        ({})
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 write_option="delete_load",
                 table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.write_option=write_option
        self.table=table

    def execute(self, context):
        '''
        - Creates Redshift (Postgres) Hook
        - Select data from staging table
        - Copies into fact table
        '''
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.write_option == 'delete_load':
            self.log.info(f'Deleting data from "{self.table}" table')
            redshift.run(f'DELETE FROM {self.table}')
            insert_sql = SqlQueries().songplay_table_insert
        elif self.write_option == 'append':
            insert_sql = SqlQueries().songplay_table_append.format(self.table, self.table)
        
        formatted_sql = LoadFactOperator.sql.format(insert_sql)
        self.log.info('Inserting data into "songplays" table')
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)