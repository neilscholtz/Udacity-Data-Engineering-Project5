from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    sql_query = """
        INSERT INTO {}
        {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table='',
                 write_option='delete_load',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.write_option=write_option

    def execute(self, context):
        '''
        - Creates Redshift (Postgres) Hook
        - Select data from staging table
        - Copies into dimension table
        '''
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.table == 'users':
            if self.write_option == 'delete_load':
                self.log.info(f'Deleting data from "{self.table}" table')
                redshift.run(f'DELETE FROM {self.table}')
                sql_query = SqlQueries().user_table_insert
            elif self.write_option == 'append':
                self.log.info(f'Appending data to "{self.table}" table')
                sql_query = SqlQueries().user_table_append.format(self.table, self.table)
        if self.table == 'songs':
            if self.write_option == 'delete_load':
                self.log.info(f'Deleting data from "{self.table}" table')
                redshift.run(f'DELETE FROM {self.table}')
                sql_query = SqlQueries().song_table_insert
            elif self.write_option == 'append':
                self.log.info(f'Appending data to "{self.table}" table')
                sql_query = SqlQueries().song_table_append.format(self.table, self.table)
        if self.table == 'artists':
            if self.write_option == 'delete_load':
                self.log.info(f'Deleting data from "{self.table}" table')
                redshift.run(f'DELETE FROM {self.table}')
                sql_query = SqlQueries().artist_table_insert
            elif self.write_option == 'append':
                self.log.info(f'Appending data to "{self.table}" table')
                sql_query = SqlQueries().artist_table_append.format(self.table, self.table)
        if self.table == 'time':
            if self.write_option == 'delete_load':
                self.log.info(f'Deleting data from "{self.table}" table')
                redshift.run(f'DELETE FROM {self.table}')
                sql_query = SqlQueries().time_table_insert
            elif self.write_option == 'append':
                self.log.info(f'Appending data to "{self.table}" table')
                sql_query = SqlQueries().time_table_append.format(self.table, self.table)
                
        formatted_sql = LoadDimensionOperator.sql_query.format(
            self.table,
            sql_query
        )
        self.log.info(f'SQL initiated: {formatted_sql}')
        redshift.run(formatted_sql)
