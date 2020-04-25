from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
import configparser

config = configparser.ConfigParser()
config.read('/home/workspace/setup.cfg')

default_args = {
    'owner': 'Sparkify',
    'depends_on_past':False,
    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag, 
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket=config['S3']['BUCKET_NAME'],
    s3_key=config['S3']['LOG_DATA'],
    json_format=config['S3']['LOG_PATH'],
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket=config['S3']['BUCKET_NAME'],
    s3_key=config['S3']['SONG_DATA'],
    json_format="'auto'",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    write_option='delete_load', 
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    write_option='append'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    write_option='append'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    write_option='append'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    write_option='append'
)

run_song_quality_check = DataQualityOperator(
    task_id='Run_song_quality_check',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
)

run_artist_quality_check = DataQualityOperator(
    task_id='Run_artist_quality_check',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
)

run_user_quality_check = DataQualityOperator(
    task_id='Run_user_quality_check',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
)

run_time_quality_check = DataQualityOperator(
    task_id='Run_time_quality_check',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >>[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
load_user_dimension_table >> run_user_quality_check
load_song_dimension_table >> run_song_quality_check
load_artist_dimension_table >> run_artist_quality_check
load_time_dimension_table >> run_time_quality_check
[run_user_quality_check, run_song_quality_check, run_artist_quality_check, run_time_quality_check] >> end_operator