from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTableInRedshiftOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'wyatt',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'retries': 0
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_event_table = CreateTableInRedshiftOperator(
    task_id='Create_staging_event_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.staging_event_table_create
)

create_staging_song_table = CreateTableInRedshiftOperator(
    task_id='Create_staging_song_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.staging_song_table_create
)

create_song_table = CreateTableInRedshiftOperator(
    task_id='Create_song_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.song_table_create
)

create_artist_table = CreateTableInRedshiftOperator(
    task_id='Create_artist_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.artist_table_create
)

create_songplay_table = CreateTableInRedshiftOperator(
    task_id='Create_songplay_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.songplay_table_create
)

create_time_table = CreateTableInRedshiftOperator(
    task_id='Create_time_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.time_table_create
)

create_user_table = CreateTableInRedshiftOperator(
    task_id='Create_user_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql = SqlQueries.user_table_create
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events_from_s3_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path_json="s3://udacity-dend/log_data",
    redshift_target_table = "staging_events",
    s3_path_schema="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs_from_s3_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path_json="s3://udacity-dend/song_data",
    redshift_target_table = "staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [create_staging_song_table, create_staging_event_table, create_song_table, create_songplay_table, create_user_table, create_song_table, create_artist_table, create_time_table]
create_staging_song_table >> [stage_events_to_redshift, stage_songs_to_redshift]
create_staging_event_table >> [stage_events_to_redshift, stage_songs_to_redshift]
create_song_table >> [stage_events_to_redshift, stage_songs_to_redshift]
create_songplay_table >> [stage_events_to_redshift, stage_songs_to_redshift]
create_user_table >> [stage_events_to_redshift, stage_songs_to_redshift]
create_song_table >> [stage_events_to_redshift, stage_songs_to_redshift]
create_artist_table >> [stage_events_to_redshift, stage_songs_to_redshift]
create_time_table >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
