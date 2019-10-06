from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTableInRedshiftOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'wyatt',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'retries': 0,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

create_staging_event_table = CreateTableInRedshiftOperator(
    task_id='Create_staging_event_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    sql_drop = SqlQueries.staging_event_table_drop,
    sql_create = SqlQueries.staging_event_table_create
)

create_staging_song_table = CreateTableInRedshiftOperator(
    task_id='Create_staging_song_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    sql_drop = SqlQueries.staging_song_table_drop,
    sql_create = SqlQueries.staging_song_table_create
)

create_song_table = CreateTableInRedshiftOperator(
    task_id='Create_song_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    sql_drop = SqlQueries.song_table_drop,
    sql_create = SqlQueries.song_table_create
)

create_artist_table = CreateTableInRedshiftOperator(
    task_id='Create_artist_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    sql_drop = SqlQueries.artist_table_drop,
    sql_create = SqlQueries.artist_table_create
)

create_songplay_table = CreateTableInRedshiftOperator(
    task_id='Create_songplay_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    sql_drop = SqlQueries.songplay_table_drop,
    sql_create = SqlQueries.songplay_table_create
)

create_time_table = CreateTableInRedshiftOperator(
    task_id='Create_time_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    sql_drop = SqlQueries.time_table_drop,
    sql_create = SqlQueries.time_table_create
)

create_user_table = CreateTableInRedshiftOperator(
    task_id='Create_user_table_in_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    sql_drop = SqlQueries.user_table_drop,
    sql_create = SqlQueries.user_table_create
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
    dag=dag,
    redshift_conn_id="redshift",
    sql_select=SqlQueries.songplay_table_insert,
    fact_table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_select=SqlQueries.user_table_insert,
    dimension_table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_select=SqlQueries.song_table_insert,
    dimension_table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_select=SqlQueries.artist_table_insert,
    dimension_table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_select=SqlQueries.time_table_insert,
    dimension_table="time"
)

check_songplay_table = DataQualityOperator(
    task_id='Check_songplay_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays"
)

check_user_table = DataQualityOperator(
    task_id='Check_user_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users"
)

check_song_table = DataQualityOperator(
    task_id='Check_song_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs"
)

check_artist_table = DataQualityOperator(
    task_id='Check_artist_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists"
)

check_time_table = DataQualityOperator(
    task_id='Check_time_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time"
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

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
load_songplays_table >> check_songplay_table
load_user_dimension_table >> check_user_table
load_song_dimension_table >> check_song_table
load_artist_dimension_table >> check_artist_table
load_time_dimension_table >> check_time_table
[check_songplay_table, check_user_table, check_song_table, check_artist_table, check_time_table] >> end_operator
