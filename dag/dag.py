from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from datetime import datetime, timedelta

# Bronze layer endpoints (same as your existing DAG)
BRONZE_LAYER_1_URL = "CLOUD_FUNCTION_URL_1"
BRONZE_LAYER_2_URL = "CLOUD_FUNCTION_URL_2"
SILVER_LAYER_1_URL = "CLOUD_FUNCTION_URL_3"

# Same functions as your existing DAG - no changes needed here
def invoke_bronze_layer_1():
    request = google.auth.transport.requests.Request()
    credentials = id_token_credential_utils.get_default_id_token_credentials(BRONZE_LAYER_1_URL, request=request)
    session = AuthorizedSession(credentials)
    response = session.request("POST", url=BRONZE_LAYER_1_URL)
    print(f"Bronze Layer 1 Status: {response.status_code}")
    print(f"Bronze Layer 1 Response: {response.content}")
    if response.status_code != 200:
        raise Exception(f"Bronze Layer 1 failed: {response.status_code} - {response.content}")
    return "Bronze Layer 1 completed successfully"

def invoke_bronze_layer_2():
    request = google.auth.transport.requests.Request()
    credentials = id_token_credential_utils.get_default_id_token_credentials(BRONZE_LAYER_2_URL, request=request)
    session = AuthorizedSession(credentials)
    response = session.request("POST", url=BRONZE_LAYER_2_URL)
    print(f"Bronze Layer 2 Status: {response.status_code}")
    print(f"Bronze Layer 2 Response: {response.content}")
    if response.status_code != 200:
        raise Exception(f"Bronze Layer 2 failed: {response.status_code} - {response.content}")
    return "Bronze Layer 2 completed successfully"

def invoke_silver_layer_1():
    request = google.auth.transport.requests.Request()
    credentials = id_token_credential_utils.get_default_id_token_credentials(SILVER_LAYER_1_URL, request=request)
    session = AuthorizedSession(credentials)
    response = session.request("POST", url=SILVER_LAYER_1_URL)
    print(f"Silver Layer 1 Status: {response.status_code}")
    print(f"Silver Layer 1 Response: {response.content}")
    if response.status_code != 200:
        raise Exception(f"Silver Layer 1 failed: {response.status_code} - {response.content}")
    return "Silver Layer 1 completed successfully"

# Same configuration as your existing DAG
production_args = {
    'owner': 'nba-analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(minutes=45),
}

dag = DAG(
    dag_id='nba_analytics_wap_pipeline_production',
    default_args=production_args,
    description='Production NBA Analytics Pipeline with WAP Pattern',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    catchup=False,
    tags=['nba', 'analytics', 'wap', 'production'],
    max_active_runs=1,
    max_active_tasks=10,
)

bronze_layer_1 = PythonOperator(
    task_id='bronze_layer_1_scoreboard',
    python_callable=invoke_bronze_layer_1,
    dag=dag,
    **production_args
)

bronze_layer_2 = PythonOperator(
    task_id='bronze_layer_2_boxscores',
    python_callable=invoke_bronze_layer_2,
    dag=dag,
    **production_args
)

update_nba_seasons = BigQueryInsertJobOperator(
    task_id='update_nba_seasons',
    configuration={
        'query': {
            'query': 'CALL `update_seasons`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

silver_layer_1 = PythonOperator(
    task_id='silver_layer_1_flatten',
    python_callable=invoke_silver_layer_1,
    dag=dag,
    **production_args
)

# This calls your stored procedure that takes raw data, cleans it, and puts it in staging
# It's the first step of WAP - we're "writing" clean data to a safe staging area
wap_part_1_write = BigQueryInsertJobOperator(
    task_id='wap_part_1_write_and_clean',
    configuration={
        'query': {
            'query': 'CALL `pushing_raw_data_to_staging`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)


# These 7 tasks are the "AUDIT" part - they scan the staging data for problems
# Each one checks for a different type of data quality issue
# If ANY of these fail, the pipeline stops and bad data never reaches production
# Think of these as quality control inspectors at a factory

# Audit #1: Check if players made more shots than they attempted (impossible!)
audit_shooting_logic = BigQueryCheckOperator(
    task_id='audit_shooting_logic',
    sql="""
    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM `staging_area`
    WHERE fieldGoalsMade > fieldGoalsAttempted
       OR threePointersMade > threePointersAttempted  
       OR freeThrowsMade > freeThrowsAttempted
    """,
    use_legacy_sql=False,
    dag=dag,
    **production_args
)

# Audit #2: Check if any player "played" more than 65 minutes (impossible in basketball!)
audit_minutes_range = BigQueryCheckOperator(
    task_id='audit_minutes_range',
    sql="""
    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM `staging_area`
    WHERE minutes < 0 OR minutes > 65
    """,
    use_legacy_sql=False,
    dag=dag,
    **production_args
)

# Audit #3: Check if any shooting percentages are impossible (over 100% or negative)
audit_percentages = BigQueryCheckOperator(
    task_id='audit_percentages',
    sql="""
    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM `staging_area`
    WHERE fieldGoalsPercentage < 0 OR fieldGoalsPercentage > 1
       OR threePointersPercentage < 0 OR threePointersPercentage > 1
       OR freeThrowsPercentage < 0 OR freeThrowsPercentage > 1
    """,
    use_legacy_sql=False,
    dag=dag,
    **production_args
)

# Audit #4: Check rebound math (total rebounds MUST equal offensive + defensive)
audit_rebound_logic = BigQueryCheckOperator(
    task_id='audit_rebound_logic',
    sql="""
    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM `staging_area`
    WHERE reboundsTotal != (reboundsOffensive + reboundsDefensive)
    """,
    use_legacy_sql=False,
    dag=dag,
    **production_args
)

# Audit #5: Check for negative stats (you can't have -5 assists!)
audit_negative_stats = BigQueryCheckOperator(
    task_id='audit_negative_stats',
    sql="""
    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM `staging_area`
    WHERE fieldGoalsMade < 0 OR fieldGoalsAttempted < 0
       OR threePointersMade < 0 OR threePointersAttempted < 0
       OR freeThrowsMade < 0 OR freeThrowsAttempted < 0
       OR reboundsOffensive < 0 OR reboundsDefensive < 0 OR reboundsTotal < 0
       OR assists < 0 OR steals < 0 OR blocks < 0 OR turnovers < 0
       OR foulsPersonal < 0 OR points < 0
    """,
    use_legacy_sql=False,
    dag=dag,
    **production_args
)

# Audit #6: Check for missing critical data (can't have games without player IDs!)
audit_required_fields = BigQueryCheckOperator(
    task_id='audit_required_fields',
    sql="""
    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM `staging_area`
    WHERE personId IS NULL 
       OR game_id IS NULL 
       OR game_date IS NULL
       OR team_id IS NULL
       OR season IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
    **production_args
)

# Audit #7: Check for duplicate players (same player can't appear twice in one game!)
audit_player_uniqueness = BigQueryCheckOperator(
    task_id='audit_player_uniqueness',
    sql="""
    SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM (
        SELECT personId, game_id, COUNT(*) as record_count
        FROM `staging_area`
        GROUP BY personId, game_id
        HAVING COUNT(*) > 1
    )
    """,
    use_legacy_sql=False,
    dag=dag,
    **production_args
)

# This is the "PUBLISH" part of WAP - it only runs if ALL audits passed
# It moves the validated data from staging to production where your dashboards can use it
# Think of this as the final approval step - only clean data gets through
wap_part_3_publish = BigQueryInsertJobOperator(
    task_id='wap_part_3_publish_to_production',
    configuration={
        'query': {
            'query': 'CALL `push_data_to_production`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# After production data is ready, we update the player reference table
# This creates a lookup table of all players, teams, and seasons for the Gold layer to use
update_player_reference_table = BigQueryInsertJobOperator(
    task_id='update_player_reference_table',
    configuration={
        'query': {
            'query': 'CALL `upadte_player_reference_table`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# These 6 tasks create your business-level analytics tables
# They all run in parallel since they don't depend on each other
# Each one takes the clean player data and creates specialized analytics views

# Analytics Table #1: Combined analysis with dual scatter plots (volume vs efficiency)
update_combined_player_analysis = BigQueryInsertJobOperator(
    task_id='update_combined_player_analysis',
    configuration={
        'query': {
            'query': 'CALL `update_combined_player_analysis`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# Analytics Table #2: Fantasy basketball leaders (top scorers by fantasy points)
update_fantasy_points_leaders = BigQueryInsertJobOperator(
    task_id='update_fantasy_points_leaders',
    configuration={
        'query': {
            'query': 'CALL `update_fantasy_points_leaders`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# Analytics Table #3: Player profile pages (detailed stats + game logs for each player)
update_player_profile_data = BigQueryInsertJobOperator(
    task_id='update_player_profile_data',
    configuration={
        'query': {
            'query': 'CALL `update_player_profile_data`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# Analytics Table #4: Trend analysis (last 10 games vs season average)
update_player_trend_analysis = BigQueryInsertJobOperator(
    task_id='update_player_trend_analysis',
    configuration={
        'query': {
            'query': 'CALL `update_player_trend_analysis`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# Analytics Table #5: Z-score analysis (statistical ranking of players by performance)
update_player_zscore_analysis = BigQueryInsertJobOperator(
    task_id='update_player_zscore_analysis',
    configuration={
        'query': {
            'query': 'CALL `update_player_zscore_analysis`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# Analytics Table #6: Top 15 fantasy performers with boxplot analysis (consistency metrics)
update_top15_fantasy_boxplot = BigQueryInsertJobOperator(
    task_id='update_top15_fantasy_boxplot',
    configuration={
        'query': {
            'query': 'CALL `update_top15_fantasy_boxplot`()',
            'useLegacySql': False,
        }
    },
    dag=dag,
    **production_args
)

# Get raw data from NBA API and store it in Google Cloud Storage
bronze_layer_1 >> bronze_layer_2

# Update the seasons reference table, then flatten the raw JSON data
bronze_layer_2 >> update_nba_seasons
update_nba_seasons >> silver_layer_1

# WAP = Write-Audit-Publish: Clean data → Check quality → Publish if good

silver_layer_1 >> wap_part_1_write

# Every single audit must pass or the pipeline stops here
# This is the critical quality gate - bad data gets trapped in staging
wap_part_1_write >> [
    audit_shooting_logic,        # Check impossible shooting stats
    audit_minutes_range,         # Check impossible playing time  
    audit_percentages,           # Check invalid percentages
    audit_rebound_logic,         # Check rebound math
    audit_negative_stats,        # Check for negative numbers
    audit_required_fields,       # Check for missing critical data
    audit_player_uniqueness      # Check for duplicate records
]

# This is the "green light" moment - data is now safe for dashboards
[
    audit_shooting_logic,
    audit_minutes_range, 
    audit_percentages,
    audit_rebound_logic,
    audit_negative_stats,
    audit_required_fields,
    audit_player_uniqueness
] >> wap_part_3_publish

# Update the player lookup table after production data is ready
wap_part_3_publish >> update_player_reference_table

# Create all your specialized analytics tables in parallel
# These are the tables that power your dashboards and reports
update_player_reference_table >> [
    update_combined_player_analysis,    # Scatter plots for volume vs efficiency
    update_fantasy_points_leaders,      # Fantasy basketball leaderboards  
    update_player_profile_data,         # Individual player detail pages
    update_player_trend_analysis,       # Recent performance vs season average
    update_player_zscore_analysis,      # Statistical player rankings
    update_top15_fantasy_boxplot        # Consistency analysis for top players
]
