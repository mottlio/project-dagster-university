from dagster import asset
from dagster_duckdb import DuckDBResource
from ..partitions import monthly_partition

@asset(
        partitions_def=monthly_partition,
        group_name='tennis'
)
def atp_matches_dataset(database: DuckDBResource) -> None:
    """This asset downloads the csv file with tennis match results from the API"""
    base = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
    csv_files = [
        f"{base}/atp_matches_{year}.csv"
        for year in range(1968, 2024)
    ]

    create_query = """
    CREATE OR REPLACE TABLE matches AS
    SELECT * REPLACE(
        cast(strptime(tourney_date, '%Y%m%d') AS date) as tourney_date
    )
    FROM read_csv_auto($1, types={  --$1 refers to the first argument passed to the execute function - a list of csv files
    'winner_seed': 'VARCHAR',
    'loser_seed': 'VARCHAR',
    'tourney_date': 'STRING'
    })
    """

    with database.get_connection() as conn:
        conn.execute(create_query, [csv_files])