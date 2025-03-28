import dagster as dg

from .assets import metrics, trips
from .resources import database_resource
from .jobs import trip_update_job, weekly_update_job
from .schedules import trip_update_schedule, weekly_update_schedule
# This is the main entry point for the Dagster application.
# It defines the assets, jobs, and schedules for the application.
# It also sets up the resources needed for the application to run.
# The assets are loaded from the modules defined in the assets directory.
# The jobs are defined to run the assets and the schedules are defined to run the jobs at specific times.
# The database resource is defined to connect to the DuckDB database.

trip_assets = dg.load_assets_from_modules([trips])
metric_assets = dg.load_assets_from_modules([metrics])
all_jobs = [trip_update_job, weekly_update_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]

defs = dg.Definitions(
    assets=[*trip_assets, *metric_assets],
    resources={"database": database_resource},
    jobs=all_jobs,
    schedules=all_schedules,
)
