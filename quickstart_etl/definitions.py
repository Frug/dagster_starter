from pathlib import Path

from dagster import AssetExecutionContext
from dagster import Definitions
from dagster import Output
from dagster import ScheduleDefinition
from dagster import asset
from dagster import define_asset_job
from dagster import graph_asset
from dagster import link_code_references_to_git
from dagster import load_assets_from_package_module
from dagster import op
from dagster import with_source_code_references
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping
from dagster._core.definitions.partition import StaticPartitionsDefinition

from . import assets

daily_refresh_schedule = ScheduleDefinition(job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *")


@op
def foo_op():
    return 5


@graph_asset
def my_asset():
    return foo_op()


LETTER_PARTITIONS = StaticPartitionsDefinition(["A", "B", "C"])


@asset(
    partitions_def=LETTER_PARTITIONS,
)
def create_partitions_data(context: AssetExecutionContext) -> str:
    letter = context.partition_key
    context.log.info(f"Creating partition data for {letter}")
    return Output(
        value=letter,
        metadata={"letter": letter},
    )


@asset(
    deps=[create_partitions_data],
)
def combine_partitions(context: AssetExecutionContext, create_partitions_data: dict[str, str]) -> None:
    context.log.info(f"Combining {len(create_partitions_data)} partitions")
    combined_data = "".join(create_partitions_data.values())
    context.log.info(combined_data)

    return None




my_assets = with_source_code_references(
    [
        my_asset,
        create_partitions_data,
        combine_partitions,
        *load_assets_from_package_module(assets),
    ]
)

my_assets = link_code_references_to_git(
    assets_defs=my_assets,
    git_url="https://github.com/dagster-io/dagster/",
    git_branch="master",
    file_path_mapping=AnchorBasedFilePathMapping(
        local_file_anchor=Path(__file__).parent,
        file_anchor_path_in_repository="examples/quickstart_etl/quickstart_etl/",
    ),
)

defs = Definitions(
    assets=my_assets,
    schedules=[daily_refresh_schedule],
)
