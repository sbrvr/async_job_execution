"""Helpers shared by the task scripts."""
import argparse

ENTITY_TYPES = ("JOB", "PIPELINE")


def get_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def get_workspace_client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes")


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--tracker_table", default="surajb.common.entity_tracker",
                        help="Fully qualified name of the tracker table")
    return parser


def resolve_entity(w, entity_type: str, name: str):
    """Exact, unique name match -> (id, error). Job and pipeline names are not unique in Databricks."""
    if entity_type == "JOB":
        ids = [str(j.job_id) for j in w.jobs.list(name=name) if j.settings and j.settings.name == name]
    else:
        escaped = name.replace("'", "''")  # LIKE treats '_' and '%' as wildcards, so re-check the exact name
        ids = [p.pipeline_id for p in w.pipelines.list_pipelines(filter=f"name LIKE '{escaped}'") if p.name == name]
    if len(ids) != 1:
        return None, f"expected exactly 1 {entity_type} named '{name}', found {len(ids)}: {ids}"
    return ids[0], None
