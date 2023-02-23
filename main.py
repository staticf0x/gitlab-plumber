#!/usr/bin/env python3
import os
from typing import List

import arrow
import click
import dotenv
import gitlab
import rich

dotenv.load_dotenv()
gl = gitlab.Gitlab(os.getenv("GITLAB_URI"), private_token=os.getenv("PRIVATE_TOKEN"))


def print_stage(stage_name: str, stage_jobs: List, pipeline):
    rich.print(f"[yellow]{stage_name}[/yellow]")

    stage_duration = 0

    if any(job.status != "manual" for job in stage_jobs):
        first_job = min(arrow.get(job.started_at) for job in stage_jobs if job.status != "manual")
        last_job = max(arrow.get(job.finished_at) for job in stage_jobs if job.status != "manual")

        delta = last_job - first_job
        stage_duration = delta.total_seconds()
        stage_perc = stage_duration / pipeline.duration

        print(f"  {stage_duration:.1f}s ({stage_perc:.1%})\n")

    for job in stage_jobs:
        match job.status:
            case "failed":
                color = "red"
            case "pending":
                color = "yellow"
            case "running":
                color = "cyan"
            case "success":
                color = "green"
            case "manual":
                color = "white"
            case _:
                color = "grey"

        display_name = job.name

        if job.allow_failure and job.status == "failed":
            display_name = f"({job.name})"

        rich.print(f"  [{color}]{display_name}[/{color}]")

        queue_duration = job.queued_duration
        duration = job.duration

        if queue_duration and duration:
            perc_time = duration / stage_duration
            print(f"    {queue_duration:.1f}s + {duration:.1f}s ({perc_time:.1%})")

    print()


@click.group()
def cli():
    pass


@cli.command(help="Display information about a pipeline")
@click.option("--project", "-p", required=True)
@click.option("--pipeline", required=True)
def pipeline(project: int, pipeline: int):
    project = gl.projects.get(project)
    pipeline = project.pipelines.get(pipeline)

    jobs = reversed(pipeline.jobs.list())
    stages = {}

    for job in jobs:
        stages.setdefault(job.stage, [])
        stages[job.stage].append(job)

    for stage, stage_jobs in stages.items():
        print_stage(stage, stage_jobs, pipeline)

    p_queued = pipeline.queued_duration or 0

    total_time = p_queued + pipeline.duration
    duration_str = f"{total_time//60:d}m {total_time%60:d}s"

    print(f"Total: {p_queued:.1f}s + {pipeline.duration:.1f}s = {duration_str}")


if __name__ == "__main__":
    if not os.getenv("GITLAB_URI"):
        rich.print("[bold red]GITLAB_URI not configured[/bold red]")
        exit(1)

    if not os.getenv("PRIVATE_TOKEN"):
        rich.print("[bold red]PRIVATE_TOKEN not configured[/bold red]")
        exit(1)

    cli()
