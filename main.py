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

    if pipeline.duration and any(job.status != "manual" for job in stage_jobs):
        if all(job.finished_at is not None for job in stage_jobs if job.status != "manual"):
            first_job = min(
                arrow.get(job.started_at) for job in stage_jobs if job.status != "manual"
            )
            last_job = max(
                arrow.get(job.finished_at) for job in stage_jobs if job.status != "manual"
            )

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

        rich.print(f"  [{color}][link={job.web_url}]{display_name}[/link][/{color}]")

        queue_duration = job.queued_duration
        duration = job.duration
        perc_time = 0

        if stage_duration and duration is not None:
            perc_time = duration / stage_duration

        if queue_duration and duration:
            print(f"    {queue_duration:.1f}s + {duration:.1f}s", end="")

            if perc_time:
                print(f" ({perc_time:.1%})")
            else:
                print()

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

    jobs = reversed(pipeline.jobs.list(all=True))
    stages = {}

    for job in jobs:
        stages.setdefault(job.stage, [])
        stages[job.stage].append(job)

    for stage, stage_jobs in stages.items():
        print_stage(stage, stage_jobs, pipeline)

    p_queued = pipeline.queued_duration or 0

    if pipeline.duration:
        total_time = p_queued + pipeline.duration
        duration_str = f"{total_time//60:d}m {total_time%60:d}s"

        print(f"Total: {p_queued:.1f}s + {pipeline.duration:.1f}s = {duration_str}")
    else:
        print("Can't show total time: pipeline hasn't finished yet.")


if __name__ == "__main__":
    if not os.getenv("GITLAB_URI"):
        rich.print("[bold red]GITLAB_URI not configured[/bold red]")
        exit(1)

    if not os.getenv("PRIVATE_TOKEN"):
        rich.print("[bold red]PRIVATE_TOKEN not configured[/bold red]")
        exit(1)

    cli()
