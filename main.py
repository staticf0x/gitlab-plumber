#!/usr/bin/env python3
import os
import re
from dataclasses import dataclass

import arrow
import click
import dotenv
import gitlab
import rich
from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn
from rich.status import Status
from rich.style import Style
from rich.tree import Tree


@dataclass
class JobDuration:
    queue: float | None
    run: float | None


@dataclass
class StageDuration:
    run: float
    jobs: dict[str, JobDuration]

    def __lt__(self, other):
        return self.run < other.run


dotenv.load_dotenv()
gl = gitlab.Gitlab(os.getenv("GITLAB_URI"), private_token=os.getenv("PRIVATE_TOKEN"))


def parse_url(url: str) -> tuple[str, int]:
    """Parse pipeline URL into project and pipeline ID."""
    url_without_host = url.replace(os.getenv("GITLAB_URI"), "").lstrip("/")

    if not (m := re.findall(r"\/?([\w+\/\w+]+)\/\-\/pipelines\/(\d+)", url_without_host)):
        raise ValueError("Cannot parse pipeline URL")

    return m[0]


def get_stage_tree(stage_name: str, stage_jobs: list, pipeline) -> Tree:
    """Get a rich.tree.Tree for a given stage, including its jobs."""
    stage_duration = 0.0
    stage_duration_str = ""

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

            stage_duration_str = f"[bright_black]{stage_duration:.1f}s ({stage_perc:.1%})[/]"

    tree = Tree(f"[yellow]{stage_name}[/yellow] {stage_duration_str}")

    for job in stage_jobs:
        status_indicator = ""

        match job.status:
            case "failed":
                color = "red"
            case "pending":
                color = "yellow"
                status_indicator = "💤 "
            case "running":
                color = "cyan"
                status_indicator = "⏳ "
            case "success":
                color = "green"
            case "manual":
                color = "white"
            case _:
                color = "grey"

        display_name = job.name

        if job.allow_failure and job.status == "failed":
            display_name = f"({job.name})"

        node = f"{status_indicator}[{color}][link={job.web_url}]{display_name}[/link][/{color}]"

        queue_duration = job.queued_duration
        duration = job.duration
        perc_time = 0

        if stage_duration and duration is not None:
            perc_time = duration / stage_duration

        if queue_duration and duration:
            node += f" [bright_black]{queue_duration:.1f}s + {duration:.1f}s[/]"

            if perc_time:
                node += f" [bright_black]({perc_time:.1%})[/]"

        tree.add(node)

    return tree


def get_stage_duration(stage_name: str, stage_jobs: list) -> StageDuration:
    """Get stage and job durations for a given stage."""
    stage_duration = 0.0

    if any(job.status != "manual" for job in stage_jobs):
        if all(job.finished_at is not None for job in stage_jobs if job.status != "manual"):
            first_job = min(
                arrow.get(job.started_at) for job in stage_jobs if job.status != "manual"
            )
            last_job = max(
                arrow.get(job.finished_at) for job in stage_jobs if job.status != "manual"
            )

            delta = last_job - first_job
            stage_duration = delta.total_seconds()

    job_durations = {}

    for job in stage_jobs:
        job_durations[job.name] = JobDuration(queue=job.queued_duration, run=job.duration)

    return StageDuration(run=stage_duration, jobs=job_durations)


@click.group()
def cli():
    pass


@cli.command(help="Show a single pipeline run as a tree of jobs")
@click.option("--project", "-p", required=False)
@click.option("--pipeline", required=False)
@click.option("--url", required=False)
def show(project: int, pipeline: int, url: str):
    if not (url or (project and pipeline)):
        print("Required options missing: need --project and --pipeline or --url")
        return

    if url:
        project, pipeline = parse_url(url)

    with Status("Loading pipeline jobs..."):
        project = gl.projects.get(project)
        pipeline = project.pipelines.get(pipeline)
        jobs = reversed(pipeline.jobs.list(all=True))

    stages = {}

    # Split jobs into stages
    for job in jobs:
        stages.setdefault(job.stage, [])
        stages[job.stage].append(job)

    # Print the job tree
    main_tree = Tree(
        f"[b][link={pipeline.web_url}]Pipeline {pipeline.id}[/link][/] in {project.path_with_namespace}\n"
        f"[grey50]Started at {arrow.get(pipeline.created_at).to('local').format('YYYY-MM-DD HH:mm')} by {pipeline.user['username']}[/]"
    )

    for stage, stage_jobs in stages.items():
        sub_tree = get_stage_tree(stage, stage_jobs, pipeline)
        main_tree.add(sub_tree)

    rich.print(main_tree)
    print()

    # Total duration
    p_queued = pipeline.queued_duration or 0

    if pipeline.duration:
        total_time = p_queued + pipeline.duration
        duration_str = f"{total_time//60:d}m {total_time%60:d}s"

        print(f"Total: {p_queued:.1f}s + {pipeline.duration:.1f}s = {duration_str}")
    else:
        print("Can't show total time: pipeline hasn't finished yet.")


@cli.command(help="Show a blame graph of long running jobs")
@click.option("--project", "-p", required=False)
@click.option("--pipeline", required=False)
@click.option("--url", required=False)
def blame(project: int, pipeline: int, url: str):
    if not (url or (project and pipeline)):
        print("Required options missing: need --project and --pipeline or --url")
        return

    if url:
        project, pipeline = parse_url(url)

    with Status("Loading pipeline jobs..."):
        project = gl.projects.get(project)
        pipeline = project.pipelines.get(pipeline)
        jobs = reversed(pipeline.jobs.list(all=True))

    if not pipeline.finished_at:
        rich.print("[bold red]Cannot analyze running pipelines[/]")
        return

    console = Console(highlight=False)
    console.print(
        f"[b][link={pipeline.web_url}]Pipeline {pipeline.id}[/link][/] in {project.path_with_namespace}\n"
        f"[grey50]Started at {arrow.get(pipeline.created_at).to('local').format('YYYY-MM-DD HH:mm')} by {pipeline.user['username']}[/]\n"
    )

    stages = {}

    # Split jobs into stages
    for job in jobs:
        stages.setdefault(job.stage, [])
        stages[job.stage].append(job)

    stage_durations: dict[str, StageDuration] = {}

    for stage, stage_jobs in stages.items():
        stage_duration = get_stage_duration(stage, stage_jobs)
        stage_durations[stage] = stage_duration

    total = max(stage_durations.items(), key=lambda v: v[1].run)[1].run

    bar = Progress(
        TextColumn("{task.description}"),
        BarColumn(bar_width=50, finished_style="bar.complete"),
        TextColumn("{task.completed:.1f}s"),
        TextColumn("({task.fields[perc_time]:.1%})"),
    )

    # Breakdown by stage
    for stage, stage_duration in sorted(stage_durations.items(), key=lambda v: v[1], reverse=True):
        bar.add_task(
            stage,
            total=total,
            completed=stage_duration.run,
            perc_time=stage_duration.run / pipeline.duration,
        )

    console.print("[u][b]Breakdown by stage[/]\n")
    console.print(bar)

    # Breakdown by job for each stage
    for stage, stage_duration in stage_durations.items():
        job_durations = [(k, v) for k, v in stage_duration.jobs.items() if v.run is not None]

        bar = Progress(
            TextColumn("{task.description}"),
            BarColumn(
                bar_width=50,
                finished_style=Style(color="white"),
                complete_style=Style(color="white"),
            ),
            TextColumn("{task.completed:.1f}s"),
            TextColumn("({task.fields[perc_time]:.1%})"),
        )

        try:
            total = max(job_durations, key=lambda v: v[1].run)[1].run
        except ValueError:
            # When there are only manual jobs, the job_durations is empty
            continue

        for job, job_duration in sorted(job_durations, key=lambda v: v[1].run, reverse=True):
            bar.add_task(
                job, total=total, completed=job_duration.run, perc_time=job_duration.run / total
            )

        console.print(f"\n[u]Stage: [b]{stage}[/]\n")
        console.print(bar)


if __name__ == "__main__":
    if not os.getenv("GITLAB_URI"):
        rich.print("[bold red]GITLAB_URI not configured[/bold red]")
        exit(1)

    if not os.getenv("PRIVATE_TOKEN"):
        rich.print("[bold red]PRIVATE_TOKEN not configured[/bold red]")
        exit(1)

    cli()
