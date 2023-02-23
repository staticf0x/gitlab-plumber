# gl-cli

## Installation

- `$ poetry install`

## Configuration

Create `.env` file with the following content:

```
GITLAB_URI=<GitLab instance URL>
PRIVATE_TOKEN=<Your private token>
```

## Running

- `$ poetry run python3 main.py --project <PROJECT_ID> --pipeline <PIPELINE_ID>`
