#!/usr/bin/env bash
set -euo pipefail

op run --env-file=.env.secrets -- dbt "$@"
