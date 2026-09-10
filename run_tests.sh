#!/usr/bin/env bash
set -euo pipefail
mkdir -p reports
pytest -v --html=reports/report.html --self-contained-html
