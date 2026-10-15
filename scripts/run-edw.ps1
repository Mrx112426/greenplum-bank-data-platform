$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

foreach ($dag in @("edw_staging", "edw_core", "edw_dm", "edw_data_quality", "edw_pipeline")) {
    docker compose exec -T airflow-scheduler airflow dags unpause $dag
    if ($LASTEXITCODE -ne 0) { throw "Cannot unpause DAG: $dag" }
}

docker compose exec -T airflow-scheduler airflow dags trigger edw_pipeline
if ($LASTEXITCODE -ne 0) { throw "Cannot trigger edw_pipeline." }
Write-Host "EDW pipeline started. Follow edw_pipeline in http://localhost:8080"
