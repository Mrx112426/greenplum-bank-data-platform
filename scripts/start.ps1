$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI is not installed or is not available in PATH."
}

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop is not running."
}

if (-not (Test-Path -LiteralPath ".env")) {
    Copy-Item -LiteralPath ".env.example" -Destination ".env"
    Write-Host "Created .env from .env.example"
}

$envContent = Get-Content -LiteralPath ".env" -Raw
if ($envContent -match "(?m)^AIRFLOW_FERNET_KEY=\s*$") {
    $keyBytes = New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Fill($keyBytes)
    $fernetKey = [Convert]::ToBase64String($keyBytes).TrimEnd("=").Replace("+", "-").Replace("/", "_") + "="
    $envContent = $envContent -replace "(?m)^AIRFLOW_FERNET_KEY=\s*$", "AIRFLOW_FERNET_KEY=$fernetKey"
    Set-Content -LiteralPath ".env" -Value $envContent -Encoding utf8 -NoNewline
    Write-Host "Generated AIRFLOW_FERNET_KEY in .env"
}

docker compose build airflow-init
if ($LASTEXITCODE -ne 0) { throw "Airflow image build failed." }

docker compose up airflow-init
if ($LASTEXITCODE -ne 0) { throw "Airflow initialization failed." }

docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "Platform startup failed." }

docker compose ps

Write-Host ""
Write-Host "Airflow: http://localhost:8080"
Write-Host "MinIO:   http://localhost:9001"
Write-Host "Postgres source: localhost:5433"
Write-Host "Greenplum DWH:   localhost:5434"
