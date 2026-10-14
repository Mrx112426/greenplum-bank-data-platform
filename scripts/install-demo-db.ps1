param(
    [string]$ArchivePath = ".cache/demo_big.zip",
    [string]$DownloadUrl = "https://edu.postgrespro.ru/demo_big.zip",
    [switch]$ForceDownload
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$archiveFullPath = if ([IO.Path]::IsPathRooted($ArchivePath)) {
    $ArchivePath
} else {
    Join-Path $repoRoot $ArchivePath
}

Push-Location $repoRoot
try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop is not running."
    }

    docker compose up -d postgres-source
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start postgres-source."
    }

    $archiveDirectory = Split-Path -Parent $archiveFullPath
    New-Item -ItemType Directory -Path $archiveDirectory -Force | Out-Null

    if ($ForceDownload -or -not (Test-Path -LiteralPath $archiveFullPath)) {
        $partialPath = "$archiveFullPath.partial"
        Write-Host "Downloading Postgres Pro demo database..."
        curl.exe -L --fail --retry 3 --output $partialPath $DownloadUrl
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to download $DownloadUrl"
        }
        Move-Item -LiteralPath $partialPath -Destination $archiveFullPath -Force
    } else {
        Write-Host "Using cached archive: $archiveFullPath"
    }

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [IO.Compression.ZipFile]::OpenRead($archiveFullPath)
    try {
        $entry = $zip.GetEntry("demo_big.sql")
        if ($null -eq $entry) {
            throw "The archive does not contain demo_big.sql."
        }

        $dbUser = (docker compose exec -T postgres-source printenv POSTGRES_USER).Trim()
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($dbUser)) {
            throw "Failed to read POSTGRES_USER from postgres-source."
        }

        Write-Host "Closing active connections to the demo database..."
        docker compose exec -T postgres-source psql `
            -X -v ON_ERROR_STOP=1 -U $dbUser -d postgres `
            -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'demo' AND pid <> pg_backend_pid();"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to prepare PostgreSQL for import."
        }

        Write-Host "Importing demo_big.sql. This can take several minutes..."
        $processInfo = [Diagnostics.ProcessStartInfo]::new()
        $processInfo.FileName = "docker"
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardInput = $true
        foreach ($argument in @(
            "compose", "exec", "-T", "postgres-source",
            "psql", "-X", "-v", "ON_ERROR_STOP=0",
            "-U", $dbUser, "-d", "postgres"
        )) {
            $processInfo.ArgumentList.Add($argument)
        }

        $process = [Diagnostics.Process]::Start($processInfo)
        $sqlStream = $entry.Open()
        try {
            $sqlStream.CopyTo($process.StandardInput.BaseStream)
            $process.StandardInput.Close()
            $process.WaitForExit()
            $exitCode = $process.ExitCode
        } finally {
            $sqlStream.Dispose()
            $process.Dispose()
        }

        if ($exitCode -ne 0) {
            throw "PostgreSQL import failed with exit code $exitCode."
        }
    } finally {
        $zip.Dispose()
    }

    Write-Host "Verifying the installed database..."
    docker compose exec -T postgres-source psql `
        -X -v ON_ERROR_STOP=1 -U $dbUser -d demo `
        -c "SELECT count(*) AS flights FROM bookings.flights;" `
        -c "SELECT pg_size_pretty(pg_database_size('demo')) AS database_size;"
    if ($LASTEXITCODE -ne 0) {
        throw "The demo database was imported but verification failed."
    }

    Write-Host "Demo database installation completed."
} finally {
    Pop-Location
}
