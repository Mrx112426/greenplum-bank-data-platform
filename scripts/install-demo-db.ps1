param(
    [string]$ArchivePath = ".cache/demo_big.zip",
    [string]$DownloadUrl = "https://edu.postgrespro.ru/demo-big-20161013.zip",
    [switch]$ForceDownload
)

$ErrorActionPreference = "Stop"

function Test-DemoDatabase([string]$UserName) {
    $query = @'
SELECT 'aircrafts' AS table_name,count(*) AS rows FROM bookings.aircrafts
UNION ALL SELECT 'airports',count(*) FROM bookings.airports
UNION ALL SELECT 'bookings',count(*) FROM bookings.bookings
UNION ALL SELECT 'seats',count(*) FROM bookings.seats
UNION ALL SELECT 'flights',count(*) FROM bookings.flights
UNION ALL SELECT 'tickets',count(*) FROM bookings.tickets
UNION ALL SELECT 'ticket_flights',count(*) FROM bookings.ticket_flights
UNION ALL SELECT 'boarding_passes',count(*) FROM bookings.boarding_passes;
'@
    $counts = docker compose exec -T postgres-source psql -X -At -v ON_ERROR_STOP=1 -U $UserName -d demo -c $query
    if ($LASTEXITCODE -ne 0) { throw "Demo schema is incompatible or unavailable." }
    $expected = @{
        aircrafts=9; airports=104; bookings=2111110; seats=1339;
        flights=214867; tickets=2949857; ticket_flights=8391852; boarding_passes=7925812
    }
    foreach ($line in $counts) {
        $name, $count = $line.Split('|')
        if (-not $expected.ContainsKey($name) -or [long]$count -ne $expected[$name]) {
            throw "Unexpected demo count: $line. Automatic replacement is refused."
        }
        Write-Host "$name : $count"
    }
    if ($counts.Count -ne 8) { throw "Expected eight demo tables." }
    Write-Host "PASS: eight tables, 21 594 950 rows."
}

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

    $dbUser = (docker compose exec -T postgres-source printenv POSTGRES_USER).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($dbUser)) {
        throw "Failed to read POSTGRES_USER."
    }
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        docker compose exec -T postgres-source pg_isready -U $dbUser -d postgres *> $null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) { throw "PostgreSQL did not become ready." }
    $exists = docker compose exec -T postgres-source psql -X -At -v ON_ERROR_STOP=1 -U $dbUser -d postgres -c "SELECT 1 FROM pg_database WHERE datname='demo'"
    if ($LASTEXITCODE -ne 0) { throw "Cannot check existing demo database." }
    if ($exists -eq '1') {
        Write-Host "Database demo already exists; checking it without reimport."
        Test-DemoDatabase $dbUser
        return
    }

    $archiveDirectory = Split-Path -Parent $archiveFullPath
    New-Item -ItemType Directory -Path $archiveDirectory -Force | Out-Null

    if ($ForceDownload -or -not (Test-Path -LiteralPath $archiveFullPath)) {
        $partialPath = "$archiveFullPath.partial"
        Write-Host "Downloading Postgres Pro demo database..."
        curl --location --fail --retry 3 --output $partialPath $DownloadUrl
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to download $DownloadUrl"
        }
        if (-not (Test-Path -LiteralPath $partialPath) -or (Get-Item -LiteralPath $partialPath).Length -eq 0) {
            throw "Downloaded archive is empty."
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
    Test-DemoDatabase $dbUser

    Write-Host "Demo database installation completed."
} finally {
    Pop-Location
}
