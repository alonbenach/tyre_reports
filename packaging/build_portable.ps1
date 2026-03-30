param(
    [string]$Python = ".\.venv\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$specPath = Join-Path $projectRoot "packaging\MotoWeeklyOperator.spec"
$distRoot = Join-Path $projectRoot "dist"
$buildRoot = Join-Path $projectRoot "build"
$packageRoot = Join-Path $distRoot "MotoWeeklyOperator"

Push-Location $projectRoot
try {
    & $Python -m PyInstaller --noconfirm --clean $specPath

    $directories = @(
        "database",
        "data",
        "data\campaign rules",
        "data\ingest",
        "data\raw",
        "reports",
        "logs",
        "assets",
        "assets\logos"
    )

    foreach ($relativePath in $directories) {
        $target = Join-Path $packageRoot $relativePath
        New-Item -ItemType Directory -Force -Path $target | Out-Null
    }

    $referenceReadme = @"
Place the approved reference workbooks for the current motorcycle campaign in this folder:

- canonical fitment mapping.xlsx
- price list Pirelli and competitors.xlsx
- campaign 2026.xlsx

Future versions of the app may manage these files through admin workflows, but for now they are maintained manually.
"@
    Set-Content -Path (Join-Path $packageRoot "data\campaign rules\README.txt") -Value $referenceReadme -Encoding UTF8

    $intakeReadme = @"
Stage weekly CSV files here only when working outside the GUI.

The normal operator workflow should stage files through the application interface.
"@
    Set-Content -Path (Join-Path $packageRoot "data\ingest\README.txt") -Value $intakeReadme -Encoding UTF8

    $logsReadme = "Application run logs will be written here automatically."
    Set-Content -Path (Join-Path $packageRoot "logs\README.txt") -Value $logsReadme -Encoding UTF8

    $reportsReadme = "Generated Excel and PDF reports will be written here automatically."
    Set-Content -Path (Join-Path $packageRoot "reports\README.txt") -Value $reportsReadme -Encoding UTF8

    Write-Host "Portable package prepared at: $packageRoot"
}
finally {
    Pop-Location
}
