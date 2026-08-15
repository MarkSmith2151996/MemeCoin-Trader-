param(
    [string]$Root = "D:\pumpapi-replay",
    [switch]$Start
)

$ErrorActionPreference = "Stop"
$RepoScript = Join-Path $PSScriptRoot "pumpapi_replay_downloader.py"
$Python = Join-Path $Root "venv\Scripts\python.exe"
$ServiceName = "PumpApiReplayDownloader"

New-Item -ItemType Directory -Force -Path $Root, "$Root\logs", "$Root\raw" | Out-Null
if (Test-Path $RepoScript) {
    Copy-Item -Force $RepoScript "$Root\downloader.py"
} elseif (-not (Test-Path "$Root\downloader.py")) {
    throw "downloader.py is missing from $Root. Run this installer from the repository scripts directory first."
}

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Windows Python launcher (py) is required. Install Python 3.11+ first."
}

if (-not (Test-Path $Python)) {
    & py -3 -m venv "$Root\venv"
}

$FreeVirtualMemoryKb = (Get-CimInstance Win32_OperatingSystem).FreeVirtualMemory
if ($FreeVirtualMemoryKb -lt 1048576) {
    throw (
        "Windows has only $FreeVirtualMemoryKb KB of free virtual memory. " +
        "Restore or increase the paging file to at least 1 GB free, reboot if required, then rerun this installer."
    )
}

$RequiredPackages = @("aiohttp", "zstandard")
foreach ($Package in $RequiredPackages) {
    & $Python -m pip install --no-cache-dir $Package
}

$Nssm = (Get-Command nssm -ErrorAction SilentlyContinue).Source
if (-not $Nssm) {
    $NssmDir = Join-Path $Root "nssm"
    $NssmZip = Join-Path $env:TEMP "nssm-2.24.zip"
    New-Item -ItemType Directory -Force -Path $NssmDir | Out-Null
    Invoke-WebRequest -Uri "https://nssm.cc/release/nssm-2.24.zip" -OutFile $NssmZip
    Expand-Archive -Force -Path $NssmZip -DestinationPath $NssmDir
    $Nssm = Join-Path $NssmDir "nssm-2.24\win64\nssm.exe"
    if (-not (Test-Path $Nssm)) {
        throw "NSSM download did not contain the expected win64 executable."
    }
}

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    & $Nssm stop $ServiceName confirm | Out-Null
    & $Nssm remove $ServiceName confirm | Out-Null
}

& $Nssm install $ServiceName $Python "$Root\downloader.py"
& $Nssm set $ServiceName AppDirectory $Root
& $Nssm set $ServiceName AppStdout "$Root\logs\downloader.log"
& $Nssm set $ServiceName AppStderr "$Root\logs\downloader.log"
& $Nssm set $ServiceName AppRotateFiles 1
& $Nssm set $ServiceName AppRotateBytes 50000000
& $Nssm set $ServiceName AppRestartDelay 30000
& $Nssm set $ServiceName AppExit Default Restart
& $Nssm set $ServiceName Start SERVICE_AUTO_START

if ($Start) {
    & $Nssm start $ServiceName
    & $Nssm status $ServiceName
} else {
    Write-Host "Service installed. Start it with: `"$Nssm`" start $ServiceName"
}
