# MT-584: install Strategy B as an NSSM Windows service with auto-restart.
#
# The service runs the WSL-side strategy B loop:
#   wsl -u dev bash -c "cd /home/dev/projects/memecoin-trader && source .env && python3 scripts/run_strategy_b.py"
#
# NSSM restarts the loop 10s after any crash; logs rotate daily.
#
# KNOWN BLOCKER (MT-584, resolved by keeping the cron watchdog instead):
# NSSM defaults to LocalSystem, and WSL refuses non-interactive accounts
# (WSL_E_LOCAL_SYSTEM_NOT_SUPPORTED; NetworkService fails with
# WSL_E_DEFAULT_DISTRO_NOT_FOUND). The service MUST run as the interactive
# Windows user. Set the account before starting:
#   & $NssmPath set StrategyB ObjectName ".\Big A" <password>
# or run `nssm edit StrategyB` and set the Log on account in the GUI.
# The existing watchdog_memecoin.sh cron job (every 3 min) remains the
# production crash-restart mechanism.
param(
    [string]$NssmPath = "D:\pumpapi-replay\nssm\nssm-2.24\win64\nssm.exe",
    [switch]$SkipKill
)

$ErrorActionPreference = "Stop"
$ServiceName = "StrategyB"
$LogDir = "D:\memecoin-logs"
$App = "wsl.exe"
$AppArgs = '-u dev bash -c "cd /home/dev/projects/memecoin-trader && source .env && python3 scripts/run_strategy_b.py"'

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
if (-not (Test-Path $NssmPath)) {
    throw "NSSM not found at $NssmPath. Set -NssmPath to the nssm.exe location."
}

if (-not $SkipKill) {
    # Kill any existing strategy B loop so the service can't double-trade.
    # The [.] bracket regex avoids pkill matching this shell's own command line.
    & wsl -u dev bash -c "pkill -f 'python3 scripts/run_strategy_b[.]py'" | Out-Null
    Start-Sleep -Seconds 2
}

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    & $NssmPath stop $ServiceName confirm | Out-Null
    & $NssmPath remove $ServiceName confirm | Out-Null
}

& $NssmPath install $ServiceName $App $AppArgs
& $NssmPath set $ServiceName AppDirectory D:\
& $NssmPath set $ServiceName AppStdout "$LogDir\strategy_b_stdout.log"
& $NssmPath set $ServiceName AppStderr "$LogDir\strategy_b_stderr.log"
& $NssmPath set $ServiceName AppRotateFiles 1
& $NssmPath set $ServiceName AppRotateSeconds 86400
& $NssmPath set $ServiceName AppRestartDelay 10000
# NSSM default AppExit action is Restart — a crash restarts after the delay.
& $NssmPath set $ServiceName Start SERVICE_AUTO_START

& $NssmPath start $ServiceName
& $NssmPath status $ServiceName
