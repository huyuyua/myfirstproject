[CmdletBinding()]
param(
    [string]$TaskName = "AStockMarketTemperature",
    [string]$PythonExe = "",
    [string]$ConfigPath = "",
    [switch]$Uninstall,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path

if ($Uninstall) {
    $existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -eq $existingTask) {
        Write-Host "Task does not exist: $TaskName"
        exit 0
    }
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Scheduled task removed: $TaskName"
    exit 0
}

$collectorPath = Join-Path $scriptDirectory "market_temperature.py"
if (-not (Test-Path -LiteralPath $collectorPath -PathType Leaf)) {
    throw "Collector script not found: $collectorPath"
}

if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = Join-Path $scriptDirectory "config.json"
}
$ConfigPath = (Resolve-Path -LiteralPath $ConfigPath).Path

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($null -eq $pythonCommand) {
        throw "python.exe was not found. Pass a Python 3.9+ path through -PythonExe."
    }
    $PythonExe = $pythonCommand.Source
}
$PythonExe = (Resolve-Path -LiteralPath $PythonExe).Path

$versionText = & $PythonExe --version 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Python cannot be executed: $PythonExe"
}

# Start at 09:25 on weekdays. Python handles the 10-minute in-session schedule.
$arguments = "`"$collectorPath`" --config `"$ConfigPath`" daemon"
$action = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument $arguments `
    -WorkingDirectory $scriptDirectory
$triggerAt = [datetime]::Today.AddHours(9).AddMinutes(25)
$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -WeeksInterval 1 `
    -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At $triggerAt
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 8) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal `
    -UserId $currentUser `
    -LogonType Interactive `
    -RunLevel Limited
$task = New-ScheduledTask `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "A-share turnover temperature collection every 10 minutes during market sessions"

Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
if ($RunNow) {
    Start-ScheduledTask -TaskName $TaskName
}

$taskInfo = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Scheduled task installed: $TaskName"
Write-Host "Python: $PythonExe ($versionText)"
Write-Host "Next run: $($taskInfo.NextRunTime)"
Write-Host "Log: $(Join-Path $scriptDirectory 'logs\market_temperature.log')"
