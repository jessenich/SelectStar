[CmdletBinding()]
param(
    [switch]$Load,
    [switch]$NoTest,
    [string]$TestName
)

if ($Load) {
    Write-Host "Original PID: $pid"
    if ($NoTest) {
        $cmd = "{0} -NoTest" -f $PSCmdlet.MyInvocation.MyCommand.Source
    }
    elseif ($TestName) {
        $cmd = "{0} -TestName '{1}'" -f $PSCmdlet.MyInvocation.MyCommand.Source, $TestName
    }
    else {
        $cmd = "{0}" -f $PSCmdlet.MyInvocation.MyCommand.Source
    }

    PowerShell -noprofile -noexit -command $cmd

    if ($global:IsNestedSessionSelectStar) {
        Write-Warning "Exited one session, but currently in another nested session!"
    }
    else {
        Write-Warning "You have exited the last nested session."
    }
}
else {
    Write-Host "Session PID: $pid"
    #Clear-Host
    Write-Host "In New PowerShell Session, [exit] to resume."
    $global:IsNestedSessionSelectStar = $true

    $PSModuleAutoLoadingPreference = "none"
    Import-Module $PSScriptRoot\SelectStar -Force
    Get-Module SelectStar | Where-Object Path -NotLike "$PSScriptRoot\*" | Remove-Module
    Import-Module Pester -Force
    Write-Host ("Loaded '{0}' of SelectStar!" -f (Get-Module SelectStar).Version.ToString())

    if (-not $NoTest) {
        if ($TestName) {
            Invoke-Pester -Script $PSScriptRoot -FullNameFilter $TestName -Show All
        }
        else {
            Invoke-Pester -Script $PSScriptRoot -Show All
        }
    }
    <#Get-Module SelectStar | Format-List

    Get-SqlProviderHelp -Provider SQL
    Get-SqlProviderHelp -Provider SQLite

    Open-SqlConnection -DataSource it4 -InitialCatalog sandbox

    Show-SqlConnection

    isq "select @a" -Parameters @{a=1}

    Get-SqlProviderHelp -Provider SQL

    #>
}
