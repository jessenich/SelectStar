Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$Script:Connections = @{}
#$Script:Providers = @{}

#Add Type for translating DataReader to PSObject
#Repo for source code is at https://github.com/mithrandyr/DataReaderToPSObject
. (Join-Path $PSScriptRoot "Convert-DataReaderToPSObject.ps1")
. (Join-Path $PSScriptRoot "DataReaderMap.ps1")

#Load up base Classes
. (Join-Path $PSScriptRoot "Classes.ps1")

#Load Up Internal Functions
. (Join-Path $PSScriptRoot "Functions" "TestConnectionName.ps1")

#Load up providers
Get-ChildItem "$PSScriptRoot\Providers\" -Directory | ForEach-Object {
    $directory = $PSItem;
    $configFile = (Join-Path $directory.FullName "config.ps1")

    if (Test-Path $configFile) {
        try {
            . $configFile
        }
        catch {
            Write-Warning ("'{0}' Provider Failed to Load: {1}" -f $directory.Name, $_.ToString())
        }
    }
}

$Private:Commands = Get-Command -Module SelectStar -Verb Open;
if ($null -eq $Private:Commands -or $Private:Commands.Count -eq 0) {
    Write-Error "No Providers were loaded!"
}
else {
    Get-ChildItem (Join-Path $PSScriptRoot "Cmdlets") -File | ForEach-Object { . $_.FullName }
}

Remove-Variable directory, configFile
