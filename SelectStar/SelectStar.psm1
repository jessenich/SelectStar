Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$Script:Connections = @{}
#$Script:Providers = @{}

#Add Type for translating DataReader to PSObject
#Repo for source code is at https://github.com/mithrandyr/DataReaderToPSObject
. (Join-Path $PSScriptRoot "Convert-DataReaderToPSObject.ps1")
. (Join-Path $PSScriptRoot "DataReaderMap.ps1")

#Load up base Classes
. (Join-Path $PSScriptRoot "Classes.ps1"

#Load Up Internal Functions
. (Join-Path $PSScriptRoot "Functions" "TestConnectionName.ps1"

#Load up providers
ForEach($f in Get-ChildItem "$PSScriptRoot\Providers\" -Directory) {
    $Configfile = (Join-Path $f.FullName ("config.ps1" -f $f.name))

    If(Test-Path $ConfigFile) {
        Try { . $ConfigFile }
        Catch { Write-Warning ("'{0}' Provider Failed to Load: {1}" -f $f.Name, $_.ToString()) }
    }
}

If (@(Get-Command -Module SelectStar -Verb Open).Count -eq 0) {
    Write-Error "No Providers were loaded!"
}
Else {
    Get-ChildItem (Join-Path $PSScriptRoot "Cmdlets") -File | ForEach-Object { . $_.FullName }
}

Remove-Variable f, Configfile
