Filter TestConnectionName {
    Param(
        [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
        [string]$ConnectionName,

        [Parameter()]
        [switch]$Quiet
    )

    If(-not $Script:Connections.ContainsKey($ConnectionName)) {
        If(-not $Quiet.IsPresent) {
            If($ConnectionName -eq "Default") { Write-Warning "There is no active SQL Connection."}
            Else { Write-Warning "There is no active SQL connection ($ConnectionName)."}
        }
        Return $false
    }
    Else {
        Return $true
    }
}
