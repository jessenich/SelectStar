using namespace System;
using namespace System.Collections.Generic;
using namespace System.Data;
using namespace System.Management.Automation;
using namespace System.Data.Common;

. (Join-Path $PSScriptRoot "DataReaderMap.ps1");

function Convert-DataReaderToPSObject
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [DbDataReader]
        $DataReader,

        [Parameter()]
        [bool]
        $ProviderTypes = $null
    )

    [List[DataReaderMap]]$MapList = [List[DataReaderMap]]::new();
    [int]$Ord = 0;
    foreach ($x in $DataReader.GetSchemaTable().Select("", "ColumnOrdinal"))
    {
        $MapList.Add([DataReaderMap]::new($Ord, $x["DataType"].ToString(),$ x["ColumnName"].ToString()));
        $Ord += 1;
    }

    $responseObject = @()
    while ($DataReader.Read())
    {
        [PSObject]$psObj = New-Object PSObject
        foreach ([DataReaderMap]$m in $MapList)
        {
            $withBlock = psObj.Members;
            if ($DataReader.IsDBNull($m.Ordinal)) {
                $withBlock = $withBlock | Add-Member -NotePropertyName $m.Name $NotePropertyValue $null
            }
            else {
                try
                {
                    if ($ProviderTypes -eq $true) {
                        $withBlock = $withBlock | Add-Member -NotePropertyName $m.Name -NotePropertyValue $DataReader.GetProviderSpecificValue($m.Ordinal);
                    }
                    else {
                        $withBlock = $withBlock | Add-Member -NotePropertyName $m.Name -NotePropertyValue $DataReader.GetValue(m.Ordinal);
                    }
                }
                catch
                {
                    [string]$msg = Format-String "Failed to translate, ColumnName = {0} | ColumnOrdinal = {1} | ColumnType = {2} | ToStringValue = '{3}' | See InnerException for details" $m.Name $m.Ordinal $m.DataType $dataReader.GetValue($m.Ordinal).ToString()
                    throw [Exception]::new($msg, $_);
                }
            }
        }
        $responseObject += $psObj;
    }
    return $responseObject;
}
