using namespace System;
using namespace System.Data;
using namespace System.Data.Common;
using namespace System.Diagnostics;
using namespace System.Collections.Generic;
using namespace System.Management.Automation;

Class SqlMessage {
    [datetime]$Received;
    [string]$Message
}

Class ProviderBase {
    [string]$ConnectionName
    [int]$CommandTimeout = 30
    [IDbConnection]$Connection
    [IDbTransaction]$Transaction
    [Queue[SqlMessage]]$Messages = (New-Object 'Collections.Generic.Queue[SqlMessage]')

    ProviderBase() { If($this.GetType().Name -eq "ProviderBase") { Throw [InvalidOperationException]::new("ProviderBase must be inherited!") } }

    [PSCustomObject] ConnectionInfo() { Throw [NotImplementedException]::new("ProviderBase.ConnectionInfo must be overloaded!") }

    [void] ChangeDatabase([string]$DatabaseName) { Throw [NotImplementedException]::new("ProviderBase.ChangeDatabase must be overloaded!") }

    [string] ProviderType() { Throw [NotImplementedException]::new("ProviderBase.ProviderType must be overloaded!") }

    [IDbCommand] GetCommand([string]$Query, [int]$cmdTimeout, [hashtable]$Parameters) {
        If($cmdTimeout -lt 0) { $cmdTimeout = $this.CommandTimeout }
        $cmd = $this.Connection.CreateCommand()
        $cmd.CommandText = $Query
        $cmd.CommandTimeout = $cmdTimeout
        if($this.HasTransaction()) { $cmd.Transaction = $this.Transaction } # apply transaction to command if connection has transaction

        ForEach($de in $Parameters.GetEnumerator()) {
            $param = $cmd.CreateParameter()
            $param.ParameterName = $de.Name
            If($de.Value -ne $null) { $param.Value = $de.Value }
            Else { $param.Value = [DBNull]::Value }
            $cmd.Parameters.Add($param)
        }

        Return $cmd
    }

    [Object] GetScalar([string]$Query, [int]$cmdTimeout, [hashtable]$Parameters) {
        $cmd = $this.GetCommand($Query, $cmdTimeout, $Parameters)
        Try { return $cmd.ExecuteScalar() }
        Catch { Throw $_ }
        Finally { $cmd.Dispose() }
    }

    [IDataReader] GetReader([string]$Query, [int]$cmdTimeout, [hashtable]$Parameters) {
        Return $this.GetCommand($Query, $cmdTimeout, $Parameters).ExecuteReader()
    }

    [long] Update([string]$Query, [int]$cmdTimeout, [hashtable]$Parameters) {
        $cmd = $this.GetCommand($Query, $cmdTimeout, $Parameters)
        Try { return $cmd.ExecuteNonQuery() }
        Catch { Throw $_ }
        Finally { $cmd.Dispose() }
    }

    [DataSet] GetDataSet([IDbCommand]$cmd, [Boolean]$ProviderTypes) { Throw [NotImplementedException]::new("ProviderBase.GetDataSet must be overloaded!") }

    [long] BulkLoad([IDataReader]$DataReader
                    , [string]$DestinationTable
                    , [hashtable]$ColumnMap = @{}
                    , [int]$BatchSize
                    , [int]$BatchTimeout
                    , [ScriptBlock]$Notify) {

        $SchemaMap = @()
        [long]$batchIteration = 0
        [int]$ord = 0
        $DataReader.GetSchemaTable().Rows | Sort-Object ColumnOrdinal | ForEach-Object { $SchemaMap += [PSCustomObject]@{Ordinal = $ord; SrcName = $_["ColumnName"]; DestName = $_["ColumnName"]}; $ord += 1}

        If($ColumnMap -and $ColumnMap.Count -gt 0) {
            $SchemaMap = $SchemaMap |
                Where-Object SrcName -In $ColumnMap.Keys |
                ForEach-Object { $_.DestName = $ColumnMap[$_.SrcName]; $_ }
        }

        [string[]]$DestNames = $SchemaMap | Select-Object -ExpandProperty DestName
        [string]$InsertSql = "INSERT INTO {0} ([{1}]) VALUES (@Param{2})" -f $DestinationTable, ($DestNames -join "], ["), (($SchemaMap | ForEach-Object Ordinal) -join ", @Param")

        $bulkCmd = $this.GetCommand($InsertSql, -1, @{})
        Try {
            $bulkCmd.Transaction = $this.Connection.BeginTransaction()
            $sw = [Stopwatch]::StartNew()
            [bool]$hasPrepared = $false
            While($DataReader.Read()) {

                If(-not $hasPrepared) {
                    ForEach($sm in $SchemaMap) {
                        $param = $bulkCmd.CreateParameter()
                        $param.ParameterName = "Param{0}" -f $sm.Ordinal
                        $param.Value = $DataReader.GetValue($sm.Ordinal)
                        $bulkCmd.Parameters.Add($param) | Out-Null
                    }
                    $bulkCmd.Prepare()
                    $hasPrepared = $true
                }
                Else { ForEach($sm in $SchemaMap) { $bulkCmd.Parameters[$sm.Ordinal].Value = $DataReader.GetValue($sm.Ordinal) } }

                $batchIteration += 1
                $null = $bulkCmd.ExecuteNonQuery()

                If($sw.Elapsed.TotalSeconds -gt $BatchTimeout) { Throw [TimeoutException]::new(("Batch took longer than {0} seconds to complete." -f $BatchTimeout)) }
                If($batchIteration % $BatchSize -eq 0) {
                    $bulkCmd.Transaction.Commit()
                    $bulkCmd.Transaction.Dispose()
                    If($Notify) { $Notify.Invoke($batchIteration) }
                    $bulkCmd.Transaction = $this.Connection.BeginTransaction()
                    $sw.Restart()
                }
            }
            $bulkCmd.Transaction.Commit()
            $bulkCmd.Transaction.Dispose()
            $bulkCmd.Transaction = $null
        }
        Finally {
            If($bulkCmd.Transaction) { $bulkCmd.Transaction.Dispose() }
            $bulkCmd.Dispose()
            $DataReader.Close()
            $DataReader.Dispose()
        }
        Return $batchIteration
    }

    [SqlMessage] GetMessage() { Return $this.Messages.Dequeue() }
    [Void] ClearMessages() { $this.Messages.Clear() }
    [bool] HasMessages() { Return $this.Messages.Count -gt 0 }
    [bool] HasTransaction() { Return $this.Transaction -ne $null }

    [void] BeginTransaction() {
        If($this.Transaction) { Throw [InvalidOperationException]::new("Cannot BEGIN a transaction when one is already in progress.") }
        $this.Transaction = $this.Connection.BeginTransaction()
    }

    [void] RollbackTransaction() {
        If($this.Transaction) {
            $this.Transaction.Rollback()
            $this.Transaction.Dispose()
            $this.Transaction = $null
        }
        Else { Throw [InvalidOperationException]::new("Cannot ROLLBACK when there is no transaction in progress.") }
    }

    [void] CommitTransaction() {
        If($this.Transaction) {
            $this.Transaction.Commit()
            $this.Transaction.Dispose()
            $this.Transaction = $null
        }
        Else { Throw [InvalidOperationException]::new("Cannot COMMIT when there is no transaction in progress.") }
    }

    [void] AttachCommand([Data.IDbCommand]$Command) {
        $Command.Connection = $this.Connection
        If($this.Transaction) { $Command.Transaction = $this.Transaction }
    }

    static [Data.IDbConnection] CreateConnection([hashtable]$ht) {
        Throw [NotImplementedException]::new("ProviderBase.CreateConnection must be overloaded!")
    }
}

class DataReaderMap
{
    [int]$Ordinal;
    [string]$Name;
    [string]$DataType;

    DataReaderMap([int]$ordinal, [string]$datatype, [string]$colName = $null) {
        if ([string]::IsNullOrWhiteSpace($colName)) {
            $colName = [string]::Format("Column{0}", $ordinal + 1);
        }

        $this.Ordinal = $ordinal;
        $this.Name = $colName;
        $this.DataType = $datatype;
    }
}

class DbDataReaderAdapter {
    [System.Data.Common.DbDataReader]$DataReader;
    [bool]$ProviderTypes;

    [DataReaderMap[]]$MapList = [DataReaderMap[]]@()

    DbDataReaderAdapter([System.Data.Common.DbDataReader]$dataReader, [bool]$providerTypes = $null) {
        if ($null -eq $dataReader) {
            $this.DataReader = $dataReader;
            $this.ProviderTypes = $providerTypes;
        }

        Set-Variable -Name 'this.DataReader' -Option ReadOnly -Visibility Private -Scope Script
        Set-Variable -Name 'this.ProviderTypes' -Option ReadOnly -Visibility Private -Scope Script
    }

    [PSCustomObject[]]ReadObjects() {
        [int]$Ord = 0;
        foreach ($x in $DataReader.GetSchemaTable().Select("", "ColumnOrdinal"))
        {
            $MapList.Add([DataReaderMap]::new($Ord, $x["DataType"].ToString(), $x["ColumnName"].ToString()));
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
}
