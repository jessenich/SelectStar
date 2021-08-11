using namespace System;
using namespace System.Collections.Generic;
using namespace System.Data;
using namespace System.Management.Automation;
using namespace System.Data.Common;

class DataReaderMap
{
    [int]$Ordinal;
    [string]$Name;
    [string]$DataType;

    DataReaderMap([int]$ordinal, [string]$datatype, [string]$name = $null) {
        if (Test-StringIsNullOrWhiteSpace $name) {
            $name = [string]::Format("Column{0}", $ordinal + 1);
        }

        $this.Ordinal = ordinal;
        $this.Name = name;
        $this.DataType = datatype;
    }
}
