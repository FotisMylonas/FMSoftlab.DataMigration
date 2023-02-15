using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace FMSoftlab.Datamigration
{
    public class FixedLengthToSqlServerDataCopy<TSource, TDest, TId> : CopyToDBBaseTyped<TSource, TDest, TId> where TDest : class, new() where TSource : class, new()
    {
        public FixedLengthToSqlServerDataCopy(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string filename, string destConnectionString) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new FixedLengthDataProvider(filename);
            DestInfo = new() { Sql = SqlGenerator.GetInsertSqlWithParams(typeof(TDest)), Connection = destConnectionString };
        }
    }
}
