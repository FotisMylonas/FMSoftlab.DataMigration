using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace FMSoftlab.Datamigration
{
    public class DelimitedToSqlServerDataCopy<TSource, TDest, TId> : CopyToDBBaseTyped<TSource, TDest, TId> where TDest : class, new() where TSource : class, new()
    {
        public DelimitedToSqlServerDataCopy(IApplicationSettings applicationSettings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string filename, string destConnectionString) : base(applicationSettings, tSqlScriptsManager, log)
        {
            SourceData = new DelimitedDataProvider(filename);
            DestInfo = new() { Sql = SqlGenerator.GetInsertSqlWithParams(typeof(TDest)), Connection = destConnectionString };
        }
    }
}
