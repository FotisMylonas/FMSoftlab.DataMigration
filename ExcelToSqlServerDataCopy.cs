using Dapper;
using Ganss.Excel;
using Mapster;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Transactions;

namespace FMSoftlab.Datamigration
{

    public abstract class ExcelToSqlServerDataCopy<TSource, TDest, TId> : CopyToDBBaseTyped<TSource, TDest, TId> where TDest : class, new() where TSource : class, new()
    {
        public ExcelToSqlServerDataCopy(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string filename, string sheet, string destSql, string destConnectionString) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new ExcelDataProvider(filename, sheet, log);
            DestInfo = new() { Sql = destSql, Connection = destConnectionString };
        }
        public ExcelToSqlServerDataCopy(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string filename, string sheet, string destConnectionString, bool identityInsert) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new ExcelDataProvider(filename, sheet, log);
            string identityInsertTable = string.Empty;
            if (identityInsert)
            {
                identityInsertTable = SqlGenerator.GetTablename(typeof(TDest));
            }
            DestInfo = new() { Sql = SqlGenerator.GetInsertSqlWithParams(typeof(TDest)), Connection = destConnectionString, IdentityInsertTable = identityInsertTable };
        }     
    }
}
