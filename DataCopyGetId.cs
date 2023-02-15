using Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Mapster;

namespace FMSoftlab.Datamigration
{

    public abstract class DataCopyGetId<TSource, TDest, TId> : CopyToDBBaseTyped<TSource, TDest, TId> where TDest : class, new() where TSource : class, new()
    {
        public DataCopyGetId(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string sourceSql, string sourceConnectionString, string destSql, string destConnectionString) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceSql, sourceConnectionString);
            DestInfo = new() { Sql = destSql, Connection = destConnectionString };
        }

        public DataCopyGetId(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string sourceSql, string sourceConnectionString, string destSql, string destConnectionString, string identityInsertTable) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceSql, sourceConnectionString);
            DestInfo = new() { Sql = destSql, Connection = destConnectionString, IdentityInsertTable = identityInsertTable };
        }

        public DataCopyGetId(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, SqlServerDatasourceInfo sourceInfo, SqlServerDatasourceInfo destInfo) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceInfo);
            DestInfo = destInfo;
        }
    }
}