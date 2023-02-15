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
using System.Transactions;
using System.Data;

namespace FMSoftlab.Datamigration
{
    public static class HelpersExtensions
    {
        public static DateTime? DateInValidRangeOrNull(this DateTime? value)
        {
            DateTime? res = null;
            if (value != null && value.HasValue)
            {
                res = value.GetValueOrDefault() >= DateTime.Parse("1920-01-01") && value.GetValueOrDefault() <= DateTime.Parse("2050-01-01") ? value.GetValueOrDefault() : null;
            }
            return res;
        }

        public static DateTime? AddTime(this DateTime? value, DateTime? time)
        {
            DateTime? res = null;
            if (value != null && value.HasValue)
            {
                res = value.GetValueOrDefault();
                if (time != null && time.HasValue)
                {
                    res = res.Value.Add(time.Value.TimeOfDay);
                }
            }
            return res;
        }
        public static TimeSpan? ConvertToTimeOnly(this int? value)
        {
            TimeSpan? res = null;
            if (value != null && value > 0)
            {
                res = TimeSpan.FromMinutes(value.GetValueOrDefault());
                if (res != null && res >= new TimeSpan(1, 0, 0, 0))
                {
                    res = new TimeSpan(0, 23, 59, 59);
                }
            }
            return res;
        }
    }

    public abstract class DataCopyFromDataSource<TSource, TDest> : CopyToDBBaseTyped<TSource, TDest, int> where TDest : class, new() where TSource : class, new()
    {
        public DataCopyFromDataSource(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string sourceSql, string sourceConnectionString, string destSql, CommandType? commandType, string destConnectionString) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceSql, sourceConnectionString);
            DestInfo = new() { Sql = destSql, Connection = destConnectionString, CommandType = commandType };
        }

        public DataCopyFromDataSource(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string sourceSql, string sourceConnectionString, string destSql, CommandType? commandType, string destConnectionString, string identityInsertTable) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceSql, sourceConnectionString);
            DestInfo = new() { Sql = destSql, Connection = destConnectionString, IdentityInsertTable = identityInsertTable, CommandType = commandType };
        }
        public DataCopyFromDataSource(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, SqlServerDatasourceInfo sourceInfo, SqlServerDatasourceInfo destInfo) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceInfo);
            DestInfo = destInfo;
        }
        public DataCopyFromDataSource(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string destSql, CommandType? commandType, string destConnectionString) : base(settings, tSqlScriptsManager, log)
        {
            DestInfo = new() { Sql = destSql, Connection = destConnectionString, CommandType = commandType };
        }
        public DataCopyFromDataSource(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string destSql, CommandType? commandType, string destConnectionString, string identityInsertTable) : base(settings, tSqlScriptsManager, log)
        {
            DestInfo = new() { Sql = destSql, Connection = destConnectionString, IdentityInsertTable = identityInsertTable, CommandType = commandType };
        }
        public DataCopyFromDataSource(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, SqlServerDatasourceInfo destInfo) : base(settings, tSqlScriptsManager, log)
        {
            DestInfo = destInfo;
        }


    }
}