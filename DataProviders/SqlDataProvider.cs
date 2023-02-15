using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace FMSoftlab.Datamigration
{
    public class SqlDataProvider : ISourceDataProvider
    {
        protected SqlServerDatasourceInfo SourceInfo;
        protected ILogger Log { get; }
        public IEnumerable<T> GetData<T>(object paramsObject) where T : class, new()
        {
            IEnumerable<T> sourceResults;
            using (SqlConnection con = new SqlConnection(SourceInfo.Connection))
            {
                if (paramsObject != null)
                {
                    DynamicParameters dyn = new(paramsObject);
                    sourceResults = con.Query<T>(SourceInfo.Sql, dyn);
                }
                else
                {
                    sourceResults = con.Query<T>(SourceInfo.Sql);
                }
            }
            return sourceResults;
        }
        public IEnumerable<T> GetData<T>(Func<object> paramsObject) where T : class, new()
        {
            object pobj = null;
            if (paramsObject != null)
            {
                pobj = paramsObject();
            }
            return GetData<T>(pobj);
        }
        public SqlDataProvider(ILogger log, string sourceSql, string sourceConnectionString) : base()
        {
            SourceInfo = new() { Sql = sourceSql, Connection = sourceConnectionString };
            Log = log;
        }
        public SqlDataProvider(ILogger log, SqlServerDatasourceInfo sourceInfo) : base()
        {
            SourceInfo = sourceInfo;
            Log = log;
        }
        public void Validate()
        {

        }

        public IEnumerable<T> GetData<T>(object paramsObject, int rowLimit) where T : class, new()
        {
            IEnumerable<T> res;
            if (rowLimit < 0)
            {
                rowLimit = rowLimit * -1;
                res = GetData<T>(paramsObject)?.Reverse()?.Skip(0)?.Take(rowLimit);
            }
            else
                res = GetData<T>(paramsObject)?.Skip(0)?.Take(rowLimit);
            return res;
        }

        public IEnumerable<T> GetData<T>(Func<object> paramsObject, int rowLimit) where T : class, new()
        {
            IEnumerable<T> res;
            if (rowLimit < 0)
            {
                rowLimit = rowLimit * -1;
                res = GetData<T>(paramsObject)?.Reverse()?.Skip(0)?.Take(rowLimit);
            }
            else
                res = GetData<T>(paramsObject)?.Skip(0)?.Take(rowLimit);
            return res;
        }
    }
}