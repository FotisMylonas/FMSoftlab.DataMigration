using Dapper;
using Mapster;
using Ganss.Excel;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.Transactions;
using System.Data;

namespace FMSoftlab.Datamigration
{
    public class TransformArgs
    {
        public string RejectReason { get; set; }
        public bool Reject { get; set; }
        public TransformArgs()
        {
            Reject = false;
        }
    }
    public interface ISourceDataProvider
    {
        IEnumerable<T> GetData<T>(object paramsObject) where T : class, new();
        IEnumerable<T> GetData<T>(Func<object> paramsObject) where T : class, new();
        IEnumerable<T> GetData<T>(object paramsObject, int rowLimit) where T : class, new();
        IEnumerable<T> GetData<T>(Func<object> paramsObject, int rowLimit) where T : class, new();
        void Validate();
    }

    public class CopyToDBBase
    {
        public int ErrorsOccured { get; set; }
        public int Rejections { get; set; }
        public int RowsInserted { get; set; }
        protected ILogger Log { get; }
        protected ISourceDataProvider SourceData;
        protected SqlServerDatasourceInfo DestInfo;
        protected IApplicationSettings AppSettings;
        protected TSqlScriptsManager SqlScripts { get; }
        public CopyToDBBase(IApplicationSettings applicationSettings, TSqlScriptsManager tSqlScriptsManager, ILogger log)
        {
            Log = log;
            SqlScripts = tSqlScriptsManager;
            AppSettings = applicationSettings;
            ErrorsOccured = 0;
            RowsInserted = 0;
            Rejections = 0;
        }

        protected void Report()
        {
            Log?.LogInformation($"{this.GetType().Name} rows inserted:{RowsInserted}");
        }

        protected void InitCopy()
        {
            ErrorsOccured = 0;
            RowsInserted = 0;
            Rejections = 0;
        }
    }


    public static class TransformationFunction
    {
        public static TDest Transform<TSource, TDest>(this TSource source, Func<TSource, TDest> transformation)
        {
            return transformation(source);
        }
        public static IEnumerable<TDest> Transform<TSource, TDest>(this IEnumerable<TSource> source, Func<TSource, TDest> transformation)
        {
            return source.Select(transformation);
        }
        public static IEnumerable<TDest> Transform<TSource, TDest>(this IEnumerable<TSource> source, Func<IEnumerable<TSource>, TDest> transformation)
        {
            return new[] { transformation(source) };
        }

        public static IEnumerable<TDest> Reduce<TSource, TDest>(this IEnumerable<TSource> source, Func<IEnumerable<TSource>, IEnumerable<TDest>> reduction)
        {
            return reduction(source);
        }
    }


    public class DataRetrieverBase<TSource> : CopyToDBBase where TSource : class, new()
    {
        public DataRetrieverBase(IApplicationSettings applicationSettings, TSqlScriptsManager tSqlScriptsManager, ILogger log) : base(applicationSettings, tSqlScriptsManager, log)
        {

        }

        public async Task<IEnumerable<TSource>> GetData(object paramsObject, int rowLimit = 0)
        {
            Log.LogDebug("GetData in");
            InitCopy();
            IEnumerable<TSource> sourceResults = Enumerable.Empty<TSource>();
            if (rowLimit == 0)
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject);
            }
            else
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject, rowLimit);
            }
            await Task.CompletedTask;
            Log.LogDebug("GetData out");
            return sourceResults;
        }

        public async Task<IEnumerable<TSource>> CopyData(Func<object> paramsObject, int rowLimit = 0)
        {
            Log.LogDebug("GetData in");
            InitCopy();
            IEnumerable<TSource> sourceResults = Enumerable.Empty<TSource>();
            if (rowLimit == 0)
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject);
            }
            else
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject, rowLimit);
            }
            await Task.CompletedTask;
            Log.LogDebug("GetData out");
            return sourceResults;
        }
    }

    public class DataRetriever<TSource> : DataRetrieverBase<TSource> where TSource : class, new()
    {
        public DataRetriever(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, string sourceSql, string sourceConnectionString) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceSql, sourceConnectionString);
        }
        public DataRetriever(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger log, SqlServerDatasourceInfo sourceInfo) : base(settings, tSqlScriptsManager, log)
        {
            SourceData = new SqlDataProvider(log, sourceInfo);
        }
    }

    public class CopyToDBBaseTyped<TSource, TDest, TId> : CopyToDBBase where TDest : class, new() where TSource : class, new()
    {
        public CopyToDBBaseTyped(IApplicationSettings applicationSettings, TSqlScriptsManager tSqlScriptsManager, ILogger log) : base(applicationSettings, tSqlScriptsManager, log)
        {

        }

        public void Adapt(TSource source, TDest dest)
        {
            try
            {
                source.Adapt(dest);
            }
            catch (Exception ex)
            {
                Log?.LogWarning("Adapt error:" + ex.Message);
                Log?.LogAllErrors(ex);
            }
        }
        public async virtual Task Transform(TSource source, TDest dest, TransformArgs args)
        {
            await Task.CompletedTask;
        }
        public async virtual Task IdGenerated(TId id, TSource source, TDest dest)
        {
            await Task.CompletedTask;
        }

        public async Task CopyData(int rowLimit = 0)
        {
            InitCopy();
            TSource source = default(TSource);
            Action<TDest> forEveryDest = null;
            await CopyData(source, forEveryDest, rowLimit: rowLimit);
            Report();
        }
        public async Task CopyData(TSource source)
        {
            await CopySingleDataRow(source, null);
        }
        public async Task CopyData(IEnumerable<TSource> source)
        {
            await CopyData(source, null);
        }

        public async Task CopyData(IEnumerable<TSource> source, Action<TDest> forEveryDest)
        {
            InitCopy();
            foreach (var sd in source)
            {
                await CopyDataRow(sd, forEveryDest);
            }
            Report();
        }

        public async Task CopySingleDataRow(TSource source, Action<TDest> forEveryDest)
        {
            InitCopy();
            await CopyDataRow(source, forEveryDest);
            Report();
        }
        public async Task CopySingleDataRow(TSource source)
        {
            await CopySingleDataRow(source, null);
        }
        public async Task CopyData(object paramsObject, Action<TDest> forEveryDest, int rowLimit = 0)
        {
            InitCopy();
            IEnumerable<TSource> sourceResults;
            if (rowLimit == 0)
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject);
            }
            else
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject, rowLimit);
            }
            if (sourceResults != null)
            {
                foreach (TSource sourceDataRow in sourceResults)
                {
                    await CopyDataRow(sourceDataRow, forEveryDest);
                }
            }
            Report();
        }

        public async Task CopyData(Func<object> paramsObject, Action<TDest> forEveryDest, int rowLimit = 0)
        {
            InitCopy();
            IEnumerable<TSource> sourceResults;
            if (rowLimit == 0)
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject);
            }
            else
            {
                sourceResults = SourceData.GetData<TSource>(paramsObject, rowLimit);
            }
            if (sourceResults != null)
            {
                foreach (TSource sourceDataRow in sourceResults)
                {
                    await CopyDataRow(sourceDataRow, forEveryDest);
                }
            }
            Report();
        }

        private async Task CopyDataRow(TSource source, Action<TDest> forEveryDest)
        {
            if (source == null)
                return;
            string idinsert = DestInfo.GetIdentityInsert();
            TDest dest = new();
            TransformArgs args = new();
            string jsonString = String.Empty;
            try
            {
                Adapt(source, dest);
                await Transform(source, dest, args);
                jsonString = JsonSerializer.Serialize(source);
                if (args.Reject)
                {
                    Rejections++;
                    Log?.LogWarning($"rejected, reason:{args.RejectReason}, rejections:{Rejections}, {jsonString}");
                    return;
                }
                if (forEveryDest != null)
                {
                    forEveryDest(dest);
                }
                StringBuilder sql = new();
                if (!string.IsNullOrWhiteSpace(idinsert))
                {
                    sql.AppendLine(idinsert);
                }
                sql.Append(DestInfo.Sql);
                DynamicParameters dyn = new DynamicParameters(dest);
                string sqlText = sql.ToString();
                //Log?.LogInformation($"migrating object {jsonString}");
                /* using (TransactionScope scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                 {*/
                using (SqlConnection destCon = new SqlConnection(DestInfo.Connection))
                {
                    CommandType commandType = CommandType.Text;
                    if (DestInfo.CommandType.HasValue)
                    {
                        commandType = DestInfo.CommandType.GetValueOrDefault();
                    }
                    TId id = await destCon.ExecuteScalarAsync<TId>(sqlText, dyn, commandType: commandType);
                    await IdGenerated(id, source, dest);
                    RowsInserted++;
                    Log?.LogInformation($"id {id}, migrated objects: {RowsInserted}, ErrorsOccured:{ErrorsOccured}, Rejections:{Rejections}");
                }
                //scope.Complete();
                // }
            }
            catch (Exception ex)
            {
                ErrorsOccured++;
                string name = this.GetType().Name;
                Log?.LogError($"{name}, Error migrating:{jsonString}");
                Log?.LogAllErrors(ex);
            }
        }
    }
}