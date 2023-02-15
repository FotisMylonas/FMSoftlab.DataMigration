using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FMSoftlab.Datamigration
{
    public class DataAccess
    {
        protected readonly int? _commandTimeout = 0;
        protected readonly string _connectionString;
        protected readonly ILogger _log;
        public DataAccess(string connectionString, int? commandTimeout, ILogger log)
        {
            _connectionString = connectionString;
            _commandTimeout = commandTimeout;
            _log = log;
        }

        protected SqlConnection GetSqlConnection()
        {
            return new SqlConnection(_connectionString);
        }

        protected async Task ExecSql(string sql, DynamicParameters dyn, CommandType ctype)
        {
            using (var conn = GetSqlConnection())
            {
                conn.Open();
                await conn.ExecuteAsync(sql, param: dyn, commandType: ctype, commandTimeout: _commandTimeout);
                conn.Close();
            }
        }
        protected async Task ExecProcedureNoParams(string procname)
        {
            using (var conn = GetSqlConnection())
            {
                conn.Open();
                await conn.ExecuteAsync(procname, param: null, commandType: CommandType.StoredProcedure, commandTimeout: _commandTimeout);
                conn.Close();
            }
        }

        protected void LogSqlError()
        {

        }
        protected int GetCommandTimeout(int? commandTimeout)
        {
            int res = 0;
            if (commandTimeout > 0)
            {
                res = commandTimeout.Value;
            }
            else
            {
                if (_commandTimeout > 0)
                {
                    res = _commandTimeout.Value;
                }
                else
                {
                    res = 30;
                }
            }
            return res;
        }

        protected async Task<bool> SafeExecSql(string sql, DynamicParameters dyn, CommandType ctype)
        {
            bool res = false;
            try
            {
                using (var conn = GetSqlConnection())
                {
                    conn.Open();
                    using (var trans = conn.BeginTransaction())
                    {
                        await conn.ExecuteAsync(sql, param: dyn, commandType: ctype, transaction: trans, commandTimeout: _commandTimeout);
                        trans.Commit();
                        conn.Close();
                        res = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Exception e = ex;
                while (e != null)
                {
                    _log.LogError(ex.Message);
                    if (e is SqlException sqle)
                    {
                        _log.LogError($"Procedure:{sqle.Procedure}, Message:{sqle.Message}, ErrorCode:{sqle.ErrorCode}, Number:{sqle.Number}, LineNumber:{sqle.LineNumber}, State:{sqle.State}, SqlState:{""}, Source:{sqle.Source}");
                        //_log.LogError(sqle.BatchCommand?.CommandText);
                        foreach (SqlError er in sqle.Errors)
                        {
                            _log.LogError($"Procedure:{er.Procedure}, Message:{er.Message}, Number:{er.Number}, LineNumber:{er.LineNumber}, class:{er.Class}, State:{er.State}, Source:{er.Source}");
                        }
                    }
                    e = e.InnerException;
                }
            }
            return res;
        }
        protected async Task<T> ExecSql<T>(string sql, DynamicParameters dyn, CommandType ctype)
        {
            T res = default(T);
            using (var conn = GetSqlConnection())
            {
                conn.Open();
                res = await conn.ExecuteScalarAsync<T>(sql, param: dyn, commandType: ctype, commandTimeout: _commandTimeout);
                conn.Close();
            }
            return res;
        }
        protected async Task<IEnumerable<T>> ExecSqlGetRows<T>(string sql, DynamicParameters dyn, CommandType ctype)
        {
            List<T> res = new List<T>();
            using (var conn = GetSqlConnection())
            {
                conn.Open();
                var tempres = await conn.QueryAsync<T>(sql, param: dyn, commandType: ctype, commandTimeout: _commandTimeout);
                if (tempres?.Count() > 0)
                {
                    res.AddRange(tempres);
                }
                conn.Close();
            }
            return res;
        }

        protected async Task<IEnumerable<T>> ExecSqlGetRowsSP<T>(string sql)
        {
            List<T> res = new List<T>();
            using (var conn = GetSqlConnection())
            {
                conn.Open();
                var tempres = await conn.QueryAsync<T>(sql, param: null, commandType: CommandType.StoredProcedure, commandTimeout: _commandTimeout);
                if (tempres?.Count() > 0)
                {
                    res.AddRange(tempres);
                }
                conn.Close();
            }
            return res;
        }

        protected async Task<T> ExecSqlGetScalarResultSP<T>(string sql)
        {
            T res = default(T);
            using (var conn = GetSqlConnection())
            {
                conn.Open();
                res = await conn.ExecuteScalarAsync<T>(sql, param: null, commandType: CommandType.StoredProcedure, commandTimeout: _commandTimeout);
                conn.Close();
            }
            return res;
        }
    }
}