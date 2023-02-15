using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using Microsoft.Extensions.Logging;

namespace FMSoftlab.Datamigration
{
    public class ScriptExecutionSmo : CopyToDBBase
    {
        public string ConnectionString { get; }
        public ScriptExecutionSmo(IApplicationSettings settings, TSqlScriptsManager tSqlScriptsManager, ILogger<ScriptExecutionSmo> log, string connectionString) : base(settings, tSqlScriptsManager, log)
        {
            ConnectionString = connectionString;
        }

        public void Execute(string scriptKey)
        {
            SqlConnectionStringBuilder conbuild = new(ConnectionString);
            ServerConnection con = new ServerConnection(ConnectionString);
            con.ServerInstance = conbuild.DataSource;
            if (!conbuild.IntegratedSecurity)
            {
                con.LoginSecure = false;
                con.Login = conbuild.UserID;
                con.Password = conbuild.Password;
            }
            else
            {
                con.LoginSecure = true;
            }
            Server s = new Server(con);
            s.ConnectionContext.StatementTimeout = AppSettings.RuntimeSettings.CommandTimeout;
            Database db = s.Databases[conbuild.InitialCatalog];
            string sql = SqlScripts.GetVarious(scriptKey);
            db.ExecuteNonQuery(sql, ExecutionTypes.Default);
        }
    }

    public class ScriptExecution : CopyToDBBase
    {
        public string ConnectionString { get; }
        public ScriptExecution(
            ApplicationSettings settings,
            TSqlScriptsManager tSqlScriptsManager,
            ILogger<ScriptExecution> log,
            string destConnectionString) : base(settings, tSqlScriptsManager, log)
        {
            ConnectionString = destConnectionString;
        }

        public void Execute(string scriptKey)
        {
            Execute(scriptKey, null);
        }

        public void Execute(string scriptKey, object paramsObject)
        {
            var sql = SqlScripts.GetVarious(scriptKey);
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                if (paramsObject != null)
                {
                    DynamicParameters dyn = new DynamicParameters(paramsObject);
                    con.Execute(sql, dyn, commandTimeout: AppSettings.RuntimeSettings.CommandTimeout);
                }
                else con.Execute(sql, commandTimeout: AppSettings.RuntimeSettings.CommandTimeout);
            }
        }
    }
}