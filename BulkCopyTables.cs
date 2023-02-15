using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FMSoftlab.Datamigration
{
    public class TableInfo
    {
        public string TableSchema
        {
            get;
            set;
        }

        public string TableName
        {
            get;
            set;
        }

        public int RecordCount { get; set; }
    }


    public class BulkCopyTables
    {
        private readonly ILogger<BulkCopyTables> _log;
        private readonly string _sourceConnectionString;
        private readonly string _destConnectionString;
        private int NumberOfParallelTasks { get; set; }
        public BulkCopyTables(int numberOfParallelTasks, string sourceConnectionString, string destConnectionString, ILogger<BulkCopyTables> log)
        {
            NumberOfParallelTasks = numberOfParallelTasks;
            _sourceConnectionString = sourceConnectionString;
            _destConnectionString = destConnectionString;
            _log = log;
        }

        private List<TableInfo> MigrationTables = new List<TableInfo>();

        private IEnumerable<string> GetSchema(string connectionString, string tableowner, string tableName)
        {
            List<string> res = new List<string>();
            using (SqlConnection sqlConnection = new SqlConnection(connectionString))
            {
                using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "sp_Columns";
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    sqlCommand.Parameters.Add("@table_owner", SqlDbType.NVarChar, 384).Value = tableowner;
                    sqlCommand.Parameters.Add("@table_name", SqlDbType.NVarChar, 384).Value = tableName;
                    sqlConnection.Open();
                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                    {
                        while (sqlDataReader.Read())
                        {
                            //_log.LogInformation((string)sqlDataReader["column_name"]);
                            res.Add((string)sqlDataReader["column_name"]);
                        }
                    }
                }
                sqlConnection.Close();
            }
            return res;
        }

        public void AddMigrationTable(string schemaname, string tablename)
        {
            MigrationTables.Add(new TableInfo
            {
                TableSchema = schemaname,
                TableName = tablename
            });
        }

        private IEnumerable<TableInfo> GetTablesByRecordCount(string connectionstring, bool empty)
        {
            string condition = (empty) ? "<=" : ">";
            string sql = string.Format(@"WITH    a AS ( SELECT   SCHEMA_NAME(sOBJ.schema_id) AS schemaname ,
                        sOBJ.name AS[TableName],
                        QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.'
                        + QUOTENAME(sOBJ.name) AS[fullTableName],
                        SUM(sdmvPTNS.row_count) AS[RowCount]
               FROM     sys.objects AS sOBJ
                        INNER JOIN sys.dm_db_partition_stats AS sdmvPTNS
                        ON sOBJ.object_id = sdmvPTNS.object_id
               WHERE    sOBJ.type = 'U'
                        AND sOBJ.is_ms_shipped = 0x0
                        AND sdmvPTNS.index_id < 2
               GROUP BY sOBJ.schema_id,
                        sOBJ.name
             )
    SELECT a.schemaname ,
            a.TableName,
            a.[RowCount]
    FROM    a
    WHERE   [RowCount]{0}0
    ORDER BY schemaname ,
            TableName", condition);

            using (SqlConnection sqlConnection = new SqlConnection(connectionstring))
            {
                using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = sql;
                    sqlCommand.CommandType = CommandType.Text;
                    sqlConnection.Open();
                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                    {
                        while (sqlDataReader.Read())
                        {
                            string schemaname = (string)sqlDataReader["schemaname"];
                            string tableName = (string)sqlDataReader["TableName"];
                            int rowcount = Convert.ToInt32(sqlDataReader.GetInt64(sqlDataReader.GetOrdinal("RowCount")));
                            yield return new TableInfo() { TableSchema = schemaname, TableName = tableName, RecordCount = rowcount };
                        }
                    }
                }
                sqlConnection.Close();
            }
        }

        private void SetupMigration_DestinationTableMustBeEmpty()
        {
            List<TableInfo> sourcetableinfolist = GetTablesByRecordCount(_sourceConnectionString, false).ToList<TableInfo>();
            List<TableInfo> desttableinfolist = GetTablesByRecordCount(_destConnectionString, true).ToList<TableInfo>();
            if (sourcetableinfolist != null && desttableinfolist != null)
            {
                foreach (TableInfo ti in sourcetableinfolist)
                {
                    if (desttableinfolist.Any(p => string.Equals(p.TableSchema, ti.TableSchema, StringComparison.OrdinalIgnoreCase) && string.Equals(p.TableName, ti.TableName, StringComparison.OrdinalIgnoreCase)))
                    {
                        _log.LogInformation("{0}.{1}", ti.TableSchema, ti.TableName);
                        AddMigrationTable(ti.TableSchema, ti.TableName);
                    }
                }
            }
        }
        /*public void Execute()
        {

            SetupMigration_DestinationTableMustBeEmpty();
            List<Task> list = new List<Task>();
            int num = MigrationTables.Count<TableInfo>();
            for (int i = 1; i <= num; i++)
            {
                TableInfo tableInfo = MigrationTables[i - 1];
                string sch = tableInfo.TableSchema;
                string tbname = tableInfo.TableName;
                Task item = new Task(() =>
                {
                    BulkCopy(sch, tbname);
                });
                list.Add(item);
                if (i % NumberOfParallelTasks == 0 || i == num)
                {
                    foreach (Task current in list)
                    {
                        current.Start();
                    }
                    Task.WaitAll(list.ToArray());
                    list.Clear();
                }
            }

        }*/

        public async Task Execute()
        {
            SetupMigration_DestinationTableMustBeEmpty(); 
            int num = MigrationTables.Count<TableInfo>();
            for (int i = 1; i <= num; i++)
            {
                TableInfo tableInfo = MigrationTables[i - 1];
                string sch = tableInfo.TableSchema;
                string tbname = tableInfo.TableName;
                await BulkCopy(sch, tbname); 
            }
        }
        private IEnumerable<string> GetMapping(string stringSource, string stringTarget, string tableowner, string tableName)
        {
            IEnumerable<string> sourceColumns = GetSourceColumns(stringSource, tableowner, tableName);
            IEnumerable<string> schema = GetSchema(stringTarget, tableowner, tableName);
            return sourceColumns.Intersect(schema, StringComparer.OrdinalIgnoreCase);
        }

        private DataTable GetSchemaTable(string sourceConString, string tableowner, string tableName)
        {
            DataTable schemaTable;
            using (SqlConnection sqlConnection = new SqlConnection(sourceConString))
            {
                sqlConnection.Open();
                using (SqlCommand sqlCommand = new SqlCommand(string.Format("SELECT top 1 * FROM [{0}].[{1}]", tableowner, tableName), sqlConnection))
                {

                    SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(CommandBehavior.KeyInfo);
                    schemaTable = sqlDataReader.GetSchemaTable();
                }
                sqlConnection.Close();
            }
            return schemaTable;
        }

        private IEnumerable<string> GetSourceColumns(string sourceConString, string tableowner, string tableName)
        {
            DataTable schemaTable = GetSchemaTable(sourceConString, tableowner, tableName);
            IEnumerable<string> list = from myrow in schemaTable.AsEnumerable()
                                       where myrow.Field<bool>("IsReadOnly") == false |
                                       myrow.Field<bool>("IsIdentity") == true |
                                       myrow.Field<bool>("IsAutoIncrement") == true |
                                       string.Equals(myrow.Field<string>("ColumnName"), "TENANTID", StringComparison.InvariantCultureIgnoreCase)
                                       select myrow.Field<string>("ColumnName");

            _log.LogInformation("table:{0} fields:{1}", tableName, string.Join(",", schemaTable.AsEnumerable().Select(myrow => myrow.Field<string>("ColumnName"))));
            _log.LogInformation("table:{0} fields:{1}", tableName, string.Join(",", list));
            return list;
        }

        private async Task BulkCopy(string tableowner, string tableName)
        {
            bool transferok = false;
            int i = 0;
            while (!transferok && i < 5)
            {
                try
                {
                    await intBulkCopy(tableowner, tableName);
                    transferok = true;
                    _log.LogInformation("Succesfully transfered [{0}].[{1}]", tableowner, tableName);
                }
                catch (Exception e)
                {
                    _log.LogError("Exception! {0}{1}{2}\n{3}", tableowner, tableName, e.Message, e.StackTrace);
                    transferok = false;
                    i++;
                }
            }
        }

        private async Task intBulkCopy(string tableowner, string tableName)
        {
            _log.LogInformation("[{0}].[{1}]", tableowner, tableName);
            using (SqlConnection sqlConnection = new SqlConnection(_sourceConnectionString))
            {
                SqlCommand sqlCommand = new SqlCommand(string.Format("SELECT * FROM [{0}].[{1}]", tableowner, tableName), sqlConnection);
                sqlCommand.CommandTimeout = 0;
                sqlConnection.Open();
                using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                {
                    using (SqlConnection destinnationConnection = new SqlConnection(_destConnectionString))
                    {
                        destinnationConnection.Open();
                        using (SqlTransaction trans = destinnationConnection.BeginTransaction("BulkImportTransaction"))
                        {

                            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(destinnationConnection, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, trans))
                            {
                                sqlBulkCopy.BulkCopyTimeout = 0;
                                sqlBulkCopy.BatchSize = 50000;
                                sqlBulkCopy.NotifyAfter = 50000;
                                sqlBulkCopy.SqlRowsCopied += delegate (object sender, SqlRowsCopiedEventArgs eventArgs)
                                {
                                    _log.LogInformation("[{1}].[{2}], records loaded:{3}", new object[]
                                    {
                                    tableowner,
                                    tableName,
                                    eventArgs.RowsCopied
                                    });
                                };
                                sqlBulkCopy.DestinationTableName = $"[{tableowner}].[{tableName}]";
                                IEnumerable<string> mapping = GetMapping(_sourceConnectionString, _destConnectionString, tableowner, tableName);
                                if (mapping.Count<string>() > 0)
                                {
                                    foreach (string current in mapping)
                                    {
                                        var map = sqlBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(current, current));
                                    }
                                }
                                try
                                {
                                    await sqlBulkCopy.WriteToServerAsync(sqlDataReader);
                                    trans.Commit();
                                }
                                catch (Exception e)
                                {
                                    trans.Rollback();
                                    throw e;
                                }
                            }
                        }
                        destinnationConnection.Close();
                    }
                }
                sqlConnection.Close();
            }
        }
    }
}
