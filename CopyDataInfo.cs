using System;
using System.Data;

namespace FMSoftlab.Datamigration
{
    public class DatasourceInfo
    {
        public string Connection { get; set; }
    }
    public class SqlServerDatasourceInfo : DatasourceInfo
    {
        public string Sql { get; set; }
        public CommandType? CommandType { get; set; }
        public string IdentityInsertTable { get; set; }
        public string GetIdentityInsert()
        {
            string res = "";
            if (!string.IsNullOrWhiteSpace(IdentityInsertTable))
            {
                res = @$"SET IDENTITY_INSERT {IdentityInsertTable} ON";
            }
            return res;
        }
    }

    public class ExcelDatasourceInfo : DatasourceInfo
    {
        public string Sheet { get; set; }
    }


    /*public abstract class DataCopyValuesFeeder<TDest> where TDest : new()
     {
         protected CopyDataInfo DestInfo;
         protected Action<TDest> ValuesFeeder { get; }


         public DataCopyValuesFeeder(string destSql, string destConnectionString, Action<TDest> valuesFeeder)
         {
             DestInfo = new() { Sql = destSql, Connection = destConnectionString };
             ValuesFeeder = valuesFeeder;
         }
         public DataCopyValuesFeeder(string destSql, string destConnectionString, Action<TDest> valuesFeeder, string identityInsertTable)
         {
             DestInfo = new() { Sql = destSql, Connection = destConnectionString, IdentityInsertTable = identityInsertTable };
             ValuesFeeder = valuesFeeder;
         }

         public DataCopyValuesFeeder(CopyDataInfo destInfo)
         {
             DestInfo = destInfo;
         }

         public void CopyData()
         {
             using (SqlConnection destCon = new SqlConnection(DestInfo.Connection))
             {
                 string idinsert = DestInfo.GetIdentityInsert();
                 TDest dest = new();
                 ValuesFeeder(dest);
                 StringBuilder sql = new();
                 if (!string.IsNullOrWhiteSpace(idinsert))
                 {
                     sql.AppendLine(idinsert);
                 }
                 sql.AppendLine(DestInfo.Sql);
                 destCon.Execute(sql.ToString(), dest);
             }
         }
     }*/






}
