using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FMSoftlab.Datamigration
{
    [System.AttributeUsage(AttributeTargets.Class |
                           AttributeTargets.Struct | AttributeTargets.Field | AttributeTargets.Property)
    ]
    public class SqlNameAttribute : Attribute
    {
        public string Name;
        public string Alias;

        public SqlNameAttribute(string name)
        {
            Name = name;
        }
        public SqlNameAttribute(string name, string alias)
        {
            Name = name;
            Alias = alias;
        }
    }

    public static class SqlGenerator
    {
        public static string GetTablename(Type type)
        {
            string tablename = string.Empty;
            PropertyInfo[] pinfo = type.GetProperties();
            if (pinfo.Length <= 0)
                return tablename;
            tablename = type.Name;
            foreach (Attribute a in type.GetCustomAttributes(false))
            {
                if (a is SqlNameAttribute)
                {
                    SqlNameAttribute dba = (SqlNameAttribute)a;
                    if (!string.IsNullOrWhiteSpace(dba.Name))
                    {
                        tablename = dba.Name;
                    }
                }
            }
            return tablename;
        }
        public static string GetInsertSqlWithParams(Type type)
        {
            string sql = string.Empty;
            PropertyInfo[] pinfo = type.GetProperties();
            if (pinfo.Length <= 0)
                return sql;
            List<string> fieldnames = new();
            List<string> valuesparams = new();
            string tablename = type.Name;
            foreach (Attribute a in type.GetCustomAttributes(false))
            {
                if (a is SqlNameAttribute)
                {
                    SqlNameAttribute dba = (SqlNameAttribute)a;
                    if (!string.IsNullOrWhiteSpace(dba.Name))
                    {
                        tablename = dba.Name;
                    }
                }
            }
            foreach (var prop in pinfo)
            {
                string fieldname = prop.Name;
                foreach (Attribute a in prop.GetCustomAttributes(false))
                {
                    if (a is SqlNameAttribute)
                    {
                        SqlNameAttribute dba = (SqlNameAttribute)a;
                        if (!string.IsNullOrWhiteSpace(dba.Name))
                        {
                            fieldname = dba.Name;
                        }
                    }
                }
                fieldnames.Add(fieldname);
                valuesparams.Add($"@{fieldname}");
            }
            sql = $"insert into {tablename} ({string.Join(',', fieldnames)}) values ({string.Join(',', valuesparams)})";
            return sql;
        }

        public static string GetSelectSql(this Type type)
        {
            string sql = string.Empty;

            PropertyInfo[] pinfo = type.GetProperties();
            if (pinfo.Length <= 0)
                return sql;

            List<string> fieldnames = new();
            string tablealias = string.Empty;
            string tablename = type.Name;
            foreach (Attribute a in type.GetCustomAttributes(false))
            {
                if (a is SqlNameAttribute)
                {
                    SqlNameAttribute dba = (SqlNameAttribute)a;
                    if (!string.IsNullOrWhiteSpace(dba.Name))
                    {
                        tablename = dba.Name;
                    }
                    if (!string.IsNullOrWhiteSpace(dba.Alias))
                    {
                        tablealias = dba.Alias;
                        tablename = $"{tablename} AS {tablealias}";
                    }
                }
            }
            foreach (var prop in pinfo)
            {
                string fieldname = prop.Name;
                foreach (Attribute a in prop.GetCustomAttributes(false))
                {
                    if (a is SqlNameAttribute)
                    {
                        SqlNameAttribute dba = (SqlNameAttribute)a;
                        if (!string.IsNullOrWhiteSpace(dba.Name))
                        {
                            fieldname = dba.Name;
                        }
                        if (!string.IsNullOrWhiteSpace(dba.Alias))
                        {
                            fieldname = $"{fieldname} AS {dba.Alias}";
                        }
                        if (!string.IsNullOrWhiteSpace(tablealias))
                        {
                            fieldname = $"{tablealias}.{fieldname}";
                        }
                    }
                }
                fieldnames.Add(fieldname);
            }
            sql = $"SELECT {string.Join(',', fieldnames)} FROM {tablename}";
            return sql;
        }
    }
}