using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace FMSoftlab.Datamigration
{
    public static class Extensions
    {
        public static void AddLineWithLabel(this StringBuilder sb, string label, string value)
        {
            if (sb != null)
            {
                if (!string.IsNullOrWhiteSpace(value?.Trim()))
                    sb.AppendLine($"{label}: {value.Trim()}");
            }
        }
        public static void AddLineWithLabel(this StringBuilder sb, string label, decimal? value)
        {
            if (sb != null)
            {
                if (value.HasValue)
                    sb.AppendLine($"{label}: {value}");
            }
        }
        public static void AddLineWithLabel(this StringBuilder sb, string label, DateTime? value)
        {
            if (sb != null)
            {
                if (value.HasValue)
                    sb.AppendLine($"{label}: {value:dd/MM/yyyy}");
            }
        }
        public static string StringOrNull(this StringBuilder sb)
        {
            string res = null;
            if (sb != null)
            {
                string value = sb.ToString()?.Trim();
                if (!string.IsNullOrWhiteSpace(value))
                {
                    res = value;
                }                
            }
            return res;
        }
        private static void LogSqlException<T>(ILogger<T> log, Exception e)
        {
            if (e is SqlException s)
            {
                log.LogError($"{s.Message}, ErrorCode:{s.ErrorCode}, Server:{s.Server}, Procedure:{s.Procedure}, LineNumber:{s.LineNumber}, Number:{s.Number}");
                foreach (SqlError er in s.Errors)
                {
                    log.LogError($"{er.Message}, Server:{er.Server}, Procedure:{er.Procedure}, LineNumber:{er.LineNumber}, Number:{er.Number}");
                }
            }
        }
        private static void LogSqlException(ILogger log, Exception e)
        {
            if (e is SqlException s)
            {
                log.LogError($"{s.Message}, ErrorCode:{s.ErrorCode}, Server:{s.Server}, Procedure:{s.Procedure}, LineNumber:{s.LineNumber}, Number:{s.Number}");
                foreach (SqlError er in s.Errors)
                {
                    log.LogError($"{er.Message}, Server:{er.Server}, Procedure:{er.Procedure}, LineNumber:{er.LineNumber}, Number:{er.Number}");
                }
            }
        }
        public static void LogAllErrors<T>(this ILogger<T> log, Exception e)
        {
            while (e != null)
            {
                log.LogError($"{e.Message}{Environment.NewLine}{e.StackTrace}");
                LogSqlException(log, e);
                e = e.InnerException;
            }
        }

        public static void LogAllErrors(this ILogger log, Exception e)
        {
            while (e != null)
            {
                log.LogError($"{e.Message}{Environment.NewLine}{e.StackTrace}");
                LogSqlException(log, e);
                e = e.InnerException;
            }
        }
    }
}
