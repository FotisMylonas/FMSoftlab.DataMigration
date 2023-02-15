using Ganss.Excel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace FMSoftlab.Datamigration
{
    public class ExcelDataProvider : ISourceDataProvider
    {
        protected ExcelDatasourceInfo SourceInfo;
        public IEnumerable<T> GetData<T>(Func<object> paramsObject) where T : class, new()
        {
            IEnumerable<T> results = new ExcelMapper().Fetch<T>(SourceInfo.Connection, SourceInfo.Sheet);
            return results;
        }
        public IEnumerable<T> GetData<T>(object paramsObject) where T : class, new()
        {
            List<T> res = new List<T>();
            if (File.Exists(SourceInfo.Connection))
            {
                IEnumerable<T> results = new ExcelMapper().Fetch<T>(SourceInfo.Connection, SourceInfo.Sheet);
                if (results?.Any() ?? false)
                {
                    res.AddRange(results);
                }
            }
            return res;
        }
        public ExcelDataProvider(string filename, string sheet)
        {
            SourceInfo = new ExcelDatasourceInfo { Connection = filename, Sheet = sheet };
        }
        public ExcelDataProvider(ExcelDatasourceInfo sourceInfo)
        {
            SourceInfo = sourceInfo;
        }
        public void Validate()
        {
            if (!File.Exists(SourceInfo.Connection))
                throw new FileNotFoundException(SourceInfo.Connection);
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
