using FileHelpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace FMSoftlab.Datamigration
{
    public class FixedLengthDataProvider : ISourceDataProvider
    {
        protected DatasourceInfo SourceInfo;
        public IEnumerable<T> GetData<T>(Func<object> paramsObject) where T : class, new()
        {
            var engine = new FixedFileEngine<T>();
            IEnumerable<T> results = engine.ReadFile(SourceInfo.Connection);
            return results;
        }
        public void Validate()
        {
            if (!File.Exists(SourceInfo.Connection))
                throw new FileNotFoundException(SourceInfo.Connection);
        }

        public IEnumerable<T> GetData<T>(object paramsObject) where T : class, new()
        {
            List<T> res = new List<T>();
            if (File.Exists(SourceInfo.Connection))
            {
                var engine = new FixedFileEngine<T>();
                IEnumerable<T> results = engine.ReadFile(SourceInfo.Connection);
                if (results?.Any() ?? false)
                {
                    res.AddRange(results);
                }
            }
            return res;
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

        public FixedLengthDataProvider(string filename)
        {
            SourceInfo = new DatasourceInfo { Connection = filename };
        }
        public FixedLengthDataProvider(DatasourceInfo sourceInfo)
        {
            SourceInfo = sourceInfo;
        }
    }
}
