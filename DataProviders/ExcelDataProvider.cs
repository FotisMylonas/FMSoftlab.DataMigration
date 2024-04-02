using Ganss.Excel;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.Management.Sdk.Differencing.SPI;
using NPOI.SS.Formula.Functions;
using NPOI.XSSF.Streaming.Values;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace FMSoftlab.Datamigration
{


    // Custom attribute to specify date format for string properties
    [AttributeUsage(AttributeTargets.Property)]
    public class DateFormatAttribute : Attribute
    {
        public string Format { get; }

        public DateFormatAttribute(string format)
        {
            Format = format;
        }
    }

    public static class ExcelMapperDateConverter
    {
        public static void SetupExcelMapper(this ExcelMapper mapper, Type type)
        {
            PropertyInfo[] properties = type.GetProperties();

            foreach (var property in properties)
            {
                if (property.PropertyType == typeof(DateTime)
                    || property.PropertyType == typeof(DateTime?)
                    )
                {
                    var dateFormatAttribute = property.GetCustomAttribute<DateFormatAttribute>();
                    if (dateFormatAttribute != null)
                    {
                        string format = dateFormatAttribute.Format;
                        IFormatProvider provider = CultureInfo.InvariantCulture;
                        ColumnInfo cinfo = mapper.AddMapping(type, property.Name, property.Name);
                        cinfo.SetPropertyUsing(value =>
                        {
                            object res = null;
                            if (value!=null)
                            {
                                if (value is string stringValue)
                                {
                                    if (DateTime.TryParseExact(stringValue, format, provider, DateTimeStyles.None, out DateTime result))
                                    {
                                        res=Convert.ChangeType(result, typeof(DateTime), provider);
                                    }
                                }
                                else
                                {
                                    res=Convert.ChangeType(value, typeof(DateTime), CultureInfo.InvariantCulture);
                                }
                            }
                            return res;
                        });
                    }
                }
            }
        }
    }


    public class ExcelDataProvider : ISourceDataProvider
    {
        private readonly ILogger _log;
        private readonly ExcelMapper _excelMapper;
        protected ExcelDatasourceInfo SourceInfo;

        public IEnumerable<T> GetData<T>(Func<object> paramsObject) where T : class, new()
        {
            IEnumerable<T> res = Enumerable.Empty<T>();
            try
            {
                _excelMapper.SetupExcelMapper(typeof(T));
                IEnumerable<T> results = _excelMapper.Fetch<T>(SourceInfo.Connection, SourceInfo.Sheet);
                if (results?.Any() ??false)
                {
                    res=results;
                }
            }
            catch (Exception ex)
            {
                _log.LogError($"{ex.Message}{Environment.NewLine}{ex.StackTrace}");
                throw;
            }
            return res;
        }
        public IEnumerable<T> GetData<T>(object paramsObject) where T : class, new()
        {
            IEnumerable<T> res = Enumerable.Empty<T>();
            if (!File.Exists(SourceInfo.Connection))
            {
                return res;
            }
            try
            {
                _excelMapper.SetupExcelMapper(typeof(T));
                IEnumerable<T> results = _excelMapper.Fetch<T>(SourceInfo.Connection, SourceInfo.Sheet);
                if (results?.Any() ?? false)
                {
                    res=results;
                }
            }
            catch (Exception ex)
            {
                _log.LogError($"{ex.Message}{Environment.NewLine}{ex.StackTrace}");
                throw;
            }
            return res;
        }

        /*private void sss<T>()
        {
            _excelMapper.AddMapping<T>("CreatedDate", p => p.CreatedDate)
                .SetPropertyUsing(v => DateTime.ParseExact((string)v, "dd/MM/yyyy", null));

        }*/

        public ExcelDataProvider(string filename, string sheet, ILogger log)
        {
            _log=log;
            SourceInfo = new ExcelDatasourceInfo { Connection = filename, Sheet = sheet };
            _excelMapper = new ExcelMapper();
        }
        public ExcelDataProvider(ExcelDatasourceInfo sourceInfo, ILogger log)
        {
            _log=log;
            SourceInfo = sourceInfo;
            _excelMapper=new ExcelMapper();
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
