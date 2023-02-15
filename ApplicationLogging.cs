using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FMSoftlab.Datamigration
{
    public static class ApplicationLogging
    {
        private static ILoggerFactory _mLoggerFactory = new LoggerFactory();
        private static ILoggerFactory LoggerFactory { get { return _mLoggerFactory; } }
        public static void InitLoggerFactory(ILoggerFactory loggerFactory)
        {
            _mLoggerFactory = loggerFactory;
        }
        public static ILogger<T> CreateLogger<T>() =>
          LoggerFactory.CreateLogger<T>();
    }
}