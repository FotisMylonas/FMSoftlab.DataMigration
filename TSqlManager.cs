using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FMSoftlab.Datamigration
{
    public class TSqlScriptsManager
    {
        public string ScriptFolder = "Scripts";
        private List<string> _fileList = new();
        public TSqlScriptsManager(string scriptFolder)
        {
            ScriptFolder = Path.GetFullPath(scriptFolder);
            LoadFiles();
        }
        public void LoadFiles()
        {
            var files = Directory.EnumerateFiles(ScriptFolder, "*.sql", SearchOption.AllDirectories);
            if (files.Count() > 0)
            {
                _fileList.AddRange(files.Select(s => GetSubPart(s)));

            }
        }

        public void ReportFiles()
        {
            foreach (var x in _fileList)
            {
                Console.WriteLine($"{x}");
            }
        }

        public string GetSubPart(string filename)
        {
            string s = filename.ToLower().Substring(filename.ToLower().IndexOf(ScriptFolder) + ScriptFolder.Length + 2);
            return s;
        }

        public string GetSql(string subfolder, string key)
        {
            string sql = string.Empty;
            //string firstFile = _fileList.Where(w => string.Equals(GetSubPart(w), $@"{subfolder}\{key}.sql", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            string firstFile = _fileList.Where(w => string.Equals(w, $@"{subfolder}\{key}.sql", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            if (string.IsNullOrWhiteSpace(firstFile))
                throw new Exception($@"TSQL file {subfolder}\{key} not found");
            firstFile = Path.Combine(ScriptFolder, firstFile);
            if (File.Exists(firstFile))
            {
                sql = File.ReadAllText(firstFile);
            }
            else throw new Exception($@"TSQL file {subfolder}\{key} not found");
            return sql;
        }

        public string GetVarious(string key)
        {
            return GetSql("Various", key);
        }

        public string GetSelect(string key)
        {
            return GetSql("Select", key);
        }

        public string GetResolver(string key)
        {
            return GetSql("Resolve", key);
        }

        public string GetInsert(string key)
        {
            return GetSql("Insert", key);
        }

        public string GetCommonSourceLookup(string table)
        {
            return @$"select idc, perigrafh from [dbo].[{table}]";
        }
    }

}
