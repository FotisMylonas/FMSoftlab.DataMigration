using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Options;
using System.Linq;

namespace FMSoftlab.Datamigration
{

    public class RolloutEnvironment
    {
        public string MachineName { get; set; }
        public string EnvironmentName { get; set; }
    }

    public static class ApplicationSettingsExtensions
    {
        public static List<RolloutEnvironment> GetEnvironments(this IConfiguration configuration)
        {
            var res = configuration.GetSection("RolloutEnvironments").Get<RolloutEnvironment[]>().ToList();
            return res;
        }

        public static List<RolloutEnvironment> GetEnvironments(this IConfigurationRoot configuration)
        {
            var res = configuration.GetSection("RolloutEnvironments").Get<RolloutEnvironment[]>().ToList();
            return res;
        }
    }
    public interface IApplicationSettings
    {
        public Dictionary<string, string> ConnectionStrings { get; }
        public RuntimeSettings RuntimeSettings { get; }
    }
    public class RuntimeSettings
    {
        public int TenantId { get; set; }
        public int CurrentUserId { get; set; }
        //public string ScriptFolder { get; set; }
        public string InputFolder { get; set; }
        public string ExportFolder { get; set; }
        public string SqlScriptsFolder { get; set; }
        public int CommandTimeout { get; set; }
    }

    public class ApplicationSettings : IApplicationSettings
    {
        public Dictionary<string, string> ConnectionStrings { get; set; }
        public RuntimeSettings RuntimeSettings { get; }
        public ApplicationSettings(IConfiguration configuration)
        {
            var dic = configuration.GetSection("ConnectionStrings").Get<Dictionary<string, string>>();
            var comparer = StringComparer.OrdinalIgnoreCase;
            this.ConnectionStrings = new Dictionary<string, string>(dic, comparer);
            this.RuntimeSettings = new RuntimeSettings();
            configuration.GetSection("RuntimeSettings").Bind(this.RuntimeSettings);
        }
        /*public void InitConnectionStrings(IConfiguration configuration)
        {
            ConnectionStrings = configuration.GetSection("ConnectionStrings").Get<Dictionary<string, string>>();
        }*/
    }
}