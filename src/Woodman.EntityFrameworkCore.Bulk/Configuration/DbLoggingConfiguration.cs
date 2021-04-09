using Microsoft.Extensions.Logging;

namespace Woodman.EntityFrameworkCore.Bulk
{
    public static class DbLoggingConfiguration
    {
        private static LoggerFactory _loggerFactory  = null;

        public static LoggerFactory LoggerFactor
        {
            get
            {
                if(_loggerFactory == null)
                {
                    _loggerFactory = new LoggerFactory();
                }

                return _loggerFactory;
            }
            set { _loggerFactory = value; }
        }
    }
}
