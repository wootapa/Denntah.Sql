using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Denntah.Sql.Reflection;

namespace Denntah.Sql
{
    /// <summary>
    /// Internal extension for commands
    /// </summary>
    internal static class CommandExtension
    {
        /// <summary>
        /// Prepares a command and makes sure connection is open
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-command to be executed</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>The created command</returns>
        public static DbCommand Prepare(this DbConnection conn, string sql, DbTransaction transaction = null)
        {
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = transaction;

            if (conn.State == ConnectionState.Closed)
                conn.Open();

            return cmd;
        }

        /// <summary>
        /// Prepares a command and makes sure connection is open
        /// </summary>
        /// <param name="conn">A connection</param>
        /// <param name="sql">SQL-command to be executed</param>
        /// <param name="transaction">Transaction to associate with the command</param>
        /// <returns>The created command</returns>
        public static async Task<DbCommand> PrepareAsync(this DbConnection conn, string sql, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = transaction;

            if (conn.State == ConnectionState.Closed)
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

            return cmd;
        }

        /// <summary>
        /// Add parameter to a command
        /// </summary>
        /// <param name="cmd">Command to add parameter to</param>
        /// <param name="key">Key of the parameter</param>
        /// <param name="value">Value of the parameter</param>
        public static void ApplyParameter(this DbCommand cmd, string key, object value)
        {
            IDbDataParameter parameter = cmd.CreateParameter();
            parameter.ParameterName = key;
            parameter.Value = value;
            cmd.Parameters.Add(parameter);
        }

        /// <summary>
        /// Add parameters to a command
        /// </summary>
        /// <param name="cmd">Command to add parameters to</param>
        /// <param name="args">Object that holds the parameters</param>
        public static void ApplyParameters(this DbCommand cmd, object args = null)
        {
            if (args == null) return;

            var typeDescriber = TypeHandler.Get(args);

            foreach (var property in typeDescriber.Arguments)
            {
                var value = typeDescriber.GetValue(property.Property.Name, args);

                cmd.ApplyParameter(property.Property.Name, value ?? DBNull.Value);
            }
        }

        /// <summary>
        /// Add parameters to an indexed bulk command
        /// </summary>
        /// <param name="cmd">Command to add parameters to</param>
        /// <param name="argsList">List of objects that holds the parameters</param>
        public static void ApplyParameters(this DbCommand cmd, IEnumerable<object> argsList = null)
        {
            if (argsList == null) return;

            var typeDescriber = TypeHandler.Get(argsList.First());

            int i = 0;
            foreach (var args in argsList)
            {
                foreach (var property in typeDescriber.Arguments)
                {
                    var value = typeDescriber.GetValue(property.Property.Name, args);

                    cmd.ApplyParameter(property.Property.Name + i, value ?? DBNull.Value);
                }
                i++;
            }
        }
    }
}
