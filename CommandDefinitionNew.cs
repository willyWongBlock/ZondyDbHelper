using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;

namespace ZondyDBHelper
{
    public struct CommandDefinitionNew
    {
        /// <summary>
        /// The command (sql or a stored-procedure name) to execute
        /// 要执行的命令（SQL或存储过程名称）
        /// </summary>
        public string CommandText { get; set; }

        /// <summary>
        /// The parameters associated with the command
        /// 与命令关联的参数
        /// </summary>
        public object Parameters { get; set; }

        /// <summary>
        /// The active transaction for the command
        /// 命令的活动事务
        /// </summary>
        public IDbTransaction Transaction { get; set; }

        /// <summary>
        /// The effective timeout for the command
        /// 命令的有效超时时间
        /// </summary>
        public int? CommandTimeout { get; set; }

        /// <summary>
        /// The type of command that the command-text represents
        /// 命令的类型(语句，存储过程，表)
        /// </summary>
        public CommandType? CommandType { get; set; }

        /// <summary>
        /// Should data be buffered before returning?
        /// 数据是否在返回之前缓冲
        /// </summary>
        public bool Buffered => (Flags & CommandFlags.Buffered) != 0;

        /// <summary>
        /// Should the plan for this query be cached?
        /// 这个查询的计划是否缓存
        /// </summary>
        internal bool AddToCache => (Flags & CommandFlags.NoCache) == 0;

        /// <summary>
        /// Additional state flags against this command
        /// 对该命令附加的状态标志
        /// </summary>
        public CommandFlags Flags { get; set; }

        /// <summary>
        /// For asynchronous operations, the cancellation-token
        /// 对于异步操作，取消令牌
        /// </summary>
        public CancellationToken CancellationToken { get; set; }


        public CommandDefinition init()
        {
            CommandDefinition command = new CommandDefinition(CommandText, Parameters, Transaction, CommandTimeout, CommandType, Flags, CancellationToken);
            return command;


        }

    }
}
