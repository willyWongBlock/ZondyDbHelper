using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ZondyDBHelper
{
    /// <summary>
    /// 查询结果集
    /// </summary>
    public class SearchResult
    {
        /// <summary>
        /// 记录行数
        /// </summary>
        public int Count { get; set; }
        /// <summary>
        /// 结果集合
        /// </summary>
        public DataSet TableResult { get; set; }
    }
}
