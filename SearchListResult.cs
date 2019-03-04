using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ZondyDBHelper
{
    /// <summary>
    /// 查询结果列表
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SearchListResult<T>
    {
        /// <summary>
        /// 结果行数
        /// </summary>
        public int Count { get; set; }
        /// <summary>
        /// 结果对象列表
        /// </summary>
        public List<T> ListResult { get; set; }
    }
}
