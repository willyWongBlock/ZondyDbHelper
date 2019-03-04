using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Data;

namespace ZondyDBHelper
{
    /// <summary>
    /// 数据库表存储操作接口
    /// </summary>
    /// <typeparam name="T">实现接口的类</typeparam>
    public interface IDBHelperAccess<T>
    {
        /// <summary>
        /// 插入一条新数据
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="result">要插入的数据</param>
        /// <returns>返回插入数据是否成功</returns>
        bool Add(IDbAccess iDb, T result);
        /// <summary>
        /// 保存数据（内部判断是更新还是插入）
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="result">要保存的数据</param>
        /// <returns>返回保存数据是否成功</returns>
        bool Save(IDbAccess iDb, T result);
        /// <summary>
        /// 获取一行数据
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="sFilter">过滤条件</param>
        /// <returns>返回查询到的数据</returns>
        T Get(IDbAccess iDb, string sFilter);
        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="sFilter">过滤条件</param>
        /// <returns>返回影响的记录行数</returns>
        int Delete(IDbAccess iDb, string sFilter); 
        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="sFilter">过滤条件</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类别 ASC/DESC</param>
        /// <returns>返回查询到的记录集</returns>
        DataSet Search(IDbAccess iDb, string sFilter,string sOrderColumn,string sOrderType);
        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="sFilter">过滤条件</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类别 ASC/DESC</param>
        /// <returns>返回查询到的记录集</returns>
        List<T> SearchList(IDbAccess iDb, string sFilter,string sOrderColumn,string sOrderType);
        /// <summary>
        /// 以分页的方式查询数据
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="sKey">关键字段名称</param>
        /// <param name="iPageSize">每页数据行大小</param>
        /// <param name="iPageIndex">页索引</param>
        /// <param name="sFilter">过滤条件，以AND开头,可以为空</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类别 ASC/DESC</param>
        /// <returns>返回总记录行数Count和记录集TableResult</returns>
        SearchResult SearchByPage(IDbAccess iDb, string sKey, int iPageSize, int iPageIndex, string sFilter, string sOrderColumn, string sOrderType);
        /// <summary>
        /// 以分页的方式查询数据
        /// </summary>
        /// <param name="iDb">数据库接口</param>
        /// <param name="sKey">关键字段名称</param>
        /// <param name="iPageSize">每页数据行大小</param>
        /// <param name="iPageIndex">页索引</param>
        /// <param name="sFilter">过滤条件，以AND开头,可以为空</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类别 ASC/DESC</param>
        /// <returns>返回总记录行数Count和记录集ListResult</returns>
        SearchListResult<T> SearchListByPage(IDbAccess iDb, string sKey, int iPageSize, int iPageIndex, string sFilter, string sOrderColumn, string sOrderType);
    }
}
