using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using Dapper;
using ZondyDBHelper;

namespace ZondyDBHelper
{

    /// <summary>
    /// 数据库操作接口
    /// </summary>
    public interface IDbAccess : IDisposable
    {
        /// <summary>
        /// 开启事务
        /// </summary>
        void BeginTrans();
        /// <summary>
        /// 关闭连接
        /// </summary>
        void Close();
        /// <summary>
        /// 提交事务
        /// </summary>
        void Commit();
        /// <summary>
        /// 打开连接
        /// </summary>
        void Open();
        /// <summary>
        /// 事务回滚
        /// </summary>
        void Rollback();
        /// <summary>
        /// 获取或设置连接状态
        /// </summary>
        bool AlwaysOpen { get; set; }
        /// <summary>
        /// 连接串
        /// </summary>
        string Connection { get; }
        /// <summary>
        /// 数据库类型
        /// </summary>
        DataBaseType DataBaseType { get; }
        /// <summary>
        /// 事务锁定级别设置
        /// </summary>
        IsolationLevel IsolationLevel { set; }
        /// <summary>
        /// 执行Sql语句
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <returns></returns>
        bool ExecuteSql(string strSql);
        /// <summary>
        /// 执行带参数的Sql语句
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <param name="listDataField">参数</param>
        /// <returns></returns>
        bool ExecuteSql(string strSql, List<DataField> listDataField);

        /// <summary>
        /// 执行带参数的Sql语句
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <param name="listDataField">参数</param>
        /// <returns></returns>
        int ExecuteSql(string strSql, object param = null);

        /// <summary>
        /// 执行Sql语句
        /// </summary>
        /// <param name="strSql">sql语句字符串数组</param>
        void ExecuteSql(ref string[] strSql);
        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="strProName">存储过程名称</param>
        /// <returns></returns>
        int ExecuteNonQuery(string strProName);

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="strProName">存储过程名称</param>
        /// <param name="listDataField">数据库对象</param>
        /// <returns></returns>
        int ExecuteNonQuery(string strProName, List<DataField> listDataField);

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="strProName">存储过程名称</param>
        /// <param name="parems">存储过程名称</param>
        /// <returns></returns>
        int ExecuteNonQuery(string strProName, DynamicParameters parems);

        /// <summary>
        /// 执行Sql语句
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <returns></returns>
        string[] ExecuteSql(params string[] strSql);

        /// <summary>
        /// 根据查询语句返回一条数据模型
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <returns></returns>
        List<T> Query<T>(string strSql);

        /// <summary>
        /// 根据查询语句返回一条数据模型
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <returns></returns>
        T QuerySingle<T>(string strSql);

        /// <summary>
        /// 删除指定的数据行
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">条件字符串，不包含where关键字，不可以为空</param>
        /// <returns>成功返回影响的记录行数，失败返回-1</returns>
        int DeleteTableRow(string tableName, string strFilter);
        /// <summary>
        /// 得到第一行第一个字段的内容
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <returns></returns>
        object GetFirstColumn(string strSql);
        /// <summary>
        /// 得到第一行第一个字段的内容
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <returns></returns>
        object GetFirstColumn(string strSql, object param = null);
        /// <summary>
        /// 得到第一行第一个字段的内容
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <param name="listDataField">参数</param>
        /// <returns></returns>
        object GetFirstColumn(string strSql, List<DataField> listDataField);
        /// <summary>
        /// 得到数据集
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <returns>返回DataSet</returns>
        DataSet GetDataSet(string strSql);
        /// <summary>
        /// 得到数据集
        /// </summary>
        /// <param name="strSql">sql语句字符串</param>
        /// <param name="htParams">参数</param>
        /// <returns>返回DataSet</returns>
        DataSet GetDataSet(string strSql, List<DataField> listDataField);
        /// <summary>
        /// 得到数据集
        /// </summary>
        /// <param name="tableName">数据表名</param>
        /// <param name="strFilter">条件字符串，不包含where关键字，不可以为空</param>
        /// <returns>返回DataSet</returns>
        DataSet GetDataSet(string tableName, string strFilter);
        /// <summary>
        /// 得到数据集
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="columnsList">字段列表字符串，用“,”连接各个字段</param>
        /// <param name="strFilter">条件字符串，不包含where关键字，不可以为空</param>
        /// <returns>返回DataSet</returns>
        DataSet GetDataSet(string tableName, string columnsList, string strFilter);
        /// <summary>
        /// 得到数据集
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="columnsList">字段列表字符串，用“,”连接各个字段</param>
        /// <param name="strFilter">条件字符串，不包含where关键字，不可以为空</param>
        /// <param name="orderList">排序字符串，不需要Order By，不可以为空</param>
        /// <returns>返回DataSet</returns>
        DataSet GetDataSet(string tableName, string columnsList, string strFilter, string orderList);
        /// <summary>
        /// 得到数据集
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="columnsList">字段列表字符串，用“,”连接各个字段</param>
        /// <param name="strFilter">条件字符串，不包含where关键字，不可以为空</param>
        /// <param name="orderList">排序字符串，不需要Order By，不可以为空</param>
        /// <param name="message">错误信息</param>
        /// <returns>返回DataSet，执行错误以Message的方式返回</returns>
        DataSet GetDataSet(string tableName, string columnsList, string strFilter, string orderList, ref string message);
        /// <summary>
        /// 判断指定的字段是否存在于指定的表中
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="columnName">数据库字段名</param>
        /// <returns></returns>
        bool JudgeColumnExist(string tableName, string columnName);
        /// <summary>
        /// 判断记录是否存在
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="strFilter">条件字符串，不包含where关键字，不可以为空</param>
        /// <returns></returns>
        bool JudgeRecordExist(string tableName, string strFilter);
        /// <summary>
        /// 判断表或视图存不存在
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <returns></returns>
        bool JudgeTableOrViewExist(string tableName);
        /// <summary>
        /// 将数据添加至数据库中
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="ht">哈希表</param>
        /// <returns>插入是否成功</returns>
        bool AddData(string tableName, Hashtable ht);
        /// <summary>
        /// 将数据更新到数据库中
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="ht">哈希表</param>
        /// <param name="filterColumnsName">关键字字段（可以是多个字段，以","隔开）</param>
        /// <returns>更新是否成功</returns>
        bool UpdateData(string tableName, Hashtable ht, string filterColumnsName);
        /// <summary>
        /// 将数据更新到数据库中,解决在数据库中设置了主键后，数据无法更新的问题
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="ht">哈希表</param>
        /// <param name="filter">条件字符串，不包含where关键字，不可以为空</param>
        /// <returns>更新是否成功</returns>
        bool UpdateData2(string tableName, Hashtable ht, string filter);
        /// <summary>
        /// 将数据保存至数据库中，由关键字段自动判断执行更新或者插入操作
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="ht">哈希表</param>
        /// <param name="filterColumnsName">关键字字段（可以是多个字段，以","隔开）</param>
        /// <returns>操作是否成功</returns>
        bool SaveData(string tableName, Hashtable ht, string filterColumnsName);
        /// <summary>
        /// 将数据添加至数据库中
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="listDataField">List</param>
        /// <returns>插入是否成功</returns>
        bool AddData(string tableName, List<DataField> listDataField);
        /// <summary>
        /// 将数据更新到数据库中
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="listDataField">List</param>
        /// <param name="filterColumnsName">关键字字段（可以是多个字段，以","隔开）</param>
        /// <returns>更新是否成功</returns>
        bool UpdateData(string tableName, List<DataField> listDataField, string filterColumnsName);
        /// <summary>
        /// 将数据更新到数据库中,解决在数据库中设置了主键后，数据无法更新的问题
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="listDataField">List</param>
        /// <param name="filter">条件字符串，不包含where关键字，不可以为空</param>
        /// <returns>更新是否成功</returns>
        bool UpdateData2(string tableName, List<DataField> listDataField, string filter);
        /// <summary>
        /// 将数据保存至数据库中，由关键字段自动判断执行更新或者插入操作
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="listDataField">List</param>
        /// <param name="filterColumnsName">关键字字段（可以是多个字段，以","隔开）</param>
        /// <returns>操作是否成功</returns>
        bool SaveData(string tableName, List<DataField> listDataField, string filterColumnsName);
        /// <summary>
        /// 设置指定数据库表的指定字段自动增加编号
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="columnName">字段名</param>
        /// <returns></returns>
        int SetID(string tableName, string columnName);
        /// <summary>
        /// 设置指定数据库表的指定字段自动增加编号
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="columnName">字段名</param>
        /// <param name="strFilter">条件字符串，不包含where关键字，不可以为空</param>
        /// <returns></returns>
        int SetID(string tableName, string columnName, string strFilter);
        /// <summary>
        /// 自定义分页查询语句的拼装
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strKeyWord">关键字</param>
        /// <param name="PageSize">页尺寸</param>
        /// <param name="PageIndex">页码</param>
        /// <param name="strWhere">条件语句</param>
        /// <param name="strOrder">排序语句</param>
        /// <returns>返回相应的Sql语句</returns>
        string GetSqlForPageSize(string tableName, string strKeyWord, int PageSize, int PageIndex, string strWhere, string strOrder);
        /// <summary>
        /// 自定义分页查询语句的拼装
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strKeyWord">关键字</param>
        /// <param name="columnList">字段列表</param>
        /// <param name="PageSize">页尺寸</param>
        /// <param name="PageIndex">页码</param>
        /// <param name="strWhere">条件语句</param>
        /// <param name="strOrder">排序语句</param>
        /// <returns>返回相应的Sql语句</returns>
        string GetSqlForPageSize(string tableName, string strKeyWord, string columnList, int PageSize, int PageIndex, string strWhere, string strOrder);
        /// <summary>
        /// 得到指定表的字段名称、字段数据类型、字段长度、字段排列序号
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <returns>返回记录集</returns>
        DataSet GetTableColunms(string tableName);
        /// <summary>
        /// 获取数据库中的所有表
        /// </summary>
        /// <returns>表名集合(TABLE_NAME)</returns>
        DataTable GetTableName();

        /// <summary>
        /// 获取数据库中的所有视图
        /// </summary>
        /// <returns>表名集合(TABLE_NAME)</returns>
        DataTable GetViewName();

        /// <summary>
        /// 获取数据库中的指定表内容
        /// </summary>
        /// <returns>表名集合(TABLE_NAME)</returns>
        TableData GetTableData(string tableName);

        /// <summary>
        /// 获取数据库中的指定表信息(字段信息)
        /// </summary>
        /// <returns>表名集合(TABLE_NAME)</returns>
        TableInfo GetTableInfo(string tableName);
        /// <summary>
        /// 获取指定数据库表的主键
        /// （如由多个字段联合组成，则以","分隔）
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <returns></returns>
        string GetPrimaryKey(string tableName);
        /// <summary>
        /// 获取对应的DataReader,与Read结合使用
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns></returns>
        IDataReader GetDataReader(string strSql);
        /// <summary>
        /// 快速插入数据
        /// </summary>
        /// <param name="dt">datatable</param>
        /// <param name="sTableName">表名</param>
        bool BulkToDB(DataTable dt, string sTableName);
    }
}
