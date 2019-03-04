/* ======================================================================== 
 * 【本类功能概述】   数据库表操作类
 *  
 * 作者：Willy Wong       时间：2018/09/28
 * 文件名：ModelDALBase 
 * CLR版本：
 * 
 * 修改者：           时间：               
 * 修改说明： 
 * ========================================================================*/
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Text;

namespace ZondyDBHelper
{
    public class ModelDALBase<T> where T : class
    {
        private string _tableName;
        private string _strPrimaryKey;
        #region 私有属性
        #endregion
        #region 共有属性
        /// <summary>
        /// 表名
        /// </summary>
        protected string tableName { get => _tableName; set => _tableName = value; }

        protected string strPrimaryKey { get => _strPrimaryKey; set => _strPrimaryKey = value; }

        #endregion
        #region 构造函数
        public ModelDALBase()
        {
            var type = typeof(T);
            this.tableName = type.Name.ToUpper().Replace("MODEL", ""); ;

            //获取所有公有属性
            PropertyInfo[] info = type.GetProperties();

            foreach (PropertyInfo var in info)
            {
                //取得属性的特性标签，false表示不获取因为继承而得到的标签
                Object[] attr = var.GetCustomAttributes(false);
                if (attr.Length > 0)
                {
                    //从注解数组中取第一个注解(一个属性可以包含多个注解)
                    MyAttribute myattr = attr[0] as MyAttribute;
                    if (myattr.PrimaryKey == true)
                    {
                        this.strPrimaryKey = var.Name;
                        break;
                    }
                }
            }
        }
        #endregion
        /// <summary>
        /// 获取查询个数的语句
        /// </summary>
        /// <param name="strFilter">查询条件</param>
        /// <returns>查询个数语句</returns>
        public string GetCount(string strFilter)
        {

            String strSql = "select count(1) from {0} where 1=1 {1}";
            strSql = String.Format(strSql, this.tableName, strFilter);
            return strSql;
        }

        public virtual int GetDataCount(IDbAccess iDb, string sFilter)
        {
            string strSql = GetCount(sFilter);
            return Convert.ToInt32(iDb.GetFirstColumn(strSql));
        }

        /// <summary>
        /// 获取查询个数的语句
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <param name="filterColumnsName">条件字段,不传值默认为主键字段</param>
        /// <returns>查询个数语句</returns>
        public string GetCountByColumns(DataBaseType baseType, string filterColumnsName)
        {
            string strFilter = " and ";
            string[] arryFilterColumns = filterColumnsName.Split(',');
            for (int i = 0; i < arryFilterColumns.Length; i++)
            {
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        strFilter += string.Format(" {0}=@{0} ", arryFilterColumns[i]); break;
                    case DataBaseType.ORACLE:
                        strFilter += string.Format(" {0}=:{0} ", arryFilterColumns[i]); break;
                    case DataBaseType.MYSQL:
                        strFilter += string.Format(" {0}=?{0} ", arryFilterColumns[i]); break;
                }
            }

            String strSql = "select count(1) from {0} where 1=1 {1}";
            strSql = String.Format(strSql, this.tableName, strFilter);
            return strSql;
        }

        /// <summary>
        /// 返回分页数据List
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="sFilter">查询条件，以AND开头,可以为空</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类型 ASC/DESC</param>
        /// <returns>返回分页数据List</returns>
        public virtual List<T> GetFullList(IDbAccess iDb, string sFilter, string sOrderColumn, string sOrderType)
        {
            string sOrderString = "";
            if (sOrderColumn != "")
            {
                sOrderString = " ORDER BY " + sOrderColumn;
                if (sOrderType.ToLower() == "desc")
                {
                    sOrderString += " DESC ";
                }
                else
                {
                    sOrderString += " ASC ";
                }
            }
            sFilter = " 1=1 " + sFilter;
            string strSql = "SELECT * FROM " + this.tableName + " t WHERE " + sFilter + sOrderString;

            List<T> list = iDb.Query<T>(strSql);
            return list;
        }


        /// <summary>
        /// 获取一行记录
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="sFilter">条件字符串，以AND开头，不能为空</param>
        /// <returns></returns>
        public T Get(IDbAccess iDb, string sFilter)
        {
            if (string.IsNullOrWhiteSpace(sFilter))
            {
                throw new Exception("条件字符串不能为空");
            }
            return iDb.QuerySingle<T>(string.Format("SELECT * FROM {0} WHERE 1=1 {1}", this.tableName, sFilter));
        }

        /// <summary>
        /// 返回全部数据List
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="sFilter">查询条件，以AND开头,可以为空</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类型 ASC/DESC</param>
        /// <returns>返回全部数据List</returns>
        public List<T> SearchList(IDbAccess iDb, string sFilter, string sOrderColumn, string sOrderType, string strKeyColumn = "")
        {
            return SearchListByPage(iDb, "ID", 0, 0, sFilter, sOrderColumn, sOrderType, strKeyColumn).ListResult;
        }

        /// <summary>
        /// 返回分页数据List
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="sKey">表关键字</param>
        /// <param name="iPageSize">单页记录行数</param>
        /// <param name="iPageIndex">分页索引</param>
        /// <param name="sFilter">查询条件，以AND开头,可以为空</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类型 ASC/DESC</param>
        /// <returns>返回分页数据List</returns>
        public SearchListResult<T> SearchListByPage(IDbAccess iDb, string sKey, int iPageSize, int iPageIndex, string sFilter, string sOrderColumn, string sOrderType, string strKeyColumn = "")
        {
            SearchListResult<T> slr = new SearchListResult<T>();
            slr.Count = GetDataCount(iDb, sFilter);
            string sOrderString = "";
            if (sOrderColumn != "")
            {
                sOrderString = " ORDER BY " + sOrderColumn;
                if (sOrderType.ToLower() == "desc")
                {
                    sOrderString += " DESC ";
                }
                else
                {
                    sOrderString += " ASC ";
                }
            }
            sFilter = " 1=1 " + sFilter;
            string strSql = string.Format("SELECT * FROM {0} t WHERE {1} {2}", this.tableName, sFilter, sOrderString);
            if (iPageSize > 0)
            {
                if (string.IsNullOrEmpty(strKeyColumn))
                {
                    strSql = iDb.GetSqlForPageSize(this.tableName, this.strPrimaryKey, iPageSize, iPageIndex, sFilter, sOrderString);
                }
                else
                {
                    strSql = iDb.GetSqlForPageSize(this.tableName, strKeyColumn, iPageSize, iPageIndex, sFilter, sOrderString);
                }
            }

            slr.ListResult = iDb.Query<T>(strSql);
            return slr;
        }

        /// <summary>
        /// 返回全部数据DataSet
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="sFilter">查询条件，以AND开头,可以为空</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类型 ASC/DESC</param>
        /// <returns>返回全部数据DataSet</returns>
        public DataSet Search(IDbAccess iDb, string sFilter, string sOrderColumn, string sOrderType)
        {
            string sOrderString = "";
            if (sOrderColumn != "")
            {
                sOrderString = " ORDER BY " + sOrderColumn;
                if (sOrderType.ToLower() == "desc")
                {
                    sOrderString += " DESC ";
                }
                else
                {
                    sOrderString += " ASC ";
                }
            }
            sFilter = " 1=1 " + sFilter;
            string strSql = string.Format("SELECT * FROM {0} t WHERE {0} {1}", this.tableName, sFilter, sOrderString);
            DataSet ds = iDb.GetDataSet(strSql);
            return ds;
        }

        /// <summary>
        /// 返回分页数据DataSet
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="sKey">表关键字</param>
        /// <param name="iPageSize">单页记录行数</param>
        /// <param name="iPageIndex">分页索引</param>
        /// <param name="sFilter">查询条件，以AND开头,可以为空</param>
        /// <param name="sOrderColumn">排序字段，可以指定多个字段，以','分隔</param>
        /// <param name="sOrderType">排序类型 ASC/DESC</param>
        /// <returns>返回分页数据DataSet</returns>
        public SearchResult SearchByPage(IDbAccess iDb, string sKey, int iPageSize, int iPageIndex, string sFilter, string sOrderColumn, string sOrderType, string strKeyColumn = "")
        {
            SearchResult sr = new SearchResult();
            sr.Count = GetDataCount(iDb, sFilter);
            string sOrderString = "";
            if (sOrderColumn != "")
            {
                sOrderString = " ORDER BY " + sOrderColumn;
                if (sOrderType.ToLower() == "desc")
                {
                    sOrderString += " DESC ";
                }
                else
                {
                    sOrderString += " ASC ";
                }
            }
            sFilter = " 1=1 " + sFilter;
            string strSql = string.Format("SELECT * FROM {0} t WHERE {1} {2}", this.tableName, sFilter, sOrderString);
            if (iPageSize > 0)
            {
                if (string.IsNullOrEmpty(strKeyColumn))
                {
                    strSql = iDb.GetSqlForPageSize(this.tableName, this.strPrimaryKey, iPageSize, iPageIndex, sFilter, sOrderString);
                }
                else
                {
                    strSql = iDb.GetSqlForPageSize(this.tableName, strKeyColumn, iPageSize, iPageIndex, sFilter, sOrderString);
                }
            }
            DataSet ds = iDb.GetDataSet(strSql);
            sr.TableResult = ds;
            return sr;
        }
    }
}
