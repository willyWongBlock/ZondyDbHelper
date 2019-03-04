/* ======================================================================== 
 * 【本类功能概述】   数据库表操作类
 *  
 * 作者：Willy Wong       时间：2018/09/27
 * 文件名：ModelDALTable 
 * CLR版本：
 * 
 * 修改者：           时间：               
 * 修改说明： 
 * ========================================================================*/
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ZondyDBHelper
{
    public class ModelDALTable<T> : ModelDALBase<T> where T : class
    {
        private string _tableName;
        private List<string> _ignoreColumns;
        private string _strPrimaryKey;
        #region 共有属性
        /// <summary>
        /// 表明
        /// </summary>
        protected string strTableName { get => _tableName; set => _tableName = value; }
        /// <summary>
        /// 更新需要屏蔽的字段
        /// </summary>
        protected List<string> IgnoreColumns { get => _ignoreColumns; set => _ignoreColumns = value; }

        protected string StrPrimaryKey { get => _strPrimaryKey; set => _strPrimaryKey = value; }

        #endregion

        #region 构造函数
        public ModelDALTable()
        {
            var type = typeof(T);
            this.strTableName = type.Name.ToUpper().Replace("MODEL", "");
            this.IgnoreColumns = new List<string>();

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
                        this.StrPrimaryKey = var.Name;
                        break;
                    }
                    if (myattr.IgnoreColumns == true)
                    {
                        this.IgnoreColumns.Add(var.Name);
                    }
                }
            }
        }
        #endregion

        /// <summary>
        /// 插入新记录
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="result">表对象</param>
        /// <returns>保存是否成功</returns>
        public bool Add(IDbAccess iDb, T result)
        {
            int i = 0;
            string strSql = InsertSql(iDb.DataBaseType);

            i = iDb.ExecuteSql(strSql, result);

            if (i > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// 更新非主键的模型全部字段
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="result">表对象</param>
        /// <param name="strColumn">更新的条件字段,不传值默认为主键字段</param>
        /// <returns>保存是否成功</returns>
        public bool Update(IDbAccess iDb, T result, string strColumn = "")
        {
            int i = 0;
            string strSql = UpdateSql(iDb.DataBaseType, strColumn);

            i = iDb.ExecuteSql(strSql, result);

            if (i > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }


        /// <summary>
        /// 更新去掉模型传入为空的字段
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="result">表对象</param>
        /// <param name="strColumn">更新的条件字段,不传值默认为主键字段</param>
        /// <returns>保存是否成功</returns>
        public bool UpdateIgnoreNull(IDbAccess iDb, T result, string strColumn = "")
        {
            int i = 0;
            string strSql = UpdateSqlIgnoreNull(iDb.DataBaseType, result, strColumn);

            i = iDb.ExecuteSql(strSql, result);

            if (i > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// 保存
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="result">表对象</param>
        /// <param name="strColumn">保存的条件字段,不传值默认为主键字段</param>
        /// <returns>保存是否成功</returns>
        public bool Save(IDbAccess iDb, T result, string strColumn = "")
        {
            bool bRrt = false;

            if (string.IsNullOrEmpty(strColumn))
            {
                strColumn = this.StrPrimaryKey;
            }

            string strSql = GetCountByColumns(iDb.DataBaseType, strColumn);

            int iCount = Convert.ToInt32(iDb.GetFirstColumn(strSql, result));
            if (iCount > 0)
            {
                bRrt = Update(iDb, result, strColumn);
            }
            else
            {
                bRrt = Add(iDb, result);
            }

            return bRrt;
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <param name="iDb">数据库对象</param>
        /// <param name="sFilter">条件字符串，以AND开头，不能为空</param>
        /// <param name="result">表对象</param>
        /// <returns>返回删除的记录行数</returns>
        public int Delete(IDbAccess iDb, string sFilter, T result)
        {
            int i = 0;
            if (string.IsNullOrWhiteSpace(sFilter))
            {
                throw new Exception("条件字符串不能为空");
            }
            string strSql = Delete(sFilter);

            i = iDb.ExecuteSql(strSql, result);
            return i;
        }


        /// <summary>
        /// 获取插入语句
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <returns>插入语句</returns>
        public string InsertSql(DataBaseType baseType)
        {
            String fileds = "";
            String values = "";
            Type type = typeof(T);
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
                        continue;
                    }
                }
                if (this.IgnoreColumns.Contains(var.Name))
                {
                    continue;
                }
                fileds += var.Name + ",";
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        values += "@" + var.Name + ","; break;
                    case DataBaseType.ORACLE:
                        values += ":" + var.Name + ","; break;
                    case DataBaseType.MYSQL:
                        values += "?" + var.Name + ","; break;
                }
            }
            fileds = fileds.Substring(0, fileds.Length - 1);
            values = values.Substring(0, values.Length - 1);
            String strSql = "insert into {0}({1}) values({2})";
            strSql = String.Format(strSql, this.strTableName, fileds, values);
            return strSql;
        }

        ///// <summary>
        ///// 获取查询语句
        ///// </summary>
        ///// <param name="strFilter">条件语句 以and开头</param>
        ///// <param name="strField">查询的字段 可为空，为空查询全部字段</param>
        ///// <returns>查询语句</returns>
        //public string Select(string strFilter, string strField = "")
        //{
        //    if (string.IsNullOrWhiteSpace(strFilter))
        //    {
        //        throw new Exception("条件字符串不能为空");
        //    }

        //    String strSql = "";
        //    if (string.IsNullOrWhiteSpace(strField))
        //    {
        //        strSql = string.Format("select * from {0} where 1=1 {1}", this.tableName, strFilter);
        //    }
        //    else
        //    {
        //        strSql = string.Format("select {2} from {0} where 1=1 {1}", this.tableName, strFilter, strField);
        //    }
        //    return strSql;
        //}

        /// <summary>
        /// 获取删除语句
        /// </summary>
        /// <param name="strFilter">条件语句 以and开头</param>
        /// <returns>删除语句</returns>
        public string Delete(string strFilter)
        {
            if (string.IsNullOrWhiteSpace(strFilter))
            {
                throw new Exception("条件字符串不能为空");
            }

            String strSql = string.Format("delete {0} where 1=1 {1}", this.strTableName, strFilter);
            return strSql;
        }

        /// <summary>
        /// 获取更新语句 更新部分字段
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <param name="hashTable">更新字段与值哈希表</param>
        /// <param name="strFilter">查询条件</param>
        /// <returns>更新语句</returns>
        public string Update(DataBaseType baseType, Hashtable hashTable, string strFilter)
        {
            String fileds = "";

            foreach (DictionaryEntry var in hashTable)
            {
                fileds += var.Key;
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        fileds += "@" + var.Key + ","; break;
                    case DataBaseType.ORACLE:
                        fileds += ":" + var.Key + ","; break;
                    case DataBaseType.MYSQL:
                        fileds += "?" + var.Key + ","; break;
                }
            }
            String strSql = "update {0} set {1} where  1=1 {2}";
            strSql = String.Format(strSql, this.strTableName, fileds, strFilter);
            return strSql;
        }

        /// <summary>
        /// 获取更新语句 更新非主键与字段为null的字段
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <param name="result">表对象</param>
        /// <param name="strColumn">条件字段,不传值默认为主键字段</param>
        /// <returns>更新语句</returns>
        public string UpdateSqlIgnoreNull(DataBaseType baseType, T result, string strColumn = "")
        {
            String fileds = "";
            string strFilter = " and ";
            Type type = typeof(T);

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
                        if (string.IsNullOrEmpty(strColumn))
                        {
                            switch (baseType)
                            {
                                case DataBaseType.SQLSERVER:
                                    strFilter = " and " + var.Name + "=@" + var.Name; break;
                                case DataBaseType.ORACLE:
                                    strFilter = " and " + var.Name + "=:" + var.Name; break;
                                case DataBaseType.MYSQL:
                                    strFilter = " and " + var.Name + "=?" + var.Name; break;
                            }
                        }
                        continue;
                    }
                }
                if (this.IgnoreColumns.Contains(var.Name))
                {
                    continue;
                }
                Object o = var.GetValue(result);
                if (o != null)
                {
                    fileds += var.Name + "=";
                    switch (baseType)
                    {
                        case DataBaseType.SQLSERVER:
                            fileds += "@" + var.Name + ","; break;
                        case DataBaseType.ORACLE:
                            fileds += ":" + var.Name + ","; break;
                        case DataBaseType.MYSQL:
                            fileds += "?" + var.Name + ","; break;
                    }
                }
                else
                {
                    continue;
                }
            }
            fileds = fileds.Substring(0, fileds.Length - 1);

            if (!string.IsNullOrEmpty(strColumn))
            {
                string[] arryFilterColumns = strColumn.Split(',');
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
            }

            String strSql = "update {0} set {1} where  1=1 {2}";
            strSql = String.Format(strSql, this.strTableName, fileds, strFilter);
            return strSql;
        }

        /// <summary>
        /// 获取更新语句 更新非主键的模型全部字段
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <param name="strColumn">条件字段,不传值默认为主键字段</param>
        /// <returns>更新语句</returns>
        public  string UpdateSql(DataBaseType baseType, string strColumn = "")
        {
            String fileds = "";
            string strFilter = " and ";
            Type type = typeof(T);

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
                        if (string.IsNullOrEmpty(strColumn))
                        {
                            switch (baseType)
                            {
                                case DataBaseType.SQLSERVER:
                                    strFilter = " and " + var.Name + "=@" + var.Name; break;
                                case DataBaseType.ORACLE:
                                    strFilter = " and " + var.Name + "=:" + var.Name; break;
                                case DataBaseType.MYSQL:
                                    strFilter = " and " + var.Name + "=?" + var.Name; break;
                            }
                        }
                        continue;
                    }
                }
                if (this.IgnoreColumns.Contains(var.Name))
                {
                    continue;
                }
                fileds += var.Name + "=";
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        fileds += "@" + var.Name + ","; break;
                    case DataBaseType.ORACLE:
                        fileds += ":" + var.Name + ","; break;
                    case DataBaseType.MYSQL:
                        fileds += "?" + var.Name + ","; break;
                }
            }
            fileds = fileds.Substring(0, fileds.Length - 1);

            if (!string.IsNullOrEmpty(strColumn))
            {
                string[] arryFilterColumns = strColumn.Split(',');
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
            }

            String strSql = "update {0} set {1} where  1=1 {2}";
            strSql = String.Format(strSql, this.strTableName, fileds, strFilter);
            return strSql;
        }

        ///// <summary>
        ///// 获取更新语句 更新全部字段
        ///// </summary>
        ///// <param name="baseType">数据库类型</param>
        ///// <returns>更新语句</returns>
        //public static string Update(DataBaseType baseType)
        //{
        //    String fileds = "";
        //    String strKey = "";
        //    Type type = typeof(T);
        //    //获取类名
        //    String className = type.Name.ToUpper().Replace("MODELS", "");
        //    //获取所有公有属性
        //    PropertyInfo[] info = type.GetProperties();

        //    foreach (PropertyInfo var in info)
        //    {
        //        //取得属性的特性标签，false表示不获取因为继承而得到的标签
        //        Object[] attr = var.GetCustomAttributes(false);
        //        if (attr.Length > 0)
        //        {
        //            //从注解数组中取第一个注解(一个属性可以包含多个注解)
        //            MyAttribute myattr = attr[0] as MyAttribute;
        //            if (myattr.PrimaryKey == true)
        //            {
        //                strKey = " and " + var.Name + "=@" + var.Name;
        //                continue;
        //            }
        //        }
        //        fileds += var.Name + "=";
        //        switch (baseType)
        //        {
        //            case DataBaseType.SQLSERVER:
        //                fileds += "@" + var.Name + ","; break;
        //            case DataBaseType.ORACLE:
        //                fileds += ":" + var.Name + ","; break;
        //        }
        //    }
        //    fileds = fileds.Substring(0, fileds.Length - 1);

        //    String strSql = "update {0} set {1} where 1=1 {2}";
        //    strSql = String.Format(strSql, className, fileds, strKey);
        //    return strSql;
        //}

        ///// <summary>
        ///// 获取查询个数的语句
        ///// </summary>
        ///// <param name="strFilter">查询条件</param>
        ///// <returns>查询个数语句</returns>
        //public string GetCount(string strFilter)
        //{

        //    String strSql = "select count(1) from {0} where 1=1 {1}";
        //    strSql = String.Format(strSql, this.tableName, strFilter);
        //    return strSql;
        //}

        ///// <summary>
        ///// 获取查询个数的语句
        ///// </summary>
        ///// <param name="baseType">数据库类型</param>
        ///// <param name="filterColumnsName">条件字段,不传值默认为主键字段</param>
        ///// <returns>查询个数语句</returns>
        //public string GetCountByColumns(DataBaseType baseType, string filterColumnsName)
        //{
        //    string strFilter = " and ";
        //    string[] arryFilterColumns = filterColumnsName.Split(',');
        //    for (int i = 0; i < arryFilterColumns.Length; i++)
        //    {
        //        switch (baseType)
        //        {
        //            case DataBaseType.SQLSERVER:
        //                strFilter += string.Format(" {0}=@{0} ", arryFilterColumns[i]); break;
        //            case DataBaseType.ORACLE:
        //                strFilter += string.Format(" {0}=:{0} ", arryFilterColumns[i]); break;
        //            case DataBaseType.MYSQL:
        //                strFilter += string.Format(" {0}=?{0} ", arryFilterColumns[i]); break;
        //        }
        //    }

        //    String strSql = "select count(1) from {0} where 1=1 {1}";
        //    strSql = String.Format(strSql, this.tableName, strFilter);
        //    return strSql;
        //}
    }
}
