using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ZondyDBHelper
{
    public class SqlQuery<T> where T : class
    {
        /// <summary>
        /// 获取插入语句
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <returns>插入语句</returns>
        public static string InsertSql(DataBaseType baseType)
        {
            String fileds = "";
            String values = "";
            Type type = typeof(T);
            //获取所有公有属性
            PropertyInfo[] info = type.GetProperties();
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");

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
                fileds += var.Name + ",";
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        values += "@" + var.Name + ","; break;
                    case DataBaseType.ORACLE:
                        values += ":" + var.Name + ","; break;
                }
            }
            fileds = fileds.Substring(0, fileds.Length - 1);
            values = values.Substring(0, values.Length - 1);
            String strSql = "insert into {0}({1}) values({2})";
            strSql = String.Format(strSql, className, fileds, values);
            return strSql;
        }

        /// <summary>
        /// 获取查询语句
        /// </summary>
        /// <param name="strFilter">条件语句 以and开头</param>
        /// <param name="strField">查询的字段 可为空，为空查询全部字段</param>
        /// <returns>查询语句</returns>
        public static string Select(string strFilter, string strField = "")
        {
            if (string.IsNullOrWhiteSpace(strFilter))
            {
                throw new Exception("条件字符串不能为空");
            }
            Type type = typeof(T);
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");
            String strSql = "";
            if (string.IsNullOrWhiteSpace(strField))
            {
                strSql = string.Format("select * from {0} where 1=1 {1}", className, strFilter);
            }
            else
            {
                strSql = string.Format("select {2} from {0} where 1=1 {1}", className, strFilter, strField);
            }
            return strSql;
        }

        /// <summary>
        /// 获取删除语句
        /// </summary>
        /// <param name="strFilter">条件语句 以and开头</param>
        /// <returns>删除语句</returns>
        public static string Delete(string strFilter)
        {
            if (string.IsNullOrWhiteSpace(strFilter))
            {
                throw new Exception("条件字符串不能为空");
            }
            Type type = typeof(T);
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");
            String strSql = string.Format("delete {0} where 1=1 {1}", className, strFilter);
            return strSql;
        }

        /// <summary>
        /// 获取更新语句 更新部分字段
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <param name="hashTable">更新字段与值哈希表</param>
        /// <param name="strFilter">查询条件</param>
        /// <returns>更新语句</returns>
        public static string Update(DataBaseType baseType, Hashtable hashTable, string strFilter)
        {
            String fileds = "";

            Type type = typeof(T);
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");

            foreach (DictionaryEntry var in hashTable)
            {
                fileds += var.Key;
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        fileds += "@" + var.Key + ","; break;
                    case DataBaseType.ORACLE:
                        fileds += ":" + var.Key + ","; break;
                }
            }
            String strSql = "update {0} set {1} where  1=1 {2}";
            strSql = String.Format(strSql, className, fileds, strFilter);
            return strSql;
        }

        /// <summary>
        /// 获取更新语句 更新部分字段
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <param name="hashTable">更新字段与值哈希表</param>
        /// <param name="strFilter">查询条件</param>
        /// <returns>更新语句</returns>
        public static string Update(DataBaseType baseType, string strColumn = "")
        {
            String fileds = "";
            string strFilter = " and ";
            Type type = typeof(T);
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");

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
                            strFilter = " and " + var.Name + "=@" + var.Name;
                        }
                        continue;
                    }
                }
                fileds += var.Name + "=";
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        fileds += "@" + var.Name + ","; break;
                    case DataBaseType.ORACLE:
                        fileds += ":" + var.Name + ","; break;
                }
            }
            fileds = fileds.Substring(0, fileds.Length - 1);

            
            string[] arryFilterColumns = strColumn.Split(',');
            for (int i = 0; i < arryFilterColumns.Length; i++)
            {
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        strFilter += string.Format(" {0}=@{0} ", arryFilterColumns[i]); break;
                    case DataBaseType.ORACLE:
                        strFilter += string.Format(" {0}=:{0} ", arryFilterColumns[i]); break;
                }
            }

            String strSql = "update {0} set {1} where  1=1 {2}";
            strSql = String.Format(strSql, className, fileds, strFilter);
            return strSql;
        }

        /// <summary>
        /// 获取更新语句 更新全部字段
        /// </summary>
        /// <param name="baseType">数据库类型</param>
        /// <returns>更新语句</returns>
        public static string Update(DataBaseType baseType)
        {
            String fileds = "";
            String strKey = "";
            Type type = typeof(T);
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");
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
                        strKey = " and " + var.Name + "=@" + var.Name;
                        continue;
                    }
                }
                fileds += var.Name + "=";
                switch (baseType)
                {
                    case DataBaseType.SQLSERVER:
                        fileds += "@" + var.Name + ","; break;
                    case DataBaseType.ORACLE:
                        fileds += ":" + var.Name + ","; break;
                }
            }
            fileds = fileds.Substring(0, fileds.Length - 1);

            String strSql = "update {0} set {1} where 1=1 {2}";
            strSql = String.Format(strSql, className, fileds, strKey);
            return strSql;
        }

        /// <summary>
        /// 获取查询个数的语句
        /// </summary>
        /// <param name="strFilter">查询条件</param>
        /// <returns>查询个数语句</returns>
        public static string GetCount(string strFilter)
        {
            Type type = typeof(T);
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");
            String strSql = "select count(1) from {0} where 1=1 {1}";
            strSql = String.Format(strSql, className, strFilter);
            return strSql;
        }

        /// <summary>
        /// 获取查询个数的语句
        /// </summary>
        /// <param name="strFilter">查询条件</param>
        /// <returns>查询个数语句</returns>
        public static string GetCountByColumns(string filterColumnsName, DataBaseType baseType)
        {
            Type type = typeof(T);
            //获取类名
            String className = type.Name.ToUpper().Replace("MODEL", "");
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
                }
            }

            String strSql = "select count(1) from {0} where 1=1 {1}";
            strSql = String.Format(strSql, className, strFilter);
            return strSql;
        }
    }
}
