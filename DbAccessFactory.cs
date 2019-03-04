

using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.IO;
using System.Text;
using ZondyDBHelper;

namespace ZondyDBHelper
{
    /// <summary>
    /// 数据库对象实例化
    /// </summary>
    public class DbAccessFactory
    {
        public DbAccessFactory()
        { }
        /// <summary>
        /// 创建实例
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="dbType">数据库类型</param>
        /// <returns>实例</returns>
        public static IDbAccess CreateInstance(string connectionString,DataBaseType dbType)
        {
            switch (dbType)
            {
                case DataBaseType.ORACLE:
                    return new Oracle(connectionString);

                case DataBaseType.SQLSERVER:
                    return new SqlServer(connectionString);

                case DataBaseType.MYSQL:
                    return new MySql(connectionString);

                    //case DataBaseType.SQLITE:
                    //    return new SQLite(connectionString);
            }
            return null;
        }

        /// <summary>
        /// 创建实例
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="strType">数据库类型字符串</param>
        /// <returns>实例</returns>
        public static IDbAccess CreateInstance(string connectionString, string strType)
        {
            switch (strType.ToUpper())
            {
                case "ORACLE":
                    return new Oracle(connectionString);

                case "SQLSERVER":
                    return new SqlServer(connectionString);

                case "MYSQL":
                    return new MySql(connectionString);

                    //case "SQLITE":
                    //    return new SQLite(connectionString);
            }
            return null;
        }
         

        /// <summary>
        /// 测试创建实例方法，减少代码维护量
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="strType">数据库类型字符串</param>
        /// <returns>实例</returns>
        public static IDbAccess CreateInstanceTest(string connectionString, string strType)
        {
            switch (strType.ToUpper())
            {
                case "ORACLE":
                    return new Oracle(connectionString);

                case "SQLSERVER":
                    return new SqlServer(connectionString);

                case "MYSQL":
                    return new MySql(connectionString);

                    //case "SQLITE":
                    //    return new SQLite(connectionString);
            }
            return null;
        }

        /// <summary>
        /// 创建实例
        /// </summary>
        /// <returns>实例</returns>
        public static IDbAccess CreateInstance()
        {
            string sConnection = ConfigurationManager.AppSettings["connectionString"];
            string strType = ConfigurationManager.AppSettings["DataServerType"];

            try
            {
                //连接字符串加密
                DESClassInterface interface2 = new ZondyDBHelper.DESClass();
                sConnection = interface2.DecryptDB(sConnection);
            }
            catch
            {

            }
            return CreateInstance(sConnection, strType);
        }

        /// <summary>
        /// 获取Oracle数据库的连接字符串
        /// </summary>
        /// <param name="sNetServiceName">服务名</param>
        /// <param name="sUserID">用户名</param>
        /// <param name="sPassword">密码</param>
        /// <returns>Oracle数据库的连接字符串</returns>
        public static string GetOracleConnectionString(string sNetServiceName, string sUserID, string sPassword)
        {
            if (string.IsNullOrWhiteSpace(sNetServiceName) || string.IsNullOrWhiteSpace(sUserID) || string.IsNullOrWhiteSpace(sPassword))
                throw new Exception("请检查参数sServiceName，sUserID，sPassword是否为空");

            string sRtnValue = "Data Source=" + sNetServiceName +
                    ";User Id=" + sUserID +
                    ";Password=" + sPassword + ";";
            return sRtnValue;
        }

        /// <summary>
        /// 获取SQLServer数据库的连接字符串
        /// </summary>
        /// <param name="sServer">服务器地址或名称</param>
        /// <param name="sDataBaseName">数据库名</param>
        /// <param name="sUserID">用户名</param>
        /// <param name="sPassword">密码</param>
        /// <returns>SQLServer数据库的连接字符串</returns>
        public static string GetSQLServerConnectionString(string sServer,string sDataBaseName,string sUserID,string sPassword)
        {
            if (string.IsNullOrWhiteSpace(sServer) || string.IsNullOrWhiteSpace(sDataBaseName) || string.IsNullOrWhiteSpace(sUserID) )//|| string.IsNullOrWhiteSpace(sPassword))
                throw new Exception("请检查参数sServer，sDataBaseName，sUserID是否为空");

            string sRtnValue = "Data Source=" + sServer +
                    ";Initial Catalog=" + sDataBaseName +
                    ";User Id=" + sUserID +
                    ";Password=" + sPassword + ";";

            return sRtnValue;
        }

        /// <summary>
        /// 获取SqlServer数据库
        /// </summary>
        /// <param name="sServer">服务器地址</param>
        /// <param name="sUserID">用户名</param>
        /// <param name="sPassword">密码</param>
        /// <returns></returns>
        public static ArrayList GetSQLServerDataBaseName(string sServer, string sUserID, string sPassword)
        {
            string sConn = GetSQLServerConnectionString(sServer, "master", sUserID, sPassword);
            string strSql = "select name from master..sysdatabases";
            SqlServer sqlserver = new SqlServer(sConn);
            DataSet ds = sqlserver.GetDataSet(strSql);
            ArrayList al = new ArrayList();

            for (int ii = 0; ii < ds.Tables[0].Rows.Count; ii++)
            {
                al.Add(ds.Tables[0].Rows[ii][0].ToString());
            }

            return al;
        }
    }
}
