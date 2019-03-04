using System;
using System.Data;

namespace ZondyDBHelper
{
    /// <summary>
    /// 数据库类型
    /// </summary>
    public enum DataBaseType
    {
        /// <summary>
        /// MangoDB
        /// </summary>
        DBTYPE,
        /// <summary>
        /// Oracle
        /// </summary>
        ORACLE,
        /// <summary>
        /// SqlServer
        /// </summary>
        SQLSERVER,
        /// <summary>
        /// MYSQL
        /// </summary>
        MYSQL,
        /// <summary>
        /// REDIS
        /// </summary>
        REDIS,
        /// <summary>
        /// MangoDB
        /// </summary>
        MANGODB
    }

    /// <summary>
    /// Word设计数据类型
    /// </summary>
    public enum WordTableComunmType
    {
        /// <summary>
        /// 字符串
        /// </summary>
        STRING,
        /// <summary>
        /// 长文本
        /// </summary>
        TEXT,
        /// <summary>
        /// 文件
        /// </summary>
        FILE,
        /// <summary>
        /// 长整形 
        /// </summary>
        LONG,
        /// <summary>
        /// 整形
        /// </summary>
        INT,
        /// <summary>
        /// 浮点型
        /// </summary>
        FLOAT,
        /// <summary>
        /// 数字型
        /// </summary>
        NUMBER,
        /// <summary>
        /// 日期
        /// </summary>
        DATETIME,
        /// <summary>
        /// bit型
        /// </summary>
        BIT
    }

    /// <summary>
    /// 支持的Oracle字段的类型
    /// </summary>
    public enum OracleColunmType
    {
        /// <summary>
        /// 数字
        /// </summary>
        NUMBER,
        /// <summary>
        /// 日期
        /// </summary>
        DATE,
        /// <summary>
        /// 长整形
        /// </summary>
        LONG,
        /// <summary>
        /// 字符串
        /// </summary>
        NVARCHAR2,
        /// <summary>
        /// 字符串
        /// </summary>
        NCHAR,
        /// <summary>
        /// 字符串
        /// </summary>
        VARCHAR2,
        /// <summary>
        /// 字符
        /// </summary>
        CHAR,
        /// <summary>
        /// 二进制文件
        /// </summary>
        BLOB,
        /// <summary>
        /// 二进制字符串
        /// </summary>
        CLOB
    }

    /// <summary>
    /// 支持SqlServer字段类型
    /// </summary>
    public enum SqlServerColunmType
    {
        /// <summary>
        /// 大整形
        /// </summary>
        BIGINT,
        /// <summary>
        /// 整形
        /// </summary>
        INT,
        /// <summary>
        /// 短整型
        /// </summary>
        SMALLINT,
        /// <summary>
        /// 
        /// </summary>
        TINYINT,
        /// <summary>
        /// 自然数
        /// </summary>
        DECIMAL,
        /// <summary>
        /// 浮点数
        /// </summary>
        FLOAT,
        /// <summary>
        /// 实数
        /// </summary>
        REAL,
        /// <summary>
        /// 日期
        /// </summary>
        DATETIME,
        /// <summary>
        /// 短日期
        /// </summary>
        SMALLDATETIME,
        /// <summary>
        /// 字符
        /// </summary>
        CHAR,
        /// <summary>
        /// 字符
        /// </summary>
        NCHAR,
        /// <summary>
        /// 字符串
        /// </summary>
        VARCHAR,
        /// <summary>
        /// 字符串
        /// </summary>
        NVARCHAR,
        /// <summary>
        /// 长文本
        /// </summary>
        TEXT,
        /// <summary>
        /// 长文本
        /// </summary>
        NTEXT,
        /// <summary>
        /// 二进制
        /// </summary>
        BINARY,
        /// <summary>
        /// 二进制
        /// </summary>
        VARBINARY,
        /// <summary>
        /// 文件
        /// </summary>
        IMAGE
    }

    /// <summary>
    /// 字段通用数据类型
    /// </summary>
    public enum DataColunmType
    {
        /// <summary>
        /// 大整形64位
        /// </summary>
        BIGINT,
        /// <summary>
        /// 整形32位
        /// </summary>
        INT,
        /// <summary>
        /// 短整型16位
        /// </summary>
        SMALLINT,
        /// <summary>
        /// 8位
        /// </summary>
        TINYINT,
        /// <summary>
        /// 固定精度和小数位位数值
        /// </summary>
        DECIMAL,
        /// <summary>
        /// 浮点数
        /// </summary>
        FLOAT,
        /// <summary>
        /// 浮点数
        /// </summary>
        REAL,
        /// <summary>
        /// 日期
        /// </summary>
        DATETIME,
        /// <summary>
        /// 短日期
        /// </summary>
        SMALLDATETIME,
        /// <summary>
        /// 字符固定长度流1-8000
        /// </summary>
        CHAR,
        /// <summary>
        /// 字符固定长度流1-4000
        /// </summary>
        NCHAR,
        /// <summary>
        /// 字符串
        /// </summary>
        VARCHAR,
        /// <summary>
        /// 字符串1-4000
        /// </summary>
        NVARCHAR,
        /// <summary>
        /// 长文本
        /// </summary>
        TEXT,
        /// <summary>
        /// 短文本
        /// </summary>
        NTEXT,
        /// <summary>
        /// 二进制
        /// </summary>
        BINARY,
        /// <summary>
        /// 二进制
        /// </summary>
        VARBINARY,
        /// <summary>
        /// 文件，长二进制
        /// </summary>
        IMAGE
    }

    /// <summary>
    /// 表信息
    /// </summary>
    [System.Serializable]
    public struct TableInfo
    {
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName;
        /// <summary>
        /// 表的描述
        /// </summary>
        public string TableDescription;
        /// <summary>
        /// 数据库类型
        /// </summary>
        public string DataBaseType;
        /// <summary>
        /// 表的结构信息
        /// </summary>
        public ColumnInfo[] Columns;
    }

    /// <summary>
    /// 字段结构
    /// </summary>
    [System.Serializable]
    public struct ColumnInfo
    {
        /// <summary>
        /// 字段名称
        /// </summary>
        public string Name;
        /// <summary>
        /// 字段类型
        /// </summary>
        public string Type;
        /// <summary>
        /// 字段长度
        /// </summary>
        public int Length;
        /// <summary>
        /// 序号
        /// </summary>
        public int Serial;
        /// <summary>
        /// 说明
        /// </summary>
        public string Remark;
        /// <summary>
        /// 允许空
        /// </summary>
        public bool IsNull;
        /// <summary>
        /// 主键
        /// </summary>
        public bool IsKey;
        /// <summary>
        /// 自增长
        /// </summary>
        public bool IsIdentity;
        /// <summary>
        /// 默认值
        /// </summary>
        public string DefauleValue;

        /// <summary>
        /// 整数位
        /// </summary>
        public int Precision;

        /// <summary>
        /// 小数位
        /// </summary>
        public int Scale;
    }
    /// <summary>
    /// 数据库表信息
    /// </summary>
    [System.Serializable]
    public struct TableData
    {
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName;
        /// <summary>
        /// 表的描述
        /// </summary>
        public string TableDescription;
        /// <summary>
        /// 数据库类型
        /// </summary>
        public string DataBaseType;
        /// <summary>
        /// 表的结构信息
        /// </summary>
        public ColumnInfo[] Columns;
        /// <summary>
        /// 表数据
        /// </summary>
        public DataTable Data;
    }

    /// <summary>
    /// SQLite的字段类型
    /// </summary>
    public enum SQLiteColunmType
    {
        /// <summary>
        /// 整数值
        /// </summary>
        INTEGER,
        /// <summary>
        /// 实数
        /// </summary>
        REAL,
        /// <summary>
        /// 文本
        /// </summary>
        TEXT,
        /// <summary>
        /// 二进制大对象
        /// </summary>
        BLOB,
        /// <summary>
        /// 没有值
        /// </summary>
        NULL
    }
    /// <summary>
    /// 字段
    /// </summary>
    public class DataField
    {
        /// <summary>
        /// 构造函数,字段类型为varchar
        /// </summary>
        /// <param name="DataFieldName"></param>
        /// <param name="DataFieldValue"></param>
        public DataField(string DataFieldName, object DataFieldValue)
        {
            this.DataFieldName = DataFieldName;
            if (DataFieldValue == null || DataFieldValue.ToString() == "")
            {
                this.DataFieldValue = DBNull.Value;
            }
            else
            {
                this.DataFieldValue = DataFieldValue;
            }
            this.DataFieldType = DataColunmType.VARCHAR;
        }
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="DataFieldName"></param>
        /// <param name="DataFieldValue"></param>
        /// <param name="DataFieldType"></param>
        public DataField(string DataFieldName, object DataFieldValue, DataColunmType DataFieldType)
        {
            this.DataFieldName = DataFieldName;
            if (DataFieldValue == null || DataFieldValue.ToString()=="")
            {
                this.DataFieldValue = DBNull.Value;
            }
            else
            {
                this.DataFieldValue = DataFieldValue;
            }
            this.DataFieldType = DataFieldType;
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="DataFieldName"></param>
        /// <param name="DataFieldValue"></param>
        /// <param name="DataFieldType"></param>
        public DataField(string DataFieldName, object DataFieldValue, DataColunmType DataFieldType,ParameterDirection ParameterDirType)
        {
            this.DataFieldName = DataFieldName;
            if (DataFieldValue == null || DataFieldValue.ToString() == "")
            {
                this.DataFieldValue = DBNull.Value;
            }
            else
            {
                this.DataFieldValue = DataFieldValue;
            }
            this.DataFieldType = DataFieldType;
            this.ParameterDirType = ParameterDirType;
        }
        /// <summary>
        /// 字段名
        /// </summary>
        public string DataFieldName{ get; set; }
        /// <summary>
        /// 字段值
        /// </summary>
        public object DataFieldValue{ get; set; }
        /// <summary>
        /// 字段类型
        /// </summary>
        public DataColunmType DataFieldType{ get; set; }

        /// <summary>
        /// 字段类型
        /// </summary>
        public ParameterDirection ParameterDirType { get; set; }
    }



}
