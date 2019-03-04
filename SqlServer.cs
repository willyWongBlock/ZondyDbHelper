using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Text;
using ZondyDBHelper;
using System.Data.SqlClient;
using Dapper;
using System.Linq;

namespace ZondyDBHelper
{
    internal class SqlServer : IDbAccess, IDisposable
    {
        private bool _alwaysOpen;
        private SqlConnection _connection;
        private SqlTransaction _transaction;
        private string sConnection;
        private IsolationLevel _IsolationLevel = IsolationLevel.ReadCommitted;
        private CommandDefinitionNew command; 

        /// <summary>
        /// 是否开启行锁
        /// </summary>
        private string _sRowLock = string.IsNullOrEmpty(System.Configuration.ConfigurationManager.AppSettings["RowLock"]) ? "" : " with(rowlock) ";
        //private string _sRowLock = "";

        public SqlServer(string connectionString)
        {
            sConnection = connectionString;
            _connection = new SqlConnection(connectionString);
            command = new CommandDefinitionNew();
        }

        #region IDbAccess 成员
        public void BeginTrans()
        {
            this.Open();
            this._transaction = this._connection.BeginTransaction(_IsolationLevel);
        }

        public void Close()
        {
            this._connection.Close();
        }

        private void CloseCommandConnection(IDbCommand dbCommand)
        {
            if ((dbCommand.Connection != null) && (dbCommand.Connection.State != ConnectionState.Closed))
            {
                dbCommand.Connection.Close();
            }
        }

        public void Commit()
        {
            if (this._transaction != null)
            {
                this._transaction.Commit();
                if (!this._alwaysOpen)
                {
                    this.Close();
                }
            }
        }

        public void Open()
        {
            if ((this._connection.State == ConnectionState.Closed) || (this._connection.State == ConnectionState.Broken))
            {
                this._connection.Open();
            }
        }

        public void Rollback()
        {
            if (this._transaction != null)
            {
                this._transaction.Rollback();
                if (!this._alwaysOpen)
                {
                    this.Close();
                }
            }
        }

        [DefaultValue(false)]
        public bool AlwaysOpen
        {
            get
            {
                return this._alwaysOpen;
            }
            set
            {
                this._alwaysOpen = value;
            }
        }
        public bool RowLock
        {
            get
            {
                if (string.IsNullOrEmpty(_sRowLock))
                {
                    return false;
                }
                else
                    return true;
            }
            set
            {
                if (value)
                {
                    _sRowLock = " with(rowlock) ";
                }
                else
                    _sRowLock = "";
            }
        }

        public string Connection
        {
            get { return sConnection; }
        }

        public DataBaseType DataBaseType
        {
            get { return DataBaseType.SQLSERVER; }
        }

        public IsolationLevel IsolationLevel
        {
            set { this._IsolationLevel = value; }
        }

        public bool ExecuteSql(string strSql)
        {
            //判断是否insert、update、delete
            if (!string.IsNullOrEmpty(_sRowLock))
            {
                strSql = strSql.ToUpper().TrimStart(' ');
                if (strSql.StartsWith("UPDATE") && strSql.IndexOf("ROWLOCK")<0)
                {
                    int index = strSql.IndexOf("SET");
                    if (index>0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);                        
                    }
                }
                else if (strSql.StartsWith("INSERT") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("(");
                    if (index<0)
                    {
                        index = strSql.IndexOf("SELECT");
                    }
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
                else if (strSql.StartsWith("DELETE") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("WHERE");
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
            }

            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                int aa = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                if (aa < 0)
                    return false;

                return true;
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception(strSql + "\r\n" + e.Message);
            }
        }

        public bool ExecuteSql(string strSql, List<DataField> listDataField)
        {
            //判断是否insert、update、delete
            if (!string.IsNullOrEmpty(_sRowLock))
            {
                strSql = strSql.ToUpper().TrimStart(' ');
                if (strSql.StartsWith("UPDATE") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("SET");
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
                else if (strSql.StartsWith("INSERT") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("(");
                    if (index < 0)
                    {
                        index = strSql.IndexOf("SELECT");
                    }
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
                else if (strSql.StartsWith("DELETE") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("WHERE");
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
            }

            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add("@" + dataField.DataFieldName, dataField.DataFieldValue);
            }

            command.Parameters = p;

            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }

                int aa = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                if (aa < 0)
                    return false;

                return true;
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception(strSql + "\r\n" + e.Message);
            }
        }

        public int ExecuteSql(string strSql, object param = null)
        {
            //判断是否insert、update、delete
            if (!string.IsNullOrEmpty(_sRowLock))
            {
                strSql = strSql.ToUpper().TrimStart(' ');
                if (strSql.StartsWith("UPDATE") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("SET");
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
                else if (strSql.StartsWith("INSERT") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("(");
                    if (index < 0)
                    {
                        index = strSql.IndexOf("SELECT");
                    }
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
                else if (strSql.StartsWith("DELETE") && strSql.IndexOf("ROWLOCK") < 0)
                {
                    int index = strSql.IndexOf("WHERE");
                    if (index > 0)
                    {
                        strSql = strSql.Substring(0, index) + " with(rowlock) " + strSql.Substring(index);
                    }
                }
            }

            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            command.Parameters = param;


            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                int aa = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                return aa;
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception(strSql + "\r\n" + e.Message);
            }
        }

        public void ExecuteSql(ref string[] strSql)
        {
            SqlConnection conn = _connection;
            SqlTransaction tra;

            conn.Open();
            tra = conn.BeginTransaction();
            try
            {

                //CommandDefinition command = new CommandDefinition();
                command.Transaction = tra;
                command.CommandType = CommandType.Text;

                for (int ii = 0; ii < strSql.Length; ii++)
                {
                    command.CommandText = strSql[ii];
                    strSql[ii] = conn.Execute(command.init()).ToString();
                }

                tra.Commit();
            }
            catch (Exception e)
            {
                tra.Rollback();
                throw new DataException(e.Message);
            }
            finally
            {
                conn.Dispose();
            }
        }

        public string[] ExecuteSql(params string[] strSql)
        {
            SqlConnection conn = _connection;
            SqlTransaction tra;

            conn.Open();
            tra = conn.BeginTransaction();
            try
            {
                //CommandDefinition comm = new CommandDefinition();
                command.Transaction = tra;
                command.CommandType = CommandType.Text;

                for (int ii = 0; ii < strSql.Length; ii++)
                {
                    command.CommandText = strSql[ii];
                    strSql[ii] = conn.Execute(command.init()).ToString();
                }
                tra.Commit();
                conn.Dispose();
                return strSql;
            }
            catch (Exception e)
            {
                tra.Rollback();
                conn.Dispose();
                throw new DataException(e.Message);
            }
        }

        public T QuerySingle<T>(string strSql)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;
            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }

                var data = conn.Query<T>(command.init()).FirstOrDefault();

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                return data;
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception(strSql + "\r\n" + e.Message);
            }
        }

        public List<T> Query<T>(string strSql)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;
            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                var data = conn.Query<T>(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                return data.ToList<T>();
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception(strSql + "\r\n" + e.Message);
            }
        }

        public int DeleteTableRow(string tableName, string strFilter)
        {
            string strSql = "delete " + tableName + _sRowLock + " where " + strFilter;

            SqlConnection conn = this._connection;

            //CommandDefinition comm = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }

                int ii = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                return ii;
            }
            catch
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                return -1;
            }
        }

        public object GetFirstColumn(string strSql)
        {
            DataSet ds = this.GetDataSet(strSql);

            if (ds.Tables.Count == 0)
                return "";

            if (ds.Tables[0].Rows.Count == 0)
                return "";

            return ds.Tables[0].Rows[0][0];
        }

        public object GetFirstColumn(string strSql, object param = null)
        {
            DataSet ds = this.GetDataSet(strSql, param);

            if (ds.Tables.Count == 0)
                return "";

            if (ds.Tables[0].Rows.Count == 0)
                return "";

            return ds.Tables[0].Rows[0][0];
        }

        //新增加方法
        public Object ExecuteScalar(string strSql)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;
            Object o = "";

            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                //o = conn.Query<object>(strSql).ToString();
                o = conn.ExecuteScalar<Object>(command.init());
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception(strSql + "\r\n" + e.Message);
            }
            return o;
        }

        public object GetFirstColumn(string strSql, List<DataField> listDataField)
        {
            DataSet ds = this.GetDataSet(strSql, listDataField);

            if (ds.Tables[0].Rows.Count == 0)
                return "";

            return ds.Tables[0].Rows[0][0];
        }

        public System.Data.DataSet GetDataSet(string strSql)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;
            DataSet ds = new DataSet();
            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                DataTable table = new DataTable("MyTable");
                using (var reader = conn.ExecuteReader(command.init()))
                { 
                    table.Load(reader);
                }
                ds.Tables.Add(table);

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                return ds;
            }
            catch(Exception ee)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                throw new Exception(strSql+"\r\n"+ee.Message);
            }
        }

        public System.Data.DataSet GetDataSet(string strSql, object param)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;
            command.Parameters = param;

            DataSet ds = new DataSet();
            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                DataTable table = new DataTable("MyTable");
                using (var reader = conn.ExecuteReader(command.init()))
                {
                    table.Load(reader);
                }
                ds.Tables.Add(table);

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                return ds;
            }
            catch (Exception ee)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                throw new Exception(strSql + "\r\n" + ee.Message);
            }
        }

        public System.Data.DataSet GetDataSet(string strSql, List<DataField> listDataField)
        {
            DataSet ds = new DataSet();
            SqlConnection conn = this._connection;

            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add("@" + dataField.DataFieldName, dataField.DataFieldValue);
            }

            command.Parameters = p;
            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }

                DataTable table = new DataTable("MyTable");
                using (var reader = conn.ExecuteReader(command.init()))
                {
                    table.Load(reader);
                }
                ds.Tables.Add(table);

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                return ds;
            }
            catch (Exception ee)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                throw new Exception(strSql + "\r\n" + ee.Message);
            }
        }
        public DataSet GetDataSet(string tableName, string strFilter)
        {
            string strSql = string.Format("select * from {0} where {1}", tableName, strFilter);

            return GetDataSet(strSql);
        }

        public DataSet GetDataSet(string tableName, string columnsList, string strFilter)
        {
            string strSql = string.Format("select {0} from {1} where {2}", columnsList, tableName, strFilter);

            return GetDataSet(strSql);
        }

        public DataSet GetDataSet(string tableName, string columnsList, string strFilter, string orderList)
        {
            string strSql = string.Format("select {0} from {1} where  {2} order by {3}", columnsList, tableName, strFilter, orderList);

            return GetDataSet(strSql);
        }

        public DataSet GetDataSet(string tableName, string columnsList, string strFilter, string orderList, ref string message)
        {
            string strSql = string.Format("select {0} from {1} where  {2} order by {3}", columnsList, tableName, strFilter, orderList);
            message = "";

            try
            {
                return GetDataSet(strSql);
            }
            catch (Exception ee)
            {
                message = ee.Message;
                return null;
            }
        }

        public bool JudgeColumnExist(string tableName, string columnName)
        {
            DataSet ds = GetTableColunms(tableName.ToUpper());
            for (int ii = 0; ii < ds.Tables[0].Rows.Count; ii++)
            {
                if (ds.Tables[0].Rows[ii][0].ToString().ToUpper() == columnName.ToUpper())
                    return true;
            }
            return false;
        }

        public bool JudgeRecordExist(string tableName, string strFilter)
        {
            string strSql = "select 1 from " + tableName + " where " + strFilter;
            DataSet ds = GetDataSet(strSql);

            if (ds.Tables.Count == 0)
            {
                return false;
            }

            if (ds.Tables[0].Rows.Count > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool JudgeTableOrViewExist(string tableName)
        {
            string strSql = string.Format("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND TABLE_NAME = '{0}'", tableName);
            DataSet ds = GetDataSet(strSql);
            if (ds.Tables[0].Rows.Count == 1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool AddData(string tableName, System.Collections.Hashtable ht)
        {
            if (ht.Count <= 0)
            {
                return false;
            }

            string strKey = "";
            string strValue = "";

            foreach (DictionaryEntry d in ht)
            {
                strKey += d.Key.ToString() + ",";
                strValue += "@" + d.Key.ToString() + ",";
            }

            strKey = strKey.Substring(0, strKey.Length - 1);
            strValue = strValue.Substring(0, strValue.Length - 1);

            string strSql = string.Format("insert into {0} {3} ({1}) values ({2})", tableName, strKey, strValue, _sRowLock);

            return this.ExecuteSql(strSql, tableName, ht);
        }

        public bool UpdateData(string tableName, Hashtable ht, string filterColumnsName)
        {
           string filter = GetFilterString(tableName, ht, filterColumnsName);
           return UpdateData2(tableName, ht, filter);
        }

        public bool UpdateData2(string tableName, Hashtable ht, string filter)
        {
            if (ht.Count <= 0)
            {
                return false;
            }

            string str = "";

            foreach (DictionaryEntry d in ht)
            {
                str += d.Key.ToString() + "= @" + d.Key.ToString() + ",";
            }
            str = str.Substring(0, str.Length - 1);

            string strSql = string.Format("update {0} {3} set {1} where {2}", tableName, str, filter, _sRowLock);

            return this.ExecuteSql(strSql, tableName, ht);
        }

        public bool SaveData(string tableName, System.Collections.Hashtable ht, string filterColumnsName)
        {
            if (ht.Count <= 0)
            {
                return false;
            }

            string filter = GetFilterString(tableName, ht, filterColumnsName);

            if (JudgeRecordExist(tableName, filter))
            {
                return UpdateData2(tableName, ht, filter);
            }
            else
            {
                return AddData(tableName, ht);
            }
        }

        private string GetFilterString(string tableName, Hashtable ht, string filterColumnName)
        {
            string filter = "";
            string[] str = filterColumnName.Split(new char[] { ',' });

            foreach (DictionaryEntry d in ht)
            {
                for (int ii = 0; ii < str.Length; ii++)
                {
                    if (d.Key.ToString() == str[ii])
                    {
                        if (d.Value == null || d.Value.ToString() == "")
                        {
                            throw new Exception("关键字段值为空！字段名称[" + d.Key + "]");
                        }

                        //对于加不加单引号，有测试反应加不加单引号效率更高，且不需要执行判断字段类型，减少逻辑复杂度，所以去掉 --willy
                        //string dataType = GetDataTypeByColumnName(tableName, d.Key.ToString());

                        //if (dataType == "int" || dataType == "bigint" || dataType == "smallint" || dataType == "tinyint" || dataType == "decimal" || dataType == "float" || dataType == "real" || dataType == "numeric")
                        //{
                        //    filter += "and " + d.Key.ToString() + "=" + d.Value.ToString() + " ";
                        //}
                        //else
                        //{
                            filter += "and " + d.Key.ToString() + "='" + d.Value.ToString() + "' ";
                        //}
                    }
                }
            }

            filter = filter.Remove(0, 3);

            return filter;
        }

        public bool AddData(string tableName, List<DataField> listDataField)
        {
            if (listDataField.Count <= 0)
            {
                return false;
            }

            string strKey = "";
            string strValue = "";

            foreach (DataField d in listDataField)
            {
                strKey += d.DataFieldName + ",";
                strValue += "@" + d.DataFieldName + ",";
            }
            strKey = strKey.Substring(0, strKey.Length - 1);
            strValue = strValue.Substring(0, strValue.Length - 1);

            string strSql = string.Format("insert into {0} {3} ({1}) values ({2})", tableName, strKey, strValue, _sRowLock);

            return this.ExecuteSql(strSql, tableName, listDataField);
        }

        public bool UpdateData(string tableName, List<DataField> listDataField, string filterColumnsName)
        {
            string filter = GetFilterString(tableName, listDataField, filterColumnsName);
            return UpdateData2(tableName, listDataField, filter);
        }

        public bool UpdateData2(string tableName, List<DataField> listDataField, string filter)
        {
            if (listDataField.Count <= 0)
            {
                return false;
            }

            string str = "";

            foreach (DataField d in listDataField)
            {
                str += d.DataFieldName + "= @" + d.DataFieldName + ",";
            }
            str = str.Substring(0, str.Length - 1);

            string strSql = string.Format("update {0} {3} set {1} where {2}", tableName, str, filter, _sRowLock);

            return this.ExecuteSql(strSql, tableName, listDataField);
        }

        public bool SaveData(string tableName, List<DataField> listDataField, string filterColumnsName)
        {
            if (listDataField.Count<=0)
            {
                return false;
            }

            string filter = GetFilterString(tableName, listDataField, filterColumnsName);

            if (JudgeRecordExist(tableName, filter))
            {
                return UpdateData2(tableName, listDataField, filter);
            }
            else
            {
                return AddData(tableName, listDataField);
            }
        }
        private string GetFilterString(string tableName, List<DataField> listDataField, string filterColumnName)
        {
            string filter = "";
            string[] str = filterColumnName.Split(new char[] { ',' });

            foreach (DataField d in listDataField)
            {
                for (int ii = 0; ii < str.Length; ii++)
                {
                    if (d.DataFieldName == str[ii])
                    {
                        if (d.DataFieldValue == null || d.DataFieldValue.ToString() == "")
                        {
                            throw new Exception("关键字段值为空！字段名称[" + d.DataFieldName + "]");
                        }
                        filter += "and " + d.DataFieldName + "='" + d.DataFieldValue.ToString() + "' ";
                    }
                }
            }

            filter = filter.Remove(0, 3);

            return filter;
        }

        public int SetID(string tableName, string columnName)
        {
            string strSql = "select top 1 " + columnName + " from " + tableName + " order by convert(int," + columnName + ") DESC";

            DataSet ds = GetDataSet(strSql);

            if (ds.Tables[0].Rows.Count > 0)
            {
                string aaa = ds.Tables[0].Rows[0][0].ToString();
                int ii = Convert.ToInt32(aaa);
                ii = ii + 1;
                return ii;
            }
            else
            {
                return 1;
            }
        }

        public int SetID(string tableName, string columnName, string strFilter)
        {
            string strSql = "select top 1 " + columnName + " from " + tableName + " where " + strFilter + " order by convert(int," + columnName + ") DESC";

            DataSet ds = GetDataSet(strSql);

            if (ds.Tables[0].Rows.Count > 0)
            {
                string aaa = ds.Tables[0].Rows[0][0].ToString();
                int ii = Convert.ToInt32(aaa);
                ii = ii + 1;
                return ii;
            }
            else
            {
                return 1;
            }
        }
        
        private bool ExecuteSql(string strSql, string tableName, Hashtable ht)
        {
            SqlConnection conn = _connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DictionaryEntry d in ht)
            {
                p.Add("@" + d.Key.ToString(), d.Value==null?"":d.Value.ToString());
            }
            command.Parameters = p;

            if (_transaction != null)
            {
                command.Transaction = _transaction;
            }
            else
            {
                Open();
            }
            try
            {

                int ii = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                if (ii >= 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ee)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception("错误语句：" + strSql + " " + ee.Message);
            }
        }
        private bool ExecuteSql(string strSql, string tableName, List<DataField> listDataField)
        {
            SqlConnection conn = _connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add("@" + dataField.DataFieldName, dataField.DataFieldValue);
            }
            command.Parameters = p;

            if (_transaction != null)
            {
                command.Transaction = _transaction;
            }
            else
            {
                Open();
            }
            try
            {
                int ii = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                if (ii >= 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ee)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception("错误语句：" + strSql + " " + ee.Message);
            }
        }

        private string GetDataTypeByColumnName(string tableName, string columnName)
        {
            string strSql = string.Format("select DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{0}' and COLUMN_NAME='{1}'", tableName, columnName);
            DataSet ds = GetDataSet(strSql);
            if (ds.Tables.Count == 1)
            {
                if (ds.Tables[0].Rows.Count == 1)
                {
                    return ds.Tables[0].Rows[0][0].ToString().ToLower();
                }
            }

            return "";
        }

        private string GetColumnLength(string tableName, string columnName)
        {
            string strSql = string.Format("select CHARACTER_MAXIMUM_LENGTH from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{0}' and COLUMN_NAME='{1}'", tableName, columnName);
            DataSet ds = GetDataSet(strSql);
            if (ds.Tables.Count == 1)
            {
                if (ds.Tables[0].Rows.Count == 1)
                {
                    return ds.Tables[0].Rows[0][0].ToString();
                }
            }
            return "";
        }

        public string GetSqlForPageSize(string tableName, string strKeyWord, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            return GetSqlForPageSize(tableName,strKeyWord, "t.*", PageSize, PageIndex, strWhere, strOrder);
        }

        public string GetSqlForPageSize(string tableName, string strKeyWord, string columnList, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            string strSql = "";

            if (PageIndex < 1)
            {
                strSql = "select top " + PageSize + " "+columnList+" from " + tableName + " t where " + strWhere + strOrder;
            }
            else
            {
                strSql = "select top " + PageSize + " " + columnList + " from " + tableName + " t where " + strKeyWord + " NOT IN (select top " + PageIndex * PageSize + " " + strKeyWord + " from " + tableName + " t where " + strWhere + strOrder + ") and" + strWhere + strOrder;
            }

            return strSql;
        }

        public System.Data.DataSet GetTableColunms(string tableName)
        {
            string strSql = string.Format("select  COLUMN_NAME,DATA_TYPE,CHARACTER_OCTET_LENGTH,ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{0}'", tableName);
            return GetDataSet(strSql);
        }


        #endregion

        #region IDisposable 成员

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.Close();
                this._connection.Dispose();
            }
        }

        #endregion

        #region IDbAccess 成员


        public string GetPrimaryKey(string tableName)
        {
            string strSql = "EXEC sp_pkeys @table_name='" + tableName + "'";

            DataSet ds = GetDataSet(strSql);

            string str = "";

            for (int ii = 0; ii < ds.Tables[0].Rows.Count; ii++)
            {
                str += "," + ds.Tables[0].Rows[ii]["COLUMN_NAME"].ToString();
            }

            if (str.Length > 0)
                str = str.Remove(0, 1);

            return str;
        }

        public IDataReader GetDataReader(string strSql)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }

                IDataReader dr = conn.ExecuteReader(command.init());

                return dr;
            }
            catch (Exception ee)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                throw new Exception(strSql+"\r\n"+ee.Message);
            }
        }
        #endregion

        //DataTable快速插入表，这个没有使用Dapper.net改造，暂时没有找到快速，且效率高的方式替换，保持接口统一沿用--willy
        public bool BulkToDB(DataTable dt, string sTableName)
        {
            if (dt.Rows.Count==0)
            {
                return false;
            }
            SqlConnection conn = this._connection;
            SqlBulkCopy bulkCopy = new SqlBulkCopy(this.sConnection, SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints);
            if (_transaction != null)
            {
                bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints, _transaction);
            }
            else
            {
                Open();
            }
            
            bulkCopy.DestinationTableName = sTableName;
            bulkCopy.BatchSize = dt.Rows.Count;
            
            try
            {
                if (dt != null && dt.Rows.Count != 0)
                {
                    foreach (DataColumn dc in dt.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                    }
                }
                bulkCopy.WriteToServer(dt);
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                if (bulkCopy != null)
                    bulkCopy.Close();
            }
        }

        public double GetTablePhySize(string tableName)
        {
            string strSql = " sp_spaceused " + tableName;
            DataSet ds = GetDataSet(strSql);

            if (ds.Tables.Count == 0)
                return 0;
            if (ds.Tables[0].Rows.Count == 0)
                return 0;

            double d1 = Convert.ToDouble(ds.Tables[0].Rows[0]["data"].ToString().ToLower().Replace("kb", "").Trim());
            double d2 = Convert.ToDouble(ds.Tables[0].Rows[0]["index_size"].ToString().ToLower().Replace("kb", "").Trim());

            return d1 + d2;
        }


        public DataTable GetTableName()
        {
            string strSql = "select name as TABLE_NAME from sysobjects where xtype='U' and  name<>'dtproperties' order by name ";

            DataTable dt = GetDataSet(strSql).Tables[0];

            return dt;
        }

        public DataTable GetViewName()
        {
            string strSql = "select TABLE_NAME from INFORMATION_SCHEMA.VIEWS order by TABLE_NAME";

            DataTable dt = GetDataSet(strSql).Tables[0];

            return dt;
        }

        public TableData GetTableData(string tableName)
        {
            TableInfo ti = GetTableInfo(tableName);
            TableData td = new TableData();
            td.TableDescription = ti.TableDescription;
            td.TableName = ti.TableName;
            td.DataBaseType = ti.DataBaseType;
            td.Columns = ti.Columns;

            string strSql = "select * from " + tableName;
            DataTable dt = this.GetDataSet(strSql).Tables[0];
            dt.TableName = "ROWS";
            td.Data = dt;

            return td;
        }

        public TableInfo GetTableInfo(string tableName)
        {
            TableInfo ti = new TableInfo();
            ti.DataBaseType = "SQLServer";
            ti.TableName = tableName.ToUpper();
            string strSql = "SELECT 表名= case when a.colorder=1 then d.name else '' end,表说明= case when a.colorder=1 then isnull(f.value,'') else '' end, 字段序号   = a.colorder, 字段名     = a.name,标识       = case when COLUMNPROPERTY( a.id,a.name,'IsIdentity')=1 then '1'else '0' end,主键       = case when exists(SELECT 1 FROM sysobjects where xtype='PK' and parent_obj=a.id and name in (SELECT name FROM sysindexes WHERE indid in(SELECT indid FROM sysindexkeys WHERE id = a.id AND colid=a.colid))) then '1' else '0' end,类型       = b.name,占用字节数 = a.length,长度       = COLUMNPROPERTY(a.id,a.name,'PRECISION'),小数位数   = isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0),允许空     = case when a.isnullable=1 then '1'else '0' end, 默认值     = isnull(e.text,''), 字段说明   = isnull(g.[value],'') FROM syscolumns a left join  systypes b on   a.xusertype=b.xusertype inner join  sysobjects d on a.id=d.id  and d.xtype='U' and  d.name<>'dtproperties' left join  syscomments e on     a.cdefault=e.id left join sysproperties g on     a.id=g.id and a.colid=g.smallid  left join  sysproperties f on     d.id=f.id and f.smallid=0 where   d.name='" + ti.TableName + "'   order by  a.id,a.colorder ";
            DataSet ds = new DataSet();
            try
            {
                ds = GetDataSet(strSql);
            }
            catch
            {
                strSql = "SELECT 表名= case when a.colorder=1 then d.name else '' end,表说明= case when a.colorder=1 then isnull(f.value,'') else '' end, 字段序号   = a.colorder, 字段名     = a.name,标识       = case when COLUMNPROPERTY( a.id,a.name,'IsIdentity')=1 then '1'else '0' end,主键       = case when exists(SELECT 1 FROM sysobjects where xtype='PK' and parent_obj=a.id and name in (SELECT name FROM sysindexes WHERE indid in(SELECT indid FROM sysindexkeys WHERE id = a.id AND colid=a.colid))) then '1' else '0' end,类型       = b.name,占用字节数 = a.length,长度       = COLUMNPROPERTY(a.id,a.name,'PRECISION'),小数位数   = isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0),允许空     = case when a.isnullable=1 then '1'else '0' end, 默认值     = isnull(e.text,''), 字段说明   = isnull(g.[value],'') FROM syscolumns a left join  systypes b on   a.xusertype=b.xusertype inner join  sysobjects d on a.id=d.id  and d.xtype='U' and  d.name<>'dtproperties' left join  syscomments e on     a.cdefault=e.id left join sys.extended_properties g on     a.id=g.major_id and a.colid=g.minor_id  left join  sys.extended_properties f on     d.id=f.major_id and f.minor_id=0 where   d.name='" + ti.TableName + "'   order by  a.id,a.colorder ";
                ds = GetDataSet(strSql);
            }

            if (ds.Tables[0].Rows.Count > 0)
            {
                ti.TableDescription = ds.Tables[0].Rows[0]["表说明"].ToString();

                ColumnInfo[] ci = new ColumnInfo[ds.Tables[0].Rows.Count];

                for (int ii = 0; ii < ds.Tables[0].Rows.Count; ii++)
                {
                    ci[ii].Name = ds.Tables[0].Rows[ii]["字段名"].ToString();
                    ci[ii].Type = ds.Tables[0].Rows[ii]["类型"].ToString();
                    ci[ii].Length = Convert.ToInt32(ds.Tables[0].Rows[ii]["长度"]);
                    ci[ii].Serial = Convert.ToInt32(ds.Tables[0].Rows[ii]["字段序号"]);
                    ci[ii].Remark = ds.Tables[0].Rows[ii]["字段说明"].ToString();
                    if (ds.Tables[0].Rows[ii]["允许空"].ToString() == "1")
                        ci[ii].IsNull = true;
                    else
                        ci[ii].IsNull = false;

                    if (ds.Tables[0].Rows[ii]["主键"].ToString() == "1")
                        ci[ii].IsKey = true;
                    else
                        ci[ii].IsKey = false;

                    if (ds.Tables[0].Rows[ii]["标识"].ToString() == "1")
                        ci[ii].IsIdentity = true;
                    else
                        ci[ii].IsIdentity = false;

                    ci[ii].DefauleValue = ds.Tables[0].Rows[ii]["默认值"].ToString();
                }

                ti.Columns = ci;
            }

            return ti;
        }

        /// <summary>
        /// 转化字段类型
        /// </summary>
        /// <param name="dbType"></param>
        /// <returns></returns>
        private SqlDbType ConvertDbType(DataColunmType dbType)
        {
            SqlDbType sqlDbType = SqlDbType.VarChar;
            switch (dbType)
            {
                case DataColunmType.BIGINT:
                    sqlDbType = SqlDbType.BigInt;
                    break;
                case DataColunmType.INT:
                    sqlDbType = SqlDbType.Int;
                    break;
                case DataColunmType.SMALLINT:
                    sqlDbType = SqlDbType.SmallInt;
                    break;
                case DataColunmType.TINYINT:
                    sqlDbType = SqlDbType.TinyInt;
                    break;
                case DataColunmType.DECIMAL:
                    sqlDbType = SqlDbType.Decimal;
                    break;
                case DataColunmType.FLOAT:
                    sqlDbType = SqlDbType.Float;
                    break;
                case DataColunmType.REAL:
                    sqlDbType = SqlDbType.Real;
                    break;
                case DataColunmType.DATETIME:
                    sqlDbType = SqlDbType.DateTime;
                    break;
                case DataColunmType.SMALLDATETIME:
                    sqlDbType = SqlDbType.SmallDateTime;
                    break;
                case DataColunmType.CHAR:
                    sqlDbType = SqlDbType.Char;
                    break;
                case DataColunmType.NCHAR:
                    sqlDbType = SqlDbType.NChar;
                    break;
                case DataColunmType.VARCHAR:
                    sqlDbType = SqlDbType.VarChar;
                    break;
                case DataColunmType.NVARCHAR:
                    sqlDbType = SqlDbType.NVarChar;
                    break;
                case DataColunmType.TEXT:
                    sqlDbType = SqlDbType.Text;
                    break;
                case DataColunmType.NTEXT:
                    sqlDbType = SqlDbType.NText;
                    break;
                case DataColunmType.BINARY:
                    sqlDbType = SqlDbType.Binary;
                    break;
                case DataColunmType.VARBINARY:
                    sqlDbType = SqlDbType.VarBinary;
                    break;
                case DataColunmType.IMAGE:
                    sqlDbType = SqlDbType.Image;
                    break;
                default:
                    break;
            }
            return sqlDbType;
        }

        public int ExecuteNonQuery(string strProName)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strProName;
            command.CommandType = CommandType.StoredProcedure;


            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                int aa = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                return aa;
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception("存储过程" + strProName + "执行失败。\r\n" + e.Message);
            }
        }

        public int ExecuteNonQuery(string strProName, DynamicParameters parems)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strProName;
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters = parems;

            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                int aa = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                return aa;
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception("存储过程" + strProName + "执行失败。\r\n" + e.Message);
            }
        }

        public int ExecuteNonQuery(string strProName, List<DataField> listDataField)
        {
            SqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strProName;
            command.CommandType = CommandType.StoredProcedure;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                SqlParameter paraConver = new SqlParameter();
                paraConver.SqlDbType = ConvertDbType(dataField.DataFieldType);
                p.Add("@" + dataField.DataFieldName, dataField.DataFieldValue, paraConver.DbType, dataField.ParameterDirType);
            }

            command.Parameters = p;

            try
            {
                if (_transaction != null)
                {
                    command.Transaction = _transaction;
                }
                else
                {
                    Open();
                }
                int aa = conn.Execute(command.init());

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                return aa;
            }
            catch (Exception e)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }
                throw new Exception("存储过程" + strProName + "执行失败。\r\n" + e.Message);
            }
        }

    }
}
