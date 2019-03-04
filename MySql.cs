using Dapper;
using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using ZondyDBHelper;

namespace ZondyDBHelper
{
    internal class MySql : IDbAccess, IDisposable
    {
        private bool _alwaysOpen;
        private MySqlConnection _connection;
        private MySqlTransaction _transaction;
        private string sConnection;
        private IsolationLevel _IsolationLevel = IsolationLevel.ReadCommitted;
        private CommandDefinitionNew command;

        public MySql(string connectionString)
        {
            sConnection = connectionString;
            _connection = new MySqlConnection(connectionString);
            command = new CommandDefinitionNew();
        }
        
        #region IDbAccess 成员

        public void BeginTrans()
        {
            this.Open();
            if (_IsolationLevel == IsolationLevel.ReadCommitted)
            {
                this._transaction = this._connection.BeginTransaction();
            }
            else
            {
                this._transaction = this._connection.BeginTransaction(_IsolationLevel);
            }
        }

        public void Close()
        {
            this._connection.Close();
        }

        private void CloseCommandConnection(IDbCommand dbCommand)
        {
            dbCommand.Connection.Close();
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
                return false;
            }
            set
            {
            }
        }
        public IsolationLevel IsolationLevel
        {
            set { this._IsolationLevel = value; }
        }

        public string Connection
        {
            get { return sConnection; }
        }

        public DataBaseType DataBaseType
        {
            get { return DataBaseType.MYSQL; }
        }

        public bool ExecuteSql(string strSql)
        {
            MySqlConnection conn = this._connection;
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
            MySqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add("?" + dataField.DataFieldName, dataField.DataFieldValue);
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
            MySqlConnection conn = this._connection;
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
            MySqlConnection conn = this._connection;
            MySqlTransaction tra;

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
            MySqlConnection conn = this._connection;
            MySqlTransaction tra;

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
            MySqlConnection conn = this._connection;
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
            MySqlConnection conn = this._connection;
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
            string strSql = "delete " + tableName + " where " + strFilter;

            MySqlConnection conn = this._connection;
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
            MySqlConnection conn = this._connection;
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

        public DataSet GetDataSet(string strSql)
        {
            MySqlConnection conn = this._connection;
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
            catch (Exception ee)
            {
                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                throw new Exception(strSql + "\r\n" + ee.Message);
            }
        }

        public DataSet GetDataSet(string strSql,object param)
        {
            MySqlConnection conn = this._connection;
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

        public DataSet GetDataSet(string strSql, List<DataField> listDataField)
        {
            DataSet ds = new DataSet();
            MySqlConnection conn = this._connection;

            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add("?" + dataField.DataFieldName, dataField.DataFieldValue);
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
            string strSql = string.Format("select 1 from {0} where {1}", tableName, strFilter);
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
            string strSql = string.Format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}' ", FormatTableName(tableName));
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
                strValue += "?" + d.Key.ToString() + ",";
            }
            strKey = strKey.Substring(0, strKey.Length - 1);
            strValue = strValue.Substring(0, strValue.Length - 1);

            string strSql = string.Format("insert into {0} ({1}) values ({2})", tableName, strKey, strValue);

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
                str += d.Key.ToString() + "= ?" + d.Key.ToString() + ",";
            }

            str = str.Substring(0, str.Length - 1);

            string strSql = string.Format("update {0} set {1} where {2}", tableName, str, filter);

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

            for (int ii = 0; ii < str.Length; ii++)
            {
                if (ht.ContainsKey(str[ii]))
                {
                    if (ht[str[ii]] == null || ht[str[ii]].ToString() == "")
                    {
                        throw new Exception("关键字段值为空！字段名称[" + str[ii] + "]");
                    }

                    filter += "and " + str[ii] + "='" + ht[str[ii]].ToString() + "' ";
                    
                }
            }

            filter = filter.Remove(0, 3);

            return filter;
        }

        private bool ExecuteSql(string strSql, string tableName, Hashtable ht)
        {
            MySqlConnection conn = _connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DictionaryEntry d in ht)
            {
                p.Add("?" + d.Key.ToString(), d.Value.ToString());
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

        public bool AddData(string tableName, List<DataField> listDataField)
        {
            string strKey = "";
            string strValue = "";

            if (listDataField.Count <= 0)
            {
                return false;
            }

            foreach (DataField d in listDataField)
            {
                strKey += d.DataFieldName + ",";
                strValue += "?" + d.DataFieldName + ",";
            }
            strKey = strKey.Substring(0, strKey.Length - 1);
            strValue = strValue.Substring(0, strValue.Length - 1);

            string strSql = string.Format("insert into {0} ({1}) values ({2})", tableName, strKey, strValue);

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
                str += d.DataFieldName + "= ?" + d.DataFieldName + ",";
            }

            str = str.Substring(0, str.Length - 1);

            string strSql = string.Format("update {0} set {1} where {2}", tableName, str, filter);

            return this.ExecuteSql(strSql, tableName, listDataField);
        }

        public bool SaveData(string tableName, List<DataField> listDataField, string filterColumnsName)
        {
            if (listDataField.Count <= 0)
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

        private bool ExecuteSql(string strSql, string tableName, List<DataField> listDataField)
        {
            MySqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add("?" + dataField.DataFieldName, dataField.DataFieldValue);
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

        public int SetID(string tableName, string columnName)
        {
            return SetID(tableName, columnName, " 1=1 ");
        }

        public int SetID(string tableName, string columnName, string strFilter)
        {
            string strSql = "select * from (select " + columnName + " from " + tableName + " where  " + columnName + " IS NOT NULL AND " + strFilter + " order by to_number(" + columnName + ") DESC) where rownum=1";

            DataSet ds = new DataSet();
            try
            {
                ds = GetDataSet(strSql);
            }
            catch (Exception ee)
            {
                throw new Exception("错误语句：" + strSql + " " + ee.Message);
            }

            if (ds.Tables[0].Rows.Count > 0)
            {
                string aaa = ds.Tables[0].Rows[0][0].ToString();

                if (aaa == "")
                    return 1;

                int ii = Convert.ToInt32(aaa);
                ii = ii + 1;
                return ii;
            }
            else
            {
                return 1;
            }
        }

        public string GetSqlForPageSize(string tableName, string strKeyWord, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            return GetSqlForPageSize(tableName, strKeyWord, "t.*", PageSize, PageIndex, strWhere, strOrder);
        }

        public string GetSqlForPageSize(string tableName, string strKeyWord, string columnList, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            string strSql = "";

            strSql = string.Format("select {0} from {1} t where {4} {5} limit {2},{3} ", columnList, tableName, PageIndex * PageSize, PageSize, strWhere, strOrder);

            return strSql;
        }

        protected string FormatTableName(string str)
        {
            return str.Replace("\"", "").Trim().ToUpper();
        }

        public DataSet GetTableColunms(string tableName)
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
            string strSql = "   SELECT" +
                " k.column_name " +
                "FROM information_schema.table_constraints t " +
                "JOIN information_schema.key_column_usage k USING(" +
                "constraint_name,table_schema,table_name) " +
                "WHERE t.constraint_type = 'PRIMARY KEY' " +
                "AND t.table_name = 'SYSUSER'";

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
            MySqlConnection conn = this._connection;
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

                throw new Exception(strSql + "\r\n" + ee.Message);
            }
        }
        #endregion

        private string GetDataTypeByColumnName(string tableName, string columnName)
        {
            string strSql = string.Format("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' AND COLUMN_NAME='{1}'", FormatTableName(tableName), columnName.ToUpper());

            DataSet ds = GetDataSet(strSql);
            if (ds.Tables.Count == 1)
            {
                if (ds.Tables[0].Rows.Count > 0)
                {
                    return ds.Tables[0].Rows[0][0].ToString();
                }
            }

            return "";
        }

        public bool BulkToDB(DataTable dt, string sTableName)
        {
            if (dt.Rows.Count == 0) return false;
            int insertCount = 0;
            using (MySqlConnection conn = this._connection)
            {
                MySqlTransaction tran = null;
                try
                {
                    conn.Open();
                    tran = conn.BeginTransaction();
                    MySqlBulkLoader bulk = new MySqlBulkLoader(conn)
                    {
                        FieldTerminator = ",",
                        FieldQuotationCharacter = '"',
                        EscapeCharacter = '"',
                        LineTerminator = "\r\n",
                        NumberOfLinesToSkip = 0,
                        TableName = sTableName,
                    };
                    bulk.Columns.AddRange(dt.Columns.Cast<DataColumn>().Select(colum => colum.ColumnName).ToList());
                    insertCount = bulk.Load();
                    tran.Commit();
                }
                catch (MySqlException ex)
                {
                    if (tran != null) tran.Rollback();
                    throw ex;
                }
            }
            return true;
        }

        public DataTable GetTableName()
        {
            string strSql = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES  order by  TABLE_NAME";

            DataTable dt = GetDataSet(strSql).Tables[0];

            return dt;
        }

        public DataTable GetViewName()
        {
            string strSql = "SELECT  TABLE_NAME  FROM  INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='test'";

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
            ti.TableName = tableName.ToUpper();
            ti.DataBaseType = "MYSQL";
            string strSql = "SELECT ORDINAL_POSITION AS 字段序号,column_name AS 字段名称,"+
                            "data_type AS 字段类型,character_MAXIMUM_length AS 字段长度,"+
                            "NUMERIC_precision - NUMERIC_SCALE AS 整数位, NUMERIC_SCALE AS 小数位,"+
                            "IS_NULLABLE AS 允许空, COLUMN_DEFAULT AS 默认值, COLUMN_KEY AS 主键,"+
                            "(SELECT TABLE_COMMENT  FROM information_schema.`TABLES` s WHERE s.table_name = t.table_name  ) AS 表说明 "+
                            "FROM information_schema.`COLUMNS` t WHERE t.table_name =  '" + ti.TableName + "' order by ORDINAL_POSITION";

            DataSet ds = GetDataSet(strSql);

            if (ds.Tables[0].Rows.Count > 0)
            {
                ti.TableDescription = ds.Tables[0].Rows[0]["表说明"].ToString();

                ColumnInfo[] ci = new ColumnInfo[ds.Tables[0].Rows.Count];

                for (int ii = 0; ii < ds.Tables[0].Rows.Count; ii++)
                {
                    ci[ii].Name = ds.Tables[0].Rows[ii]["字段名称"].ToString();
                    ci[ii].Type = ds.Tables[0].Rows[ii]["字段类型"].ToString();
                    ci[ii].Length = Convert.ToInt32(ds.Tables[0].Rows[ii]["字段长度"]);
                    ci[ii].Serial = Convert.ToInt32(ds.Tables[0].Rows[ii]["字段序号"]);
                    ci[ii].Remark = ds.Tables[0].Rows[ii]["说明"].ToString();

                    if (ds.Tables[0].Rows[ii]["允许空"].ToString() == "YES")
                        ci[ii].IsNull = true;
                    else
                        ci[ii].IsNull = false;

                    if (ds.Tables[0].Rows[ii]["主键"].ToString() == "PRI")
                        ci[ii].IsKey = true;
                    else
                        ci[ii].IsKey = false;

                    ci[ii].DefauleValue = ds.Tables[0].Rows[ii]["默认值"].ToString();
                    if (ds.Tables[0].Rows[ii]["整数位"].ToString() != "")
                        ci[ii].Precision = Convert.ToInt32(ds.Tables[0].Rows[ii]["整数位"]);
                    if (ds.Tables[0].Rows[ii]["小数位"].ToString() != "")
                        ci[ii].Scale = Convert.ToInt32(ds.Tables[0].Rows[ii]["小数位"]);
                }

                ti.Columns = ci;
            }

            return ti;
        }

        public int ExecuteNonQuery(string strProName)
        {
            MySqlConnection conn = this._connection;
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

        public int ExecuteNonQuery(string strProName, List<DataField> listDataField)
        {
            MySqlConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strProName;
            command.CommandType = CommandType.StoredProcedure;

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
            MySqlConnection conn = this._connection;
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
    }
}
