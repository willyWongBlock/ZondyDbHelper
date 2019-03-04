using Dapper;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using ZondyDBHelper;

namespace ZondyDBHelper
{
    internal class Oracle : IDbAccess, IDisposable 
    {
        private bool _alwaysOpen;
        private OracleConnection _connection;
        private OracleTransaction _transaction;
        private string sConnection;
        private IsolationLevel _IsolationLevel = IsolationLevel.ReadCommitted;
        private CommandDefinitionNew command;

        public Oracle(string connectionString)
        {
            sConnection = connectionString;
            _connection = new OracleConnection(connectionString);
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
            get { return DataBaseType.ORACLE; }
        }

        public bool ExecuteSql(string strSql)
        {
            OracleConnection conn = this._connection;
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
            OracleConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add(":" + dataField.DataFieldName, dataField.DataFieldValue);
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
            OracleConnection conn = this._connection;
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
            OracleConnection conn = this._connection;
            OracleTransaction tra;

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
            OracleConnection conn = this._connection;
            OracleTransaction tra;

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
            OracleConnection conn = this._connection;
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
            OracleConnection conn = this._connection;
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

            OracleConnection conn = this._connection;
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
            DataSet ds = this.GetDataSet(strSql,param);

            if (ds.Tables.Count == 0)
                return "";

            if (ds.Tables[0].Rows.Count == 0)
                return "";

            return ds.Tables[0].Rows[0][0];
        }

        //新增加方法
        public Object ExecuteScalar(string strSql)
        {
            OracleConnection conn = this._connection;
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
            OracleConnection conn = this._connection;
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

        public DataSet GetDataSet(string strSql, object param)
        {
            OracleConnection conn = this._connection;
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
            OracleConnection conn = this._connection;

            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add(":" + dataField.DataFieldName, dataField.DataFieldValue);
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
            catch(Exception ee)
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
            string strSql = string.Format("select 1 from user_objects where object_type in ('TABLE','VIEW') AND OBJECT_NAME='{0}'", FormatTableName(tableName));
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

        private string GetDataTypeByColumnName(string tableName, string columnName)
        {
            string strSql = string.Format("select DATA_TYPE from user_tab_cols where table_name='{0}' and COLUMN_NAME='{1}'", FormatTableName(tableName), columnName.ToUpper());
            
            if (tableName.Contains("."))
            {
                strSql = string.Format("select DATA_TYPE from all_tab_columns where owner='" + tableName.Split('.')[0].ToUpper()
                    + "' AND table_name='{0}' and COLUMN_NAME='{1}'", FormatTableName(tableName.Split('.')[1].Trim().ToUpper()), columnName.ToUpper());
            }

            DataSet ds = GetDataSet(strSql);
            if (ds.Tables.Count == 1)
            {
                if (ds.Tables[0].Rows.Count >0 )
                {
                    return ds.Tables[0].Rows[0][0].ToString();
                }
            }

            return "";
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
                JudgeColumnLength(d.Key.ToString(), "表" + tableName + "的字段" + d.Key.ToString() + "名称过长，不能超过27个字符");

                strKey += d.Key.ToString() + ",";
                strValue += ":" + d.Key.ToString() + ",";
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
                JudgeColumnLength(d.Key.ToString(), "表" + tableName + "的字段" + d.Key.ToString() + "名称过长，不能超过27个字符");
                
                str += d.Key.ToString() + "= :" + d.Key.ToString() + ",";
            }

            str = str.Substring(0, str.Length - 1);

            string strSql = string.Format("update {0} set {1} where {2}", tableName, str, filter);

            return this.ExecuteSql(strSql, tableName, ht);
        }

        private void JudgeColumnLength(string sName,string sMessage)
        {
            if (sName.Length > 27)
            {
                throw new Exception(sMessage);
            }
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
            //string filter = "";
            //string[] str = filterColumnName.Split(new char[] { ',' });

            //for (int ii = 0; ii < str.Length; ii++)
            //{
            //    if (ht.ContainsKey(str[ii]))
            //    {
            //        if (ht[str[ii]] == null || ht[str[ii]].ToString() == "")
            //        {
            //            throw new Exception("关键字段值为空！字段名称[" + str[ii] + "]");
            //        }

            //        string dataType = GetDataTypeByColumnName(tableName, str[ii]);

            //        if (dataType == "NUMBER")
            //        {
            //            filter += "and " + str[ii] + "=" + ht[str[ii]].ToString() + " ";
            //        }
            //        else if (dataType == "DATE")
            //        {
            //            filter += "and " + str[ii] + "=to_date('" + ht[str[ii]].ToString() + "','yyyy-mm-dd hh24:mi:ss') ";
            //        }
            //        else
            //        {
            //            filter += "and " + str[ii] + "='" + ht[str[ii]].ToString() + "' ";
            //        }
            //    }
            //}

            //filter = filter.Remove(0, 3);

            //return filter;

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
                    filter += "and " + str[ii] + "= :" + str[ii] + " ";
                    
                }
            }

            filter = filter.Remove(0, 3);

            return filter;

        }

        private bool ExecuteSql(string strSql, string tableName, Hashtable ht)
        {
            OracleConnection conn = this._connection;
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DictionaryEntry d in ht)
            {
                p.Add(":" + d.Key.ToString(), d.Value.ToString());
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

            if (listDataField.Count<=0)
            {
                return false;
            }

            foreach (DataField d in listDataField)
            {
                JudgeColumnLength(d.DataFieldName, "表" + tableName + "的字段" + d.DataFieldName + "名称过长，不能超过27个字符");

                strKey += d.DataFieldName + ",";
                strValue += ":" + d.DataFieldName + ",";
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
                JudgeColumnLength(d.DataFieldName, "表" + tableName + "的字段" + d.DataFieldName + "名称过长，不能超过27个字符");

                str += d.DataFieldName + "= :" + d.DataFieldName + ",";
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
                        if (d.DataFieldType == DataColunmType.DATETIME || d.DataFieldType == DataColunmType.SMALLDATETIME)
                        {
                            filter += "and " + d.DataFieldName + "=to_date('" + d.DataFieldValue.ToString() + "','yyyy-mm-dd hh24:mi:ss') ";
                        }
                        else
                        {
                            filter += "and " + d.DataFieldName + "='" + d.DataFieldValue.ToString() + "' ";
                        }
                    }
                }
            }
            filter = filter.Remove(0, 3);

            return filter;
        }

        private bool ExecuteSql(string strSql, string tableName, List<DataField> listDataField)
        {
            OracleConnection conn = this._connection;
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
            catch(Exception ee)
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
            string strSql = "";
            StringBuilder sb = new StringBuilder();
            sb.Append("select * from (select t.*,rownum rno ");
            sb.Append(" from (select * from " + tableName + " ");
            sb.Append(" where " + strWhere);
            //sb.Append("select * from " + tableName);
            //sb.Append("  where rowid in (select rid from (select rownum rno, rid ");
            //sb.Append(" from (select rowid rid from " + tableName + " t ");
            //sb.Append(" where " + strWhere);
            sb.Append(strOrder);
            sb.Append(")t where rownum <= ");
            sb.Append(((PageIndex + 1) * PageSize).ToString(System.Globalization.NumberFormatInfo.CurrentInfo));//页码从零开始
            sb.Append(" )");
            sb.Append(" where rno >= ");
            sb.Append((PageIndex * PageSize + 1).ToString(System.Globalization.NumberFormatInfo.CurrentInfo));
            //sb.Append(") ");
            sb.Append(strOrder);
            strSql = sb.ToString();

            return strSql;
        }

        public string GetSqlForPageSize(string tableName, string strKeyWord, string columnList, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            string strSql = "";
            StringBuilder sb = new StringBuilder();
            sb.Append("select " + columnList + " from (select t.*,rownum rno ");
            sb.Append(" from (select * from " + tableName + " ");
            sb.Append(" where " + strWhere);
            //sb.Append("select * from " + tableName);
            //sb.Append("  where rowid in (select rid from (select rownum rno, rid ");
            //sb.Append(" from (select rowid rid from " + tableName + " t ");
            //sb.Append(" where " + strWhere);
            sb.Append(strOrder);
            sb.Append(")t where rownum <= ");
            sb.Append(((PageIndex + 1) * PageSize).ToString(System.Globalization.NumberFormatInfo.CurrentInfo));//页码从零开始
            sb.Append(" )");
            sb.Append(" where rno >= ");
            sb.Append((PageIndex * PageSize + 1).ToString(System.Globalization.NumberFormatInfo.CurrentInfo));
            //sb.Append(") ");
            sb.Append(strOrder);
            strSql = sb.ToString();

            return strSql;
        }

        protected string FormatTableName(string str)
        {
            return str.Replace("\"", "").Trim().ToUpper();
        }

        public DataSet GetTableColunms(string tableName)
        {
            string strSql = string.Format("select {0} from user_tab_cols where table_name='{1}' order by COLUMN_ID", "COLUMN_NAME,DATA_TYPE,DATA_LENGTH,COLUMN_ID", FormatTableName(tableName));
            return GetDataSet(strSql);
        }

        public string GetCreateColumnSql(string tableName, ColumnInfo ci)
        {
            string str = "ALTER TABLE " + tableName + " ADD " + ci.Name + " " + ci.Type;

            if (ci.Type.ToUpper() == "VARCHAR2" || ci.Type.ToUpper() == "NVARCHAR2")
                str += "(" + ci.Length + ") ";

            if (ci.DefauleValue != null && ci.DefauleValue != "")
                str += " default " + ci.DefauleValue;

            if (!ci.IsNull)
                str += " not null ";

            return str;
        }

        public string GetCreateTableSql(string tableName, ColumnInfo[] ci)
        {
            string sColumn = "";
            string sPrimary = "";
            for (int ii = 0; ii < ci.Length; ii++)
            {
                ColumnInfo cc = ci[ii];
                sColumn += ",";
                sColumn += cc.Name + " " + cc.Type;
                if (cc.Type.ToUpper() == "VARCHAR2" || cc.Type.ToUpper() == "NVARCHAR2")
                    sColumn += "(" + cc.Length + ") ";

                if (cc.Type.ToUpper() == "NUMBER" && cc.Precision > 0)
                    sColumn += "(" + cc.Precision + "," + cc.Scale + ") ";

                if (cc.DefauleValue != null && cc.DefauleValue != "")
                    sColumn += " default " + cc.DefauleValue;

                if (!cc.IsNull)
                    sColumn += " not null ";

                if (cc.IsKey)
                    sPrimary += "," + cc.Name;
            }
            sColumn = sColumn.Remove(0, 1);

            sColumn = "create table " + tableName + "(" + sColumn + ")";
            if (sPrimary.Length > 1)
            {
                sPrimary = sPrimary.Remove(0, 1);
                sColumn += ";alter table " + tableName + " add primary key (" + sPrimary + ")";
            }
            return sColumn;
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
            string strSql = "select   column_name   from   user_cons_columns  where constraint_name in (select index_name from user_constraints where constraint_type='P' and table_name='"+tableName+"') ";

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
            OracleConnection conn = this._connection;
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

        public string GetDataBaseUser()
        {
            string str = _connection.ConnectionString;
            int ii = str.ToUpper().IndexOf("USER=");
             str = str.Substring(ii + 5);
            if (ii == -1)
            {
                ii = str.ToUpper().IndexOf("USER ID=");
                str = str.Substring(ii + 8);
            }
           
            ii = str.IndexOf(";");
            str = str.Remove(ii);
            return str.ToUpper();
        }


        public double GetTablePhySize(string tableName)
        {
            string strSql = "select SEGMENT_NAME,INDEX_NAME from DBA_LOBS t where table_name='"+tableName+"' and t.owner = '" + GetDataBaseUser() + "' ";
            DataSet ds = GetDataSet(strSql);

            if (ds.Tables.Count == 0)
                return 0;

            string sFilter = "'"+tableName+"'";
            if (ds.Tables[0].Rows.Count > 0)
            {
                sFilter += ",'" + ds.Tables[0].Rows[0]["SEGMENT_NAME"].ToString() + "','" + ds.Tables[0].Rows[0]["INDEX_NAME"].ToString() + "'";
            }

            strSql = "select sum(bytes)/1024 from dba_segments where segment_name in (" + sFilter + ") and owner = '" + GetDataBaseUser() + "'";

            string str = GetFirstColumn(strSql).ToString();

            if (str == "")
                return 0;

            return Convert.ToDouble(str);
        }

        public bool BulkToDB(DataTable dt, string sTableName)
        {
            if (dt.Rows.Count == 0)
            {
                return false;
            }
            string strKey = "";
            string strValue = "";

            foreach (DataColumn col in dt.Columns)
            {
                JudgeColumnLength(col.ColumnName, "表" + sTableName + "的字段" + col.ColumnName + "名称过长，不能超过27个字符");

                strKey += col.ColumnName + ",";
                strValue += ":" + col.ColumnName + ",";
            }
            strKey = strKey.Substring(0, strKey.Length - 1);
            strValue = strValue.Substring(0, strValue.Length - 1);

            string strSql = string.Format("insert into {0} ({1}) values ({2})", sTableName, strKey, strValue);

            OracleConnection conn = this._connection;
            OracleCommand comm = new OracleCommand(strSql, conn);
            comm.ArrayBindCount = dt.Rows.Count;
            comm.BindByName = true;
            comm.CommandType = CommandType.Text;
            comm.CommandTimeout = 1800;

            foreach (DataColumn col in dt.Columns)
            {


                string dataType = GetDataTypeByColumnName(sTableName, col.ColumnName);



                switch (dataType)
                {
                    case "NVARCHAR2":
                        string[] arrStr = new string[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            arrStr[i] = dt.Rows[i][col.ColumnName].ToString();
                        }
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.NVarchar2));
                        comm.Parameters[":" + col.ColumnName].Value = arrStr;
                        break;
                    case "NCHAR":
                        arrStr = new string[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            arrStr[i] = dt.Rows[i][col.ColumnName].ToString();
                        }
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.NChar));
                        comm.Parameters[":" + col.ColumnName].Value = arrStr;
                        break;
                    case "VARCHAR2": 
                        arrStr = new string[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            arrStr[i] = dt.Rows[i][col.ColumnName].ToString();
                        }
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.Varchar2));
                        comm.Parameters[":" + col.ColumnName].Value = arrStr;
                        break;
                    case "CHAR":
                        arrStr = new string[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            arrStr[i] = dt.Rows[i][col.ColumnName].ToString();
                        }
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.Char));
                        comm.Parameters[":" + col.ColumnName].Value = arrStr;
                        break;
                    case "NUMBER":
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.NChar));
                        comm.Parameters[":" + col.ColumnName].ArrayBindStatus = new OracleParameterStatus[dt.Rows.Count];
                        decimal[] arrDec = new decimal[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            if (dt.Rows[i][col.ColumnName]!=null && dt.Rows[i][col.ColumnName].ToString()!="")
                            {
                                arrDec[i] = Convert.ToDecimal(dt.Rows[i][col.ColumnName]);
                            }
                        }
                        comm.Parameters[":" + col.ColumnName].Value = arrDec;
                        break;
                    case "DATE":
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.Date));
                        comm.Parameters[":" + col.ColumnName].ArrayBindStatus = new OracleParameterStatus[dt.Rows.Count];
                        DateTime[] arrDate = new DateTime[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            if (dt.Rows[i][col.ColumnName] == null || dt.Rows[i][col.ColumnName].ToString() == "")
                            {
                                comm.Parameters[":" + col.ColumnName].ArrayBindStatus[i] = OracleParameterStatus.NullInsert;
                            }
                            else
                            {
                                arrDate[i] = Convert.ToDateTime(dt.Rows[i][col.ColumnName]);
                            }
                        }
                        comm.Parameters[":" + col.ColumnName].Value = arrDate;
                        break;
                    case "LONG":
                        arrStr = new string[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            arrStr[i] = dt.Rows[i][col.ColumnName].ToString();
                        }
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.Long));
                        comm.Parameters[":" + col.ColumnName].Value = arrStr;
                        break;
                    
                    case "CLOB":
                        arrStr = new string[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            arrStr[i] = dt.Rows[i][col.ColumnName].ToString();
                        }
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.Clob));
                        comm.Parameters[":" + col.ColumnName].Value = arrStr;
                        break;
                    default:
                        arrStr = new string[dt.Rows.Count];
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            arrStr[i] = dt.Rows[i][col.ColumnName].ToString();
                        }
                        comm.Parameters.Add(new OracleParameter(":" + col.ColumnName, OracleDbType.Varchar2));
                        comm.Parameters[":" + col.ColumnName].Value = arrStr;
                        break;
                }
            }

            if (_transaction != null)
            {
                comm.Transaction = _transaction;
            }
            else
            {
                Open();
            }

            try
            {
                int ii = comm.ExecuteNonQuery();

                if ((this._transaction == null) && !this._alwaysOpen)
                {
                    this.Close();
                }

                if (ii >= 1)
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

        public DataTable GetTableName()
        {
            string strSql = "select TABLE_NAME from user_tables order by  TABLE_NAME";

            DataTable dt = GetDataSet(strSql).Tables[0];

            return dt;
        }

        public DataTable GetViewName()
        {
            string strSql = "select * from user_views";

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
            ti.DataBaseType = "ORACLE";
            string strSql = "select column_id as 字段序号,column_name as 字段名称, data_type as 字段类型, data_length as 字段长度," +
                            "data_precision as 整数位,data_scale as 小数位,nullable as 允许空,data_default as 默认值," +
                            "(select comments from user_col_comments where table_name=t.TABLE_NAME and column_name=t.COLUMN_NAME) as 说明," +
                            "(select '1' from dual where (select count(*) from user_constraints con,user_cons_columns col where " +
                             " con.constraint_name=col.constraint_name and con.constraint_type='P'" +
                             " and col.table_name=t.TABLE_NAME and col.column_name=t.COLUMN_NAME)=1) as 主键," +
                             "(select COMMENTS from user_tab_comments where TABLE_NAME=t.table_name ) as 表说明" +
                            " from user_tab_columns t where table_name = '" + ti.TableName + "' order by column_id";

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

                    if (ds.Tables[0].Rows[ii]["允许空"].ToString() == "Y")
                        ci[ii].IsNull = true;
                    else
                        ci[ii].IsNull = false;

                    if (ds.Tables[0].Rows[ii]["主键"].ToString() == "1")
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
            OracleConnection conn = this._connection;
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
            OracleConnection conn = this._connection;
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
            OracleConnection conn = this._connection;
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
