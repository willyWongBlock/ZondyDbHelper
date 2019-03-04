using Dapper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using ShuErDBHelper.DataBase;

namespace ShuErDBHelper
{
    internal class SqlBase : IDbAccess, IDisposable
    {
        private bool _alwaysOpen;
        private DbConnection _connection;
        private string _identifier = "";
        private DbTransaction _transaction;
        private string sConnection;
        private IsolationLevel _IsolationLevel = IsolationLevel.ReadCommitted;
        private CommandDefinitionNew command;

        public SqlBase(DbConnection conn,string sIdentifier)
        {
            _identifier = sIdentifier;
            _connection = conn;
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

        public virtual DataBaseType DataBaseType
        {
            get { return DataBaseType.DBTYPE; }
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

        public bool ExecuteSql(string strSql)
        {
            //CommandDefinition command = new CommandDefinition(strSql,null,_transaction,null, CommandType.Text);
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

                int aa = this._connection.Execute(command.init());

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
            //CommandDefinition command = new CommandDefinition(strSql,param,_transaction,null,CommandType.Text);
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
                int aa = this._connection.Execute(command.init());

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

        public bool ExecuteSql(string strSql, List<DataField> listDataField)
        {
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add(this._identifier + dataField.DataFieldName, dataField.DataFieldValue);
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

                int aa = this._connection.Execute(command.init());

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

        public void ExecuteSql(ref string[] strSql)
        {
            DbTransaction tra;

            this._connection.Open();
            tra = this._connection.BeginTransaction();
            try
            {
                //CommandDefinition command = new CommandDefinition();
                command.Transaction = tra;
                command.CommandType = CommandType.Text;

                for (int ii = 0; ii < strSql.Length; ii++)
                {
                    command.CommandText = strSql[ii];
                    strSql[ii] = this._connection.Execute(command.init()).ToString();
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
                this._connection.Dispose();
            }
        }

        public string[] ExecuteSql(params string[] strSql)
        {
            DbTransaction tra;

            this._connection.Open();
            tra = this._connection.BeginTransaction();
            try
            {
                //CommandDefinition comm = new CommandDefinition();
                command.Transaction = tra;
                command.CommandType = CommandType.Text;

                for (int ii = 0; ii < strSql.Length; ii++)
                {
                    command.CommandText = strSql[ii];
                    strSql[ii] = this._connection.Execute(command.init()).ToString();
                }
                tra.Commit();
                this._connection.Dispose();
                return strSql;
            }
            catch (Exception e)
            {
                tra.Rollback();
                this._connection.Dispose();
                throw new DataException(e.Message);
            }
        }

        public T QuerySingle<T>(string strSql)
        {
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

                var data = this._connection.Query<T>(command.init()).SingleOrDefault();

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
            //commandDefinition command = new CommandDefinition();
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
                var data = this._connection.Query<T>(command.init());

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

                int ii = this._connection.Execute(command.init());

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
                o = this._connection.ExecuteScalar<Object>(command.init());
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
                using (var reader = this._connection.ExecuteReader(command.init()))
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
                using (var reader = this._connection.ExecuteReader(command.init()))
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
            //记
            DataSet ds = new DataSet();

            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add(this._identifier + dataField.DataFieldName, dataField.DataFieldValue);
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
                using (var reader = this._connection.ExecuteReader(command.init()))
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

        public virtual bool JudgeTableOrViewExist(string tableName)
        {
            return false;
        }

        public bool AddData(string tableName, System.Collections.Hashtable ht)
        {
            //记
            if (ht.Count <= 0)
            {
                return false;
            }

            string strKey = "";
            string strValue = "";

            foreach (DictionaryEntry d in ht)
            {
                strKey += d.Key.ToString() + ",";
                strValue += this._identifier + d.Key.ToString() + ",";
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
            //记
            if (ht.Count <= 0)
            {
                return false;
            }

            string str = "";

            foreach (DictionaryEntry d in ht)
            {
                str += d.Key.ToString() + "= "+ this._identifier + d.Key.ToString() + ",";
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
            return "";
        }

        private bool ExecuteSql(string strSql, string tableName, Hashtable ht)
        {
            //记
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DictionaryEntry d in ht)
            {
                p.Add(this._identifier + d.Key.ToString(), d.Value.ToString());
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

                int ii = this._connection.Execute(command.init());

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
            //记
            string strKey = "";
            string strValue = "";

            if (listDataField.Count <= 0)
            {
                return false;
            }

            foreach (DataField d in listDataField)
            {
                strKey += d.DataFieldName + ",";
                strValue += this._identifier + d.DataFieldName + ",";
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
            //记
            if (listDataField.Count <= 0)
            {
                return false;
            }

            string str = "";

            foreach (DataField d in listDataField)
            {
                str += d.DataFieldName + "= "+ this._identifier + d.DataFieldName + ",";
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
            return "";
        }

        private bool ExecuteSql(string strSql, string tableName, List<DataField> listDataField)
        {
            //记
            //CommandDefinition command = new CommandDefinition();
            command.CommandText = strSql;
            command.CommandType = CommandType.Text;

            DynamicParameters p = new DynamicParameters();
            foreach (DataField dataField in listDataField)
            {
                p.Add(this._identifier + dataField.DataFieldName, dataField.DataFieldValue);
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
                int ii = _connection.Execute(command.init());

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

        public virtual int SetID(string tableName, string columnName, string strFilter)
        {
            return 1;
        }

        public virtual string GetSqlForPageSize(string tableName, string strKeyWord, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            return "";
        }

        public virtual string GetSqlForPageSize(string tableName, string strKeyWord, string columnList, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            string strSql = "";
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

        public virtual DataTable GetTableName()
        {
            DataTable dt =new DataTable();
            return dt;
        }

        public virtual string GetPrimaryKey(string tableName)
        {
            return "";
        }

        public IDataReader GetDataReader(string strSql)
        {
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

                IDataReader dr = this._connection.ExecuteReader(command.init());

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

        public virtual bool BulkToDB(DataTable dt, string sTableName)
        {
            return true;
        }

    }
}
