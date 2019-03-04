using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using System.IO;

namespace ZondyDBHelper
{
    /// <summary>
    /// 序列化相关操作
    /// </summary>
    public class Serialization
    {
        /// <summary>
        /// 序列化
        /// </summary>
        /// <param name="oo">要序列化的对象</param>
        /// <param name="sPath">序列文件的存放路径</param>
        /// <returns>错误信息</returns>
        public static string Serializer(object oo, string sPath)
        {
            if (sPath.Trim() == "")
                return "输入的路径为空！";

            try
            {
                StreamWriter sw = new StreamWriter(sPath, false);
                XmlSerializer sr = new XmlSerializer(oo.GetType());
                sr.Serialize(sw, oo);
                sw.Close();

                return null;
            }
            catch (Exception ee)
            {
                return ee.Message;
            }
        }

        /// <summary>
        /// 反序列化
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="oo">反序列化的对象</param>
        /// <param name="sPath">序列文件的存放路径</param>
        public static void UnSerializer<T>(ref T oo, string sPath)
        {
            Stream ss = System.IO.File.OpenRead(sPath);
            XmlSerializer sr = new XmlSerializer(oo.GetType());
            oo = (T)sr.Deserialize(ss);
            ss.Close();
        }

        /// <summary>
        /// 序列化
        /// </summary>
        /// <param name="oo">要序列化的对象</param>
        /// <returns>序列化后以字符串形式返回</returns>
        public static string Serializer(object oo)
        {
            XmlSerializer sr = new XmlSerializer(oo.GetType());

            MemoryStream stream = new MemoryStream();
            sr.Serialize(stream, oo);
            stream.Seek(0, SeekOrigin.Begin);
            string s = Encoding.UTF8.GetString(stream.ToArray());

            return s;
        }


        /// <summary>
        /// 字符串反序列化
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="oo">反序列化的对象</param>
        /// <param name="str">序列化字符串</param>
        public static void UnSerializerStream<T>(ref T oo, string str)
        {
            MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(str));
            XmlSerializer sr = new XmlSerializer(oo.GetType());
            oo = (T)sr.Deserialize(ms);
            ms.Close();
        }
    }
}
