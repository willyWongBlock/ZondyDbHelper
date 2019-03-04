

using System;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace ZondyDBHelper
{
    public class DESClass : DESClassInterface
    {
        public DESClass()
        { 
        
        }
        
        /// <summary>
        /// 对字符串进行DES加密
        /// </summary>
        /// <param name="sourceString">加密原数据</param>
        /// <param name="key">密钥</param>
        /// <param name="iv">初始化向量</param>
        /// <returns></returns>
        public string Encrypt(string sourceString, string key, string iv)
        {
            try
            {
                byte[] btKey = Encoding.UTF8.GetBytes(key);

                byte[] btIV = Encoding.UTF8.GetBytes(iv);

                DESCryptoServiceProvider des = new DESCryptoServiceProvider();

                using (MemoryStream ms = new MemoryStream())
                {
                    byte[] inData = Encoding.UTF8.GetBytes(sourceString);
                    try
                    {
                        using (CryptoStream cs = new CryptoStream(ms, des.CreateEncryptor(btKey, btIV), CryptoStreamMode.Write))
                        {
                            cs.Write(inData, 0, inData.Length);

                            cs.FlushFinalBlock();
                        }
                        string strTemp = Convert.ToBase64String(ms.ToArray());
                            
                        return StringToASCIIX2String(strTemp);
                    }
                    catch
                    {
                        return sourceString;
                    }
                }
            }
            catch { }

            return "DES加密出错";
        }


        //对DES加密后的字符串进行解密
        public string Decrypt(string encryptedString, string key, string iv)
        {
            byte[] btKey = Encoding.UTF8.GetBytes(key);

            byte[] btIV = Encoding.UTF8.GetBytes(iv);

            DESCryptoServiceProvider des = new DESCryptoServiceProvider();

            System.Text.ASCIIEncoding asciiEncoding = new System.Text.ASCIIEncoding();
            
            string strTemp = "";
            try
            {
                int num2;
                byte[] buffer = new byte[encryptedString.Length / 2];
                int length = buffer.Length;
                string str2 = "";
                for (num2 = 0; num2 < length; num2++)
                {
                    str2 = encryptedString.Substring(num2 * 2, 2);
                    buffer[num2] = Convert.ToByte(str2, 16);
                }

                strTemp = asciiEncoding.GetString(buffer);
            }
            catch (Exception)
            {

                strTemp = "";
            }
            if (!String.IsNullOrEmpty(strTemp))
            {
                encryptedString = strTemp;
            }
                
            

            
            
            
            using (MemoryStream ms = new MemoryStream())
            {
                byte[] inData = Convert.FromBase64String(encryptedString);
                try
                {
                    using (CryptoStream cs = new CryptoStream(ms, des.CreateDecryptor(btKey, btIV), CryptoStreamMode.Write))
                    {
                        cs.Write(inData, 0, inData.Length);

                        cs.FlushFinalBlock();
                    }
                    return Encoding.UTF8.GetString(ms.ToArray());
                }
                catch
                {
                    return encryptedString;
                }
            }
        }

        //对字符串进行DES加密
        public string EncryptString(string sInputString, string sKey, string sIV)
        {
            try
            {
                byte[] data = Encoding.UTF8.GetBytes(sInputString);

                DESCryptoServiceProvider DES = new DESCryptoServiceProvider();

                DES.Key = ASCIIEncoding.ASCII.GetBytes(sKey);

                DES.IV = ASCIIEncoding.ASCII.GetBytes(sIV);

                ICryptoTransform desencrypt = DES.CreateEncryptor();

                byte[] result = desencrypt.TransformFinalBlock(data, 0, data.Length);

                return BitConverter.ToString(result);
            }
            catch { }

            return "转换出错！";
        }

        //对字符串进行DES解密
        public string DecryptString(string sInputString, string sKey, string sIV)
        {
            try
            {
                string[] sInput = sInputString.Split("-".ToCharArray());

                byte[] data = new byte[sInput.Length];

                for (int i = 0; i < sInput.Length; i++)
                {
                    data[i] = byte.Parse(sInput[i], NumberStyles.HexNumber);
                }

                DESCryptoServiceProvider DES = new DESCryptoServiceProvider();

                DES.Key = ASCIIEncoding.ASCII.GetBytes(sKey);

                DES.IV = ASCIIEncoding.ASCII.GetBytes(sIV);

                ICryptoTransform desencrypt = DES.CreateDecryptor();

                byte[] result = desencrypt.TransformFinalBlock(data, 0, data.Length);

                return Encoding.UTF8.GetString(result);
            }
            catch { }

            return "解密出错！";
        }

        private string StringToASCIIX2String(string strTemp)
        {
            if (!String.IsNullOrEmpty(strTemp))
            {
                System.Text.ASCIIEncoding asciiEncoding = new System.Text.ASCIIEncoding();
                byte[] buffer = asciiEncoding.GetBytes(strTemp);

                strTemp = "";
                for (int i = 0; i < buffer.Length; i++)
                {
                    //int asciicode = (int)(buffer[i]);
                    strTemp += buffer[i].ToString("X2");
                }

            }
            return strTemp;
        }

        /// <summary>
        /// 快速生成验证码功能-5天试用
        /// </summary>
        /// <param name="sourceString">加密原数据</param>
        /// <returns></returns>
        public string EncryptQuickly(string sourceString)
        {
            string strB64DiskID = sourceString;
            string strDiskID = Encoding.UTF8.GetString(Convert.FromBase64String(strB64DiskID));
            string strDate = Convert.ToDateTime(DateTime.Now.AddDays(5).ToString("yyyy-MM-dd")).ToString("yyyy-MM-dd");
            string strKey = DateTime.Now.ToString("yyyy-MM-dd").Replace("-", "");
            string striv = "MapGis10";
            string strType = "1";

            string strTemp = Encrypt(strDiskID + "@" + strDate + "@" + strType + "@试用账户", strKey, striv);

            return strTemp;
            
        }

        /// <summary>
        /// 对数据库链接字符串进行加密
        /// </summary>
        /// <param name="sourceString">加密原数据</param>
        /// <returns></returns>
        public string EncryptDB(string sourceString)
        {
            string strKey = "ZONDYYUN";
            string striv = "WORKFLOW";
            return Encrypt(sourceString, strKey, striv);
        }

        /// <summary>
        /// 对数据库链接字符串进行解密
        /// </summary>
        /// <param name="sourceString">加密原数据</param>
        /// <returns></returns>
        public string DecryptDB(string sourceString)
        {
            string strKey = "ZONDYYUN";
            string striv = "WORKFLOW";
            return Decrypt(sourceString, strKey, striv);
        }
    }
}
