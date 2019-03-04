using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace ZondyDBHelper
{
    public class Encryption
    {
        /// <summary>
        /// 获取用户信息
        /// </summary>
        /// <param name="sEncode">已加密的用户信息</param>
        /// <returns>以数组方式返回解密后的信息，顺序为 用户名，密码，加密时间</returns>
        public string[] GetUserInfoByEncodeString(string sEncode)
        {
            string[] strArr = new string[3];
            int iIndex = sEncode.IndexOf("B");
            string sTime = sEncode.Substring(0, iIndex);
            strArr[2] = DateTime.FromBinary(Convert.ToInt64(sTime)).ToString();
            string[] str = sEncode.Substring(iIndex + 2).Split('I');
            strArr[0] = DecodeString(str[0]);
            if (str.Length == 2)
            {
                strArr[1] = DecodeString(str[1]);
            }
            return strArr;
        }

        /// <summary>
        /// 给用户信息加密
        /// </summary>
        /// <param name="sName">用户名</param>
        /// <param name="sPWD">用户密码</param>
        /// <returns></returns>
        public string SetUserInfoEncode(string sName, string sPWD)
        {
            long ll = DateTime.Now.Ticks;
            string str = "";
            Random rr = new Random();
            string sRandom = rr.Next(0, 9).ToString();
            if (sPWD.Length == 0)
            {
                str = ll.ToString() + "B" + sRandom + EncodeString(sName);
            }
            else
            {
                str = ll.ToString() + "B" + sRandom + EncodeString(sName) + "I" + EncodeString(sPWD);
            }
            return str;
        }

        /// <summary>
        /// 字符串加密
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public string EncodeString(string str)
        {
            str = HttpUtility.UrlEncode(str);
            char[] cc = str.ToCharArray();
            byte[] bb = new byte[cc.Length + 1];
            int iCount = cc.Length;
            for (int ii = 0; ii < iCount; ii++)
            {
                bb[ii] = Convert.ToByte(cc[ii] ^ 19);

            }
            Random rr = new Random();

            int ir = rr.Next(0, cc.Length);

            bb[cc.Length] = Convert.ToByte(cc[ir]);

            string strEncode = "";
            iCount = bb.Length;
            for (int ii = 0; ii < iCount; ii++)
            {
                strEncode += bb[ii].ToString("X");
            }

            return strEncode;
        }

        /// <summary>
        /// 字符串解密
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public string DecodeString(string str)
        {
            byte[] bb = new byte[str.Length / 2];
            int iCount = bb.Length;
            string strTemp = "";
            for (int ii = 0; ii < iCount; ii++)
            {
                strTemp = str.Substring(ii * 2, 2);
                bb[ii] = Convert.ToByte(strTemp, 16);
            }

            char[] cc = new char[bb.Length - 1];
            strTemp = "";
            iCount = cc.Length;
            for (int ii = 0; ii < iCount; ii++)
            {
                cc[ii] = Convert.ToChar(bb[ii] ^ 19);
                strTemp += cc[ii].ToString();
            }

            strTemp = HttpUtility.UrlDecode(strTemp);
            return strTemp;
        }


        public string StringCode(params string[] str)
        {
            string sAll = "";
            foreach (string s in str)
            {
                sAll += s;
            }

            return Encrypt(sAll);
        }

        /// <summary>
        /// SHA1加密
        /// </summary>
        /// <param name="cleanString">要加密的字符串</param>
        /// <returns>加密后的字符串</returns>
        protected string Encrypt(string cleanString)
        {
            System.Text.Encoding ed = Encoding.Default;
            Byte[] clearBytes = ed.GetBytes(cleanString);
            Byte[] result;
            SHA1 sha = new SHA1CryptoServiceProvider();
            result = sha.ComputeHash(clearBytes);
            string strPwd = BitConverter.ToString(result);
            string[] strPwd2 = strPwd.Split('-');
            string t_strPwd = "";
            for (int i = 0; i < strPwd2.Length; i++)
            {
                t_strPwd += strPwd2[i];
            }
            return t_strPwd.Trim();
        }
    }
}
