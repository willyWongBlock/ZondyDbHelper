

namespace ZondyDBHelper
{
    /// <summary>
    /// 数据加密接口
    /// </summary>
    public interface DESClassInterface
    {

        /// <summary>
        /// 对字符串进行DES加密
        /// </summary>
        string Encrypt(string sourceString, string key, string iv);

        //对DES加密后的字符串进行解密
        string Decrypt(string encryptedString, string key, string iv);

        //对字符串进行DES加密
        string EncryptString(string sInputString, string sKey, string sIV);


        //对字符串进行DES解密
        string DecryptString(string sInputString, string sKey, string sIV);

        //快速生成验证码功能-5天试用
        string EncryptQuickly(string sourceString);

        /// <summary>
        /// 对数据库链接字符串进行加密
        /// </summary>
        string EncryptDB(string sourceString);

        /// <summary>
        /// 对数据库链接字符串进行解密
        /// </summary>
        string DecryptDB(string sourceString);
    }
}
