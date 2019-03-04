

using System;

namespace ZondyDBHelper
{
    //结合电子政务调用方法类
    public class DESLandSiteFun
    {
        //对模型进行解密返回验证模型
        public static Enroll Decrypt(Enroll enroll,string strBate="5.0")
        {
            DESClassInterface DesClsInt = new DESClass();

            if (String.IsNullOrEmpty(enroll.ActivationDate))
            {
                enroll.ActivationDate = DateTime.Now.ToString("yyyy-MM-dd").Replace("-", "");
            }
            string strTemp = "";
            if (strBate=="4.0")
            {
                strTemp = DesClsInt.Decrypt(enroll.Code, enroll.ActivationDate, "MapGis09");
            }
            else
            {
                strTemp = DesClsInt.Decrypt(enroll.Code, enroll.ActivationDate, "MapGis10");
            }


            string[] str = strTemp.Split('@');
            if (str.Length != 4)
                return null;

            enroll.DiskNo = str[0];
            enroll.Date = str[1];
            enroll.State = Convert.ToInt32(str[2]);
            enroll.Name = str[3];

            return enroll;

            
        }

        public static Enroll Decrypt(Enroll enroll)
        {
            DESClassInterface DesClsInt = new DESClass();

            if (String.IsNullOrEmpty(enroll.ActivationDate))
            {
                enroll.ActivationDate = DateTime.Now.ToString("yyyy-MM-dd").Replace("-", "");
            }

            string strTemp = DesClsInt.Decrypt(enroll.Code, enroll.ActivationDate, "MapGis10");

            string[] str = strTemp.Split('@');
            if (str.Length != 4)
                return null;

            enroll.DiskNo = str[0];
            enroll.Date = str[1];
            enroll.State = Convert.ToInt32(str[2]);
            enroll.Name = str[3];

            return enroll;


        }

    }
}
