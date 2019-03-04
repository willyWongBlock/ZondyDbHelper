using System;
using System.Management;

namespace ZondyDBHelper
{
    [Serializable]
    public class Enroll 
    {
        /// <summary>
        /// 使用期限，配合State使用
        /// </summary>
        public string Date { get; set; }
        /// <summary>
        /// 授权用户名称
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 授权状态
        /// 1 检查使用期限
        /// 0 不检查使用期限
        /// 2 开发授权
        /// </summary>
        public int State { get; set; }
        /// <summary>
        /// 授权码
        /// </summary>
        public string Code { get; set; }
        /// <summary>
        /// 授权码中的硬盘编号
        /// </summary>
        public string DiskNo { get; set; }
        /// <summary>
        /// 激活日期
        /// </summary>
        public string ActivationDate { get; set; }


        /// <summary>
        /// 获取硬件ID
        /// 2013-8-30过滤掉USB设备，同时处理掉序列号中的空格，并追加Signature属性值
        /// </summary>
        /// <returns></returns>
        public static string GetDiskID()
        {
            try
            {
                //获取硬盘ID   
                String HDid = "";
                ManagementClass mc = new ManagementClass("Win32_DiskDrive");
                ManagementObjectCollection moc = mc.GetInstances();
                foreach (ManagementObject mo in moc)
                {
                    if (mo.Properties["InterfaceType"].Value.ToString() != "USB")
                    {
                        if (mo.Properties["Signature"] != null)
                        {
                            HDid += mo.Properties["SerialNumber"].Value.ToString().Trim();
                        }
                        else
                        {
                            HDid += mo.Properties["SerialNumber"].Value.ToString().Trim() + mo.Properties["Signature"].Value.ToString().Trim();   //SerialNumber //Name   
                        }
                    }
                }
                moc = null;
                mc = null;

                if (string.IsNullOrWhiteSpace(HDid))//如果取不出来序列号，换一种方式再试试
                {
                    HDid = GetLogicaldiskC();
                }

                return HDid;
            }
            catch
            {
                return GetLogicaldiskC();
            }
        }

        public static string GetLogicaldiskC()
        {
            try
            {
                ManagementObject disk = new ManagementObject("win32_logicaldisk.deviceid=\"c:\"");
                disk.Get();
                return disk.GetPropertyValue("VolumeSerialNumber").ToString();
            }
            catch
            {
                return "";
            }
        }
    }
}