using System;
using System.Collections.Generic;
using System.Text;

namespace ZondyDBHelper
{
    //自定义注解类
    public class MyAttribute : Attribute
    {
        public Boolean PrimaryKey = false;
        public Boolean IgnoreColumns = false;
        public String Type = null;
    }
}
