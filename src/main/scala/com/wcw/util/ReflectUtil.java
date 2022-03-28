package com.wcw.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过反射根据对象属性名称获取对应属性的值(在实际项目中用aop实现连接redis的时候有用到
 * @description:
 * @authror: snaker
 * @date: create:2022/3/27 下午 9:23
 * @version: V1.0
 * @modified By:
 */
public class ReflectUtil {

    /*
     *获取字段的值
     *o 类对象
     *fieldName 字段名称
     */
    public static Object getValueByFieldName(Object o,String filedName) throws Exception {
        return getValueByFieldName(o.getClass(),o,filedName);
    }

    /*
     *通过字段名称获取字段值（反射）
     *classType 类名称
     *o 类对象
     *fieldName 字段名称
     */
    public static Object getValueByFieldName(Class classType,Object o,String fieldName) throws Exception {
        if (fieldName.startsWith("#")) {
            //格式为#empList.id
            String fieldList = fieldName.substring(1,fieldName.indexOf("."));
            String subField = fieldName.substring(fieldName.indexOf(".") + 1);
            Object l = getValueByGetter(classType,o,"get" + upperFirstLetter(fieldList));
            List list = new ArrayList();
            if (l instanceof List) {
                for (Object object : (List)l) {
                    list.add(getValueByFieldName(object.getClass(),object,subField));
                }
            } else {
                throw new RuntimeException("[" + fieldName + "]参数解析失败，非集合类型数据");
            }
            return list;
        } else if(fieldName.contains(".")) {
            String[] fieldNames = fieldName.split("\\.");
            String getName = "get" + upperFirstLetter(fieldNames[fieldNames.length - 1]);
            for (int i = 1;i < fieldNames.length;i++) {
                String getSubNamePre = "get" + upperFirstLetter(fieldNames[i-1]);
                String getSubName = "get" + upperFirstLetter(fieldNames[i]);
                o = getValueByGetter(classType,o,getSubNamePre,getSubName);
            }
            return getValueByGetter(classType,o,getName);
        } else {
            String getName = "get" + upperFirstLetter(fieldName);
            return getValueByGetter(classType,o,getName);
        }
    }

    /*
     *通过getter方法反射获取字段值
     *classType 类名称
     *o 类对象
     *String 类中属性的get方法名称
     */
    public static Object getValueByGetter(Class classType,Object o,String getMethodName) throws NoSuchMethodException,IllegalAccessException, InvocationTargetException {
        //获取相应的方法
        Method getMethod = classType.getMethod(getMethodName,new Class[]{});
        //调用源对象的get方法
        Object value = getMethod.invoke(o);
        return value;
    }

    /*
     *通过getter方法反射获取对象对应的字段值
     *classType 类类别
     */
    public static Object getValueByGetter(Class classType,Object o,String getMethodName,String getSubMethodName) throws NoSuchMethodException,IllegalAccessException,InvocationTargetException {
        //获取相应的方法
        Method getMethod = classType.getMethod(getMethodName,new Class[]{});
        //调用方法
        Object value = getMethod.invoke(o);
        return value;
    }

    /*
     *将首字母转大写
     */
    public static String upperFirstLetter(String str) {
        return str.substring(0,1).toUpperCase() + str.substring(1);
    }
}
