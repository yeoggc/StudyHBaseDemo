package com.ggc.study_hbase.weibo;

import org.apache.hadoop.hbase.TableName;

public class WeiBoUtils {


    public static TableName toTableName(String namespace, String tableName) {
        return TableName.valueOf(namespace + ":" + tableName);
    }

}
