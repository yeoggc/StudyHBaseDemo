package com.ggc.study_hbase.weibo;

public interface WeiBoConstants {

    String NAMESPACE = "WeiBo4Ggc";
    String TABLE_WEIBO_CONTENT = "WeiBo_Content";
    String TABLE_USER_RELATION = "User_Relation";
    String TABLE_USER_INBOX = "User_Inbox";

    String COLUMN_FAMILY_DATA = "data";
    String COLUMN_NAME_CONTENT = "content";
    String COLUMN_NAME_TIME = "time";
    String COLUMN_NAME_WEIBO_ROWKEY = "WeiBo_RowKey";

}
