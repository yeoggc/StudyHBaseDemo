package com.ggc.study_hbase.weibo.service;

import com.ggc.study_hbase.weibo.dao.WeiBoDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.ggc.study_hbase.weibo.WeiBoConstants.*;

public class WeiBoService {

    private final WeiBoDao dao = new WeiBoDao();

    public WeiBoService() {

    }

    public void createNSAndTable() throws IOException {
        //1) 创建命名空间以及表名的定义
        dao.createNameSpace(NAMESPACE);

        //2) 创建微博内容表
        dao.createTable(NAMESPACE, TABLE_WEIBO_CONTENT, COLUMN_FAMILY_DATA);

        //3) 创建用户关系表
        dao.createTable(NAMESPACE, TABLE_USER_RELATION, COLUMN_FAMILY_DATA);

        //4) 创建用户微博内容接收邮件表,最近3条微博内容
        dao.createTable(NAMESPACE, TABLE_USER_INBOX, COLUMN_FAMILY_DATA, 3);
    }


    public void publish(String userId, String content) throws IOException {
        //5) 发布微博内容

        //5.1 往表WeiBo_Content增加一行记录
        String rowKey4WeiBoContent = userId + "_" + System.currentTimeMillis();
        dao.insertRow(NAMESPACE, TABLE_WEIBO_CONTENT, rowKey4WeiBoContent, COLUMN_FAMILY_DATA, COLUMN_NAME_CONTENT, content);

        //5.2 根据userId，从表User_Relation中查找出所有粉丝
        String rowKeyPrefix4UserRelation = userId + ":followedBy:";
        List<String> rowKeyList4UserRelation = dao.scanRowByPrefix(NAMESPACE, TABLE_USER_RELATION, rowKeyPrefix4UserRelation);

        List<String> fansList = new ArrayList<>();
        rowKeyList4UserRelation.forEach((rowKey4UserRelation) -> {
            String[] split = rowKey4UserRelation.split(":followedBy:");
            fansList.add(split[1]);
        });

        System.err.println("publish - 获取粉丝数 = > " + fansList);

        //5.3 在表User_Inbox中，每个粉丝在其对应的行中，需要往其对应所关注用户的那一列中增加一个Cell
        dao.insertCells(NAMESPACE, TABLE_USER_INBOX, fansList, COLUMN_FAMILY_DATA, userId, rowKey4WeiBoContent);

    }


    public void addAttend(String userId, String beFollowedUserId) throws IOException {

        //6) 添加关注用户
        String rowKey1 = userId + ":followedBy:" + beFollowedUserId;
        String value1 = String.valueOf(System.currentTimeMillis());
        dao.insertRow(NAMESPACE, TABLE_USER_RELATION, rowKey1, COLUMN_FAMILY_DATA, COLUMN_NAME_TIME, value1);


        String rowKey2 = beFollowedUserId + ":follow:" + userId;
        String value2 = String.valueOf(System.currentTimeMillis());
        dao.insertRow(NAMESPACE, TABLE_USER_RELATION, rowKey2, COLUMN_FAMILY_DATA, COLUMN_NAME_TIME, value2);

    }

    public void unfollow(String userId, String beUnfollowedUserId) throws IOException {
        //7) 移除（取关）用户
        String rowKey1 = userId + ":followedBy:" + beUnfollowedUserId;
        String value1 = String.valueOf(System.currentTimeMillis());
        dao.deleteRow(NAMESPACE, TABLE_USER_RELATION, rowKey1);


        String rowKey2 = beUnfollowedUserId + ":follow:" + userId;
        String value2 = String.valueOf(System.currentTimeMillis());
        dao.deleteRow(NAMESPACE, TABLE_USER_RELATION, rowKey2);

    }


    /**
     * 在所有关注的人中获取最近的微博
     */
    public List<String> getAllRecentWeiBo(String userId) throws IOException {
        //第一步，根据userId,从收件箱查所有列的版本号为2的数据
        List<String> weiBoRowKeyList = dao.scanCells(NAMESPACE,TABLE_USER_INBOX,userId,COLUMN_FAMILY_DATA);
        System.err.println(weiBoRowKeyList);
        //第二步，从第一步得到的微博RowKey，去微博表中找相应的数据
        return dao.scanRow(NAMESPACE, TABLE_WEIBO_CONTENT, weiBoRowKeyList);

    }

    public void clearTableUserInboxData() throws IOException {
        dao.clearTableData(NAMESPACE, TABLE_USER_INBOX);
    }

}
