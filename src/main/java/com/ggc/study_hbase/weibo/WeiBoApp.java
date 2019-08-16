package com.ggc.study_hbase.weibo;

import com.ggc.study_hbase.weibo.service.WeiBoService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class WeiBoApp {
    private WeiBoService weiBoService = null;

    @Before
    public void init() {
        weiBoService = new WeiBoService();
    }

    @Test
    public void testCreateNSAndTable() {
        try {
            weiBoService.createNSAndTable();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testPublish() {
        try {
            weiBoService.publish("1001", "1001 发 我要14薪+月薪25116");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAddAttend() {
        try {
            weiBoService.addAttend("1008", "1009");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUnfollow() {
        try {
            weiBoService.unfollow("1008", "1009");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetAllRecentWeiBo() {
        try {
            List<String> allRecentWeiBo = weiBoService.getAllRecentWeiBo("1002");
            System.err.println("testGetAllRecentWeiBo => " + allRecentWeiBo);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void clearAllWeiBoContent() {

        try {
            weiBoService.clearTableUserInboxData();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
