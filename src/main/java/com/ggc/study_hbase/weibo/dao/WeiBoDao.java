package com.ggc.study_hbase.weibo.dao;

import com.ggc.study_hbase.weibo.WeiBoUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiBoDao {

    private static Connection connection;

    static {

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "ggc");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createNameSpace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor;

        //通过是否抛出NamespaceExistException异常，判断Namespace是否存在
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceExistException e) {
//            e.printStackTrace();
            namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }

        admin.close();
    }

    public void createTable(String namespace, String tableName, String columnFamily) throws IOException {
        createTable(TableName.valueOf(namespace + ":" + tableName), columnFamily, 1);
    }

    public void createTable(String namespace, String tableName, String columnFamily, int maxVersions) throws IOException {
        createTable(TableName.valueOf(namespace + ":" + tableName), columnFamily, maxVersions);
    }

    private void createTable(TableName tableName, String columnFamily, int maxVersions) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                return;
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            hColumnDescriptor.setMaxVersions(maxVersions);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
        }

    }

    public void insertRow(String namespace, String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        insertRow(TableName.valueOf(namespace + ":" + tableName), Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
    }

    private void insertRow(TableName tableName, byte[] rowKey, byte[] columnFamily, byte[] column, byte[] content) throws IOException {

        try (Table table = connection.getTable(tableName)) {
            Put put = new Put(rowKey);
            put.addColumn(columnFamily, column, content);

            table.put(put);

        }

    }

    public void clearTableData(String namespace, String tableName) throws IOException {

        TableName tn = TableName.valueOf(namespace + ":" + tableName);
        try (Admin admin = connection.getAdmin()) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }


    }

    public List<String> scanRowByPrefix(String namespace, String tableName, String rowKeyPrefix) throws IOException {

        try (Table table = connection.getTable(WeiBoUtils.toTableName(namespace, tableName))) {

            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(rowKeyPrefix));
            ResultScanner resultScanner = table.getScanner(scan);

            List<String> rowKeyList = new ArrayList<>();
            resultScanner.forEach(result -> {
                rowKeyList.add(Bytes.toString(result.getRow()));
            });
            return rowKeyList;
        }
    }

    public List<String> scanRow(String namespace, String tableName, List<String> rowKeyList) throws IOException {
        try (Table table = connection.getTable(WeiBoUtils.toTableName(namespace, tableName))) {

            List<Get> getList = new ArrayList<>();

            rowKeyList.forEach((rowKey) -> {
                Get get = new Get(Bytes.toBytes(rowKey));
                getList.add(get);
            });

            List<String> valueList = new ArrayList<>();
            for (Result result : table.get(getList)) {
                String value = Bytes.toString(CellUtil.cloneValue(result.rawCells()[0]));
                valueList.add(value);
            }
            return valueList;
        }
    }

    public void insertCells(String namespace, String tableName, List<String> fansList, String columnFamily, String columnName, String value) throws IOException {

        try (Table table = connection.getTable(WeiBoUtils.toTableName(namespace, tableName))) {

            List<Put> putList = new ArrayList<>();
            fansList.forEach((fans) -> {

                Put put = new Put(Bytes.toBytes(fans));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
                putList.add(put);
            });

            table.put(putList);

        }

    }

    public void insertRows(String namespace, String tableName, List<String> rowKeyList, String columnFamily, String value4UserRelation) throws IOException {


    }

    public void deleteRow(String namespace, String tableName, String rowKey) throws IOException {
        try (Table table = connection.getTable(WeiBoUtils.toTableName(namespace, tableName))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            table.delete(delete);

        }
    }

    public List<String> scanCells(String namespace, String tableName, String rowKey, String columnFamily) throws IOException {

        try (Table table = connection.getTable(WeiBoUtils.toTableName(namespace, tableName))) {

            List<String> list = new ArrayList<>();

            Get get = new Get(Bytes.toBytes(rowKey));
            get.setMaxVersions(2);

            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                list.add(Bytes.toString(CellUtil.cloneValue(cell)));
            }
            return list;

        }

    }


}
