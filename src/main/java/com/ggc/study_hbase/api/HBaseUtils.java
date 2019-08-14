package com.ggc.study_hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {

    private static Connection connection = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
//            conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
            conf.set("hbase.zookeeper.quorum", "ggc");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        Admin admin = connection.getAdmin();
        if (isTableExist(tableName, admin)) {
            System.err.println("table : " + tableName + " already exists!");
            return;
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilies) {
            hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
        }
        admin.createTable(hTableDescriptor);

        admin.close();
    }

    private static boolean isTableExist(String tableName, Admin admin) throws IOException {

        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));

        return tableExists;
    }

    public static void modifyTable(String tableName, String columnFamily) throws IOException {
        Admin admin = connection.getAdmin();

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        hColumnDescriptor.setMaxVersions(3);
        admin.modifyColumn(TableName.valueOf(tableName), hColumnDescriptor);

        admin.close();
    }


    public static void deleteTable(String tableName) throws IOException {

        Admin admin = connection.getAdmin();

        if (isTableExist(tableName, admin)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }

        admin.close();
    }

    public static void putCell(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }


    public static void getRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        //get.addColumn();

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }

        table.close();

    }

    private static void scanTable(String tableName, String startRow, String stopRow) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        resultScanner.close();
        table.close();
    }

    public static void scanTableByFilter(String tableName, String columnFamily
            , String column, String value) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

//        scan.setRowPrefixFilter();
        SingleColumnValueFilter singleColumnValueFilter
                = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column)
                , CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        singleColumnValueFilter.setFilterIfMissing(true);


        SingleColumnValueFilter singleColumnValueFilter2
                = new SingleColumnValueFilter(Bytes.toBytes("family"), Bytes.toBytes("column")
                , CompareFilter.CompareOp.EQUAL, Bytes.toBytes("value"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL
                , singleColumnValueFilter, singleColumnValueFilter2);

        scan.setFilter(filterList);

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        resultScanner.close();
        table.close();
    }

    public static void deleteRow(String tableName, String rowKey) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        table.delete(delete);
        table.close();

    }

    public static void deleteColumn(String tableName, String rowKey, String columnFamily, String column) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

//        delete.addColumns();//删除所有版本
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));//删除最新版本或者指定版本
        table.delete(delete);
        table.close();

    }

    public static void main(String[] args) throws IOException {
//        createTable("class", "info");
//        modifyTable("class", "info");
        deleteTable("class");

//        createTable("person", "info");
//        putCell("person", "1001", "info", "name", "张三");

//        getRow("person", "1001");

//        scanTable("student", "1001", "1004");

//        scanTableByFilter("student", "info", "name", "zs");

//        deleteRow("student", "1001");
//        deleteColumn("student", "1002", "info", "name");

    }
}
