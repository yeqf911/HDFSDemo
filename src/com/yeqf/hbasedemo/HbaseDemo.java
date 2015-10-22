package com.yeqf.hbasedemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yeqf on 2015/10/22.
 */
public class HbaseDemo {
    private static final String TABLENAME = "test";

    public static void main(String[] args) throws Exception {
        create();
    }

    public static Configuration getConfiuration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://101.200.197.107:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "yehost");
        return conf;
    }

    public static void create() throws Exception {
        Configuration conf = getConfiuration();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(TABLENAME)) {
            System.out.println("table already exsits!");
            return;
        }
        HTableDescriptor descriptor = new HTableDescriptor(TABLENAME);
        descriptor.addFamily(new HColumnDescriptor("info"));
        admin.createTable(descriptor);
        System.out.println("create table successful!");
    }

    public static void put(String tableName, String row, String columnFamily, String column, String value) throws IOException {
        HTable table = new HTable(getConfiuration(), tableName);
        Put put = new Put(Bytes.toBytes(row));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    public static void get(String tableName, String row) throws IOException {
        HTable table = new HTable(getConfiuration(), tableName);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
    }

    public static void scan(String tableName) throws IOException {
        HTable table = new HTable(getConfiuration(), tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println("Scan: " + result);
        }
    }

    public static void delete(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(getConfiuration());
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } catch (IOException e) {
                System.out.println("Delete " + tableName + "failed");
            }
        }
        System.out.println("Delete " + tableName + "successful");
    }


}
