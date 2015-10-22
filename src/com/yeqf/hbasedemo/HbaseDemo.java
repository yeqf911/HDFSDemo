package com.yeqf.hbasedemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

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
        conf.set("hbase.zookeeper.quorum","yehost");
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
}
