//package hbase;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.util.Bytes;
//
////HBaseAdmin 管理hbase的，主要负责DDL操作
////HTable 管理表中数据的，主要负责DML操作
//public class HBaseTest {
//	public static final String TABLE_NAME = "t1";
//	public static final String ROW_KEY = "rk1";
//	
//	public static void main(String[] args) throws Exception{
//		//ddl(TABLE_NAME, "f1");
//		dml();
//		scan();
//	}
//	
//
//	private static void scan() throws IOException {
//		final Configuration conf = getConf();
//		final HTable hTable = new HTable(conf, TABLE_NAME);
//		
//		//使用scan对象可以设定startRow、stopRow
//		Scan scan = new Scan();
//		//scan.setStartRow(startRow);
//		//scan.setStopRow(stopRow);
//		//scan.setFilter(filter);
//		final ResultScanner scanner = hTable.getScanner(scan);
//		
//		//指定列簇、列
//		//final ResultScanner scanner = hTable.getScanner(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
//		for (Result result : scanner) {
//			System.out.println(result);
//		}
//		
//		hTable.close();
//	}
//
//	private static void dml() throws IOException {
//		final Configuration conf = getConf();
//		final HTable hTable = new HTable(conf, TABLE_NAME);
//		Put put = new Put(Bytes.toBytes(ROW_KEY));
//		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes("张三"));
//		hTable.put(put);
//		
//		Get get = new Get(Bytes.toBytes(ROW_KEY));
//		final Result result = hTable.get(get);
//		final Object valueString = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1")));
//		//System.out.println(valueString);
//		
//		//Delete delete = new Delete(Bytes.toBytes(ROW_KEY));
//		//hTable.delete(delete);
//		
//		hTable.close();
//	}
//	
//	public static void ddl(String tableName, String... familyNames) throws Exception{
//		final Configuration conf = getConf();
//		final HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
//
//		//创建表
//		HTableDescriptor htableDesc = new HTableDescriptor(tableName);
//		for (String familyName : familyNames) {
//			HColumnDescriptor familyDesc = new HColumnDescriptor(familyName);
//			htableDesc.addFamily(familyDesc);
//		}
//		if(!hBaseAdmin.tableExists(TABLE_NAME)) {
//			hBaseAdmin.createTable(htableDesc);
//		}
//		
//		System.out.println("表是否存在"+hBaseAdmin.tableExists(tableName));
//		
//		//hBaseAdmin.disableTable(TABLE_NAME);
//		//hBaseAdmin.deleteTable(TABLE_NAME);
//		
//		hBaseAdmin.close();
//	}
//
//	private static Configuration getConf() {
//		final Configuration conf = HBaseConfiguration.create();
//		conf.setStrings("hbase.zookeeper.quorum", "hadoop0");
//		return conf;
//	}
//}
