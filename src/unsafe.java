import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;


import ProductData.CountData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;



public class unsafe {

    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        //configuration.addResource(new Path("/home/hadoop/hbase-1.3.2/conf/hbase-site.xml"));
        configuration.set("hbase.zookeeper.quorum", "asterix-2,asterix-4,asterix-6,asterix-8,asterix-10");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.set("hbase.master", "192.168.200.229:16010");

    }

    //public static void main(String[] args) {
        //createTable("earthquakedata");
        // insertData("wujintao");  
        //QueryAll("eatrhquakedata");
        // QueryByCondition1("wujintao");  
        // QueryByCondition2("wujintao");  
        //QueryByCondition3("wujintao");  
        //deleteRow("wujintao","abcdef");  
        //deleteByCondition("wujintao","abcdef");

        //int start = Integer.parseInt(args[0]);
        //int end = Integer.parseInt(args[1]);
        int start =0;
        int end = 50;

        String tableName = "earthquakedata";
        //insert_data_by_loop(tableName,start,end);


    /**
     * 创建表 
     * @param tableName
     */
    public static void createTable(String tableName) {
        System.out.println("start create table ......");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println(tableName + " is exist,detele....");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("Day"));
            tableDescriptor.addFamily(new HColumnDescriptor("Hour"));
            tableDescriptor.addFamily(new HColumnDescriptor("Min"));
            tableDescriptor.addFamily(new HColumnDescriptor("Second"));
            hBaseAdmin.createTable(tableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end create table ......");
    }

    /**
     * 插入数据 
     * @param tableName
     */
    public static void insertData(String tableName, HTable table,String rowkey, String value) {


        //String rowkey  = "34003B9130320010101";
        //String value = "84.26";


        Put  put = new Put(rowkey.getBytes());
        put.addColumn("Day".getBytes(),"D".getBytes(),value.getBytes());
        try{
            table.put(put);
        } catch (Exception e){
            e.printStackTrace();
        }


    }

    /**
     * 循环插入数据
     * @param tableName ,start ,end num of records
     */

    public static void insert_data_by_loop(String tableName,int start,int end){
        System.out.println("start insert data ......");
        Configuration conf = HBaseConfiguration.create();
        String rowkey = "13002B91303200010101";
        String value = "32.63";
        int[] start_date = new int[3];
        start_date[0] = 2001;
        start_date[1] = 1;
        start_date[2] = 1;

        try {
            HTable table = new HTable(conf,tableName);
            table.setAutoFlushTo(true);//不显示设置则默认是true
            SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
            Random random = new Random();
            DecimalFormat decimalFormat=new DecimalFormat(".00");
            CountData string_data = new CountData();

            long start_time = new Date().getTime();
            for (int count = start;count < end; count++){
                rowkey = string_data.CountData(count,start_date);
                value = decimalFormat.format(random.nextFloat()*100);
                insertData(tableName,table,rowkey,value);
            }
            long end_time = new Date().getTime();
            SimpleDateFormat format_time = new SimpleDateFormat("HH:mm:ss:SS");
            long spend_time = end_time - start_time;

            Writelog.writeLog("测试插入" + String.valueOf(end-start) + "条记录时间");
            Writelog.writeLog("start time:" + format1.format(start_time) +"  timestamp  " +start_time);
            Writelog.writeLog("end time:" + format1.format(end_time)+ "  timestamp " + end_time);
            Writelog.writeLog("spend time:" + format_time.format(spend_time) + "  timestamp  " + spend_time);

            System.out.println("inserted record.");


            table.close();//关闭hbase连接

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end insert data ......");
    }

    /**
     * 删除一张表 
     * @param tableName
     */
    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    /**
     * 根据 rowkey删除一条记录 
     * @param tablename
     * @param rowkey
     */
    public static void deleteRow(String tablename, String rowkey)  {
        try {
            HTable table = new HTable(configuration, tablename);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);

            table.delete(list);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * 组合条件删除
     * @param tablename
     * @param rowkey
     */
    public static void deleteByCondition(String tablename, String rowkey)  {
        //目前还没有发现有效的API能够实现 根据非rowkey的条件删除 这个功能能，还有清空表全部数据的API操作

    }


    /**
     * 查询所有数据 
     * @param tableName
     */
    public static void QueryAll(String tableName) {
        Configuration conf=HBaseConfiguration.create();

        try {
            HTable table = new HTable(conf, tableName);
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录 
     * @param tableName
     */
    public static void QueryByCondition1(String tableName,String rowkey) {

        HTablePool pool = new HTablePool(configuration, 1000);
        HTable table = (HTable) pool.getTable(tableName);
        try {
            Get scan = new Get(rowkey.getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条件按查询，查询多条记录 
     * @param tableName
     */
    public static void QueryByCondition2(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = (HTable) pool.getTable(tableName);
            Filter filter = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询  
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 组合条件查询 
     * @param tableName
     */
    public static void QueryByCondition3(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = (HTable) pool.getTable(tableName);

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes
                    .toBytes("aaa"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("column2"), null, CompareOp.EQUAL, Bytes
                    .toBytes("bbb"));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes
                    .toBytes("column3"), null, CompareOp.EQUAL, Bytes
                    .toBytes("ccc"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}  