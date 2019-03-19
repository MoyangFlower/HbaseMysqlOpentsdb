
import ProductData.CountData;
import ProductData.RandomValue;
import ProductData.Station;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class HbasePutTest {
    private HBaseAdmin admin = null;
    static int TIME_FLAG = 3;
    // 定义配置对象HBaseConfiguration
    private static Configuration configuration;
    public HbasePutTest() throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","asterix-2,asterix-4,asterix-6,asterix-8,asterix-10");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        admin = new HBaseAdmin(configuration);
    }
    // Hbase获取所有的表信息
    public List getAllTables() {
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tables;

    }
    /**
     * 插入数据
     * @param table rowkey value
     */
    public void insertData(HTable table, String rowkey,String family,String str_colume, String value) {


        //String rowkey  = "34003B9130320010101";
        //String value = "84.26";

        Put put = new Put(rowkey.getBytes());
        put.addColumn(family.getBytes(),str_colume.getBytes(),value.getBytes());
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

    public void insert_data_by_loop(String tableName,int start,int end){
        System.out.println("start insert data ......");
        int num=0;
        String rowkey = "13002B91303200010101";
        String value = "32.63";
        int[] start_date = new int[3];
        start_date[0] = 1980;
        start_date[1] = 0;
        start_date[2] = 1;
        String family ="Day";
        String flag="";
        String str_column ;
        int column = 1;
        String dec_format="";
        List<Put> puts = new LinkedList<Put>() ;
        switch (TIME_FLAG){
            case 0:{
                family = "Second";
                column = 24*60*60;
                dec_format="00000";
            }break;
            case 1:{
                family = "Minute";
                column =24*60;
                dec_format="0000";
            }break;
            case 2:{
                family = "Hour";
                column = 24;
                dec_format="00";
            }break;
            case 3:{
                family = "Day";
                column = 1;
            }break;
        }

        try {
            HTable table = new HTable(configuration,tableName);
            //关闭自动提交功能
            table.setAutoFlushTo(false);
            //设置缓存buffer
            table.setWriteBufferSize(128*1024*1024);

            SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
            RandomValue Ran = new RandomValue();
            CountData string_data = new CountData();
            Station station = new Station();
            Object[] list_station =station.query_date(250);

            long start_time = new Date().getTime();
            for (Object stationID:list_station) {
                for (int count = start; count < end; count++) {
                    rowkey = stationID + String.valueOf(TIME_FLAG) + string_data.CountData(count, start_date);
                    for(int i=0;i<column;i++) {
                        value = Ran.data();
                        str_column = column == 1 ? "D" : String.valueOf(new DecimalFormat(dec_format).format(i));
                        //insertData(table, rowkey, family, str_column, value);
                        Put put = new Put(rowkey.getBytes());
                        put.addColumn(family.getBytes(), str_column.getBytes(), value.getBytes());
                        puts.add(put);
                        num++;

                        if (num % 1000 == 0) {
                            //刷新缓存区
                            try {
                                table.put(puts);
                                table.flushCommits();
                                Thread.currentThread().sleep(10);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }

                }

                try {
                    table.put(puts);
                    table.flushCommits();
                    System.out.println();
                    Thread.currentThread().sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(stationID+"&"+num);
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
    public static void main(String[] args) throws Exception {
        HbasePutTest hbaseTest = new HbasePutTest();
        hbaseTest.getAllTables();
        //int start = Integer.parseInt(args[0]);
        //int end = Integer.parseInt(args[1]);
        String tableName = "earthquakedata";
        //hbaseTest.insert_data_by_loop(tableName,start,end);


        JavaHbaseAPI API = new JavaHbaseAPI();
        if ("insert".contentEquals(args[0])){
            if (args.length>3){
                TIME_FLAG=Integer.parseInt(args[3]);
            }
            hbaseTest.insert_data_by_loop(tableName,Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        }
        if ("get".contentEquals(args[0])){
            API.getDate(args[1]);
        }
        if ("scan".contentEquals(args[0])) {
            API.scanDate(args[1], args[2]);
        }
        if ("listget".contentEquals(args[0])){
            String rowkey;
            CountData string_data = new CountData();
            Station station =new Station();
            List<String> list =new ArrayList();
            Object[] list_station =station.query_date(100);
            int[] start_date = new int[3];
            start_date[0] = 1990;
            start_date[1] = 1;
            start_date[2] = 1;

            for (int count = 0;count < 1000; count++) {
                rowkey = list_station[count/1000] + string_data.CountData(count % 1000, start_date);
                list.add(rowkey);
            }
            API.GetDateByList(list);
            list.clear();
        }
        hbaseTest.admin.close();
    }
}
