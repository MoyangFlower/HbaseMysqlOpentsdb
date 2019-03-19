import java.io.IOException;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import ProductData.*;

import com.kenai.jaffl.annotations.In;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * @author Moyang
 * @create 2018-10-09 测试OpenTSDB数据库
 */
public class TSDBPutTest {


    static BasicDataSource bs = new BasicDataSource();
    static int THREAD_RUNNING_TIME_SECOND=60*3; //单位秒 测试运行时间
    static int START_YAER = 1990; //开始时间
    static int INDEXNUMBER = 0;


    public static class ImportTSDBThread extends Thread {

        private Object[] list_station;
        private int threadnumber;
        private double[] responsetime;
        private int duration;
        private long[] tx;

        public ImportTSDBThread( Object[] list_station, int threadnumber, double[] responsetime, long[] tx, int duration) {
            this.list_station = list_station;
            this.threadnumber = threadnumber;
            this.responsetime = responsetime;
            this.duration = duration;
            this.tx = tx;
        }

        public void run() {

            long starttime = System.currentTimeMillis();
            long endtime = starttime + duration;
            int txNum = 0, count = 0;
            double avgResponseTime = 0;
            String[] row = new String[6];
            int[] start_date = new int[3];
            start_date[0] = START_YAER;
            start_date[1] = 1;
            start_date[2] = 1;
            RandomValue Ran = new RandomValue();
            //EarthquakeDataEntity e = new EarthquakeDataEntity();
            TestOpenTSDB tsdb = new TestOpenTSDB();
            CountData string_data = new CountData();
            String ThreadName = Thread.currentThread().getName();
            // int ThreadFlag = Integer.parseInt(ThreadName);

            // 建议TRANSACTION_RECORDS条提交一次，如果数据量太大不宜做为一个事务
            // 插入数据单个操作为一个事务,统计每一个事务的耗时，然后求平均值
            int index= threadnumber +INDEXNUMBER;
            String temp_string = list_station[index].toString();
            SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd");
            try {
                long start = System.currentTimeMillis();
                int NUM_PER_TRANSACTION =10; //100条以上出错，数据一次不能太多
                List<EarthquakeDataEntity> earthquakelist = new ArrayList<EarthquakeDataEntity>();
                CloseableHttpClient client = HttpClients.createDefault();// getClient();

                row[0] = temp_string.substring(0, 5);
                row[1] = temp_string.substring(5, 6);
                row[2] = temp_string.substring(6, 10);
                row[3] = "3";
                while (!Thread.interrupted() && System.currentTimeMillis() < endtime) {
                    EarthquakeDataEntity e = new EarthquakeDataEntity();
                    row[4] = string_data.CountData_OpenTSDB(count, start_date);
                    row[5] = Ran.data();

                    e.setStationId(row[0]);
                    e.setPointId(row[1]);
                    e.setItemId(row[2]);
                    //e.setFlag(row[3]);
                    e.setDate(formatter.parse(row[4]));
                    e.setObsValue(Float.valueOf(row[5]));
                    earthquakelist.add(e);
                    if (earthquakelist.size() >= NUM_PER_TRANSACTION)
                    {
                        //NUM_PER_TRANSACTION提交一次
                        long retvalue = tsdb.put2tsdb_multipoints(client,earthquakelist);
                        if (retvalue == -1)
                        {
                            System.out.println(" Thread "+ threadnumber+" fail to add data to opentsdb!");
                        }
                        earthquakelist.clear();
                    }
                    //avgResponseTime += tsdb.put2tsdb_by_one(e);
                    try {
                        Thread.sleep(1); // 避免过度占用CPU
                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    count++;
                    txNum++; // 事务计数
                }
                //提交

                long end =System.currentTimeMillis();
                avgResponseTime = (end -start);
                avgResponseTime = avgResponseTime/txNum;

            }catch (Exception ee){
                ee.printStackTrace();
            }
            //System.gc();
            System.out.println("线程名:" + Thread.currentThread().getName()
                    + "插入数据：" + (count) + " 耗时：" + (avgResponseTime) + "ms");

            responsetime[threadnumber] = avgResponseTime;
            tx[threadnumber] = txNum;

        }
    }



    public static class ImportTSDBQueryThread extends Thread {

        private int threadnumber;
        private double [] responsetime; //平均响应时间，用于返回数据
        private long [] tx;   //事务吞吐量，用于返回数据
        Object[] list_station = null;
        int retrievalnum ;

        public ImportTSDBQueryThread(Object[] list_station,int threadnumber,int retrievalnum, double []responsetime,long []tx) {
            this.list_station = list_station;
            this.threadnumber = threadnumber;
            this.responsetime =responsetime;
            this.tx  = tx;
            this.retrievalnum = retrievalnum;
        }

        public void run() {


            long starttime = System.currentTimeMillis();
            long endtime = starttime+THREAD_RUNNING_TIME_SECOND*1000;
            int txNum =0,count =0 ;
            double avgResponseTime =0;
            int[] start_date = new int[3];
            start_date[0] = 2000;
            start_date[1] = 1;
            start_date[2] = 1;
            int randomday = 0;
            CountData string_data = new CountData();
            TestOpenTSDB tsdb = new TestOpenTSDB();
            SimpleDateFormat formattype = new SimpleDateFormat("yyyyMMdd");
            Random random = new Random();
            String startrow = "";
            String endrow="";
            Long endday = string_data.Timestamp_OpenTSDB(randomday+retrievalnum, start_date);
            Long startday=string_data.Timestamp_OpenTSDB(randomday, start_date);

            // 创建客户端
            CloseableHttpClient client = HttpClients.createDefault();// getClient();
            try{

                while(!Thread.interrupted() && System.currentTimeMillis() < endtime) {

                    //randomday = random.nextInt(20*365);//大概20年时间

                    startrow = list_station[threadnumber+INDEXNUMBER] + "3" + startday;

                    //System.out.println("Thread "+threadnumber+ " get rowkey from"+startrow+" to "+endrow);

                    avgResponseTime  += tsdb.getDataFromTsdb(client,startrow,endday);

                    try {
                        Thread.sleep(1); //避免过度占用CPU
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    txNum ++; //事务计数
                }
                Date sdt = new Date(startday);
                String strdt = formattype.format(sdt);
                Date edt = new Date(endday);
                String stret = formattype.format(edt);
                String ss = list_station[threadnumber+INDEXNUMBER] + "3" + strdt;
                String se = list_station[threadnumber+INDEXNUMBER] + "3" + stret;
                System.out.println("Thread "+threadnumber+ " get rowkey from"+ss+" to "+se);
                avgResponseTime = avgResponseTime/txNum;
                responsetime[threadnumber] = avgResponseTime;
                tx[threadnumber] = txNum;
            }
            catch(Exception e){
                e.printStackTrace();
            }finally {

                System.gc();
            }

        }
    }

    /*
     * Mutil thread insert test 多客户端并发插入测试
     */
    public void MultThreadInsert(int ThreadNumber) throws InterruptedException {
        try {

            System.out.println("---------开始OpenTSDB MultThreadInsert测试----------");
            System.out.println(START_YAER);
            System.out.println(INDEXNUMBER);
            Station station = new Station();

            Thread[] threads = new Thread[ThreadNumber];
            // 记录各个线程的内各个操作的平均响应时间测试
            double[] ts = new double[ThreadNumber];
            // 各个线程的事务吞吐量
            long[] tx = new long[ThreadNumber];
            // 台站 测点 测项 信息
            Object[] list_station = station.all_date();
            for (int i = 0; i < threads.length; i++) {
                String name = String.valueOf(i);
  /*              at java.text.DateFormat.parse(DateFormat.java:366)
                at TSDBPutTest$ImportTSDBThread.run(TSDBPutTest.java:83)
                线程名:7插入数据：0 耗时：0.0msjava.text.ParseException: Unparseable date: "1990-02-01"
*/
                threads[i] = new ImportTSDBThread(list_station, i, ts, tx, THREAD_RUNNING_TIME_SECOND*1000);
                threads[i].start();
                threads[i].setName(name);
            }
            for (int j = 0; j < threads.length; j++) {
                (threads[j]).join();
            }


            // 计算平均耗时
            double tts = 0;
            double ttx = 0;
            String strtts = "";
            for (int j = 0; j < threads.length; j++) {
                tts += ts[j];
                strtts += String.format("%.2f", ts[j]) + ",";
                ttx += tx[j];
            }
            tts = tts * 1.0 / threads.length;
            ttx = ttx *1.0 /THREAD_RUNNING_TIME_SECOND  ;
            //String.format("%.2f", f)
            System.out.println("MultThreadInsert：各个线程耗时:" + strtts + ",平均响应时间："
                    +String.format("%.2f", tts)  + "ms" + ",事务吞吐量:" + String.format("%.2f tx/s", ttx) );


            System.out.println("---------结束OpenTSDB MultThreadInsert测试----------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Mutl thread query Record test 测试多线程并发查询
     */
    public void MultThreadQuery(int ThreadNumber, int retrievalnum)
            throws InterruptedException {
        System.out.println("---------开始TSDB MultThreadQuery测试----------");
        // long start = System.currentTimeMillis();
        Thread[] threads = new Thread[ThreadNumber];
        double []ts = new double[ThreadNumber];
        Station station = new Station();
        Object[] list_station = station.all_date();
        // 记录各个线程的时间
        long []tx = new long[ThreadNumber];
        for (int i = 0; i < threads.length; i++) {
            String name = String.valueOf(i);
            threads[i] = new ImportTSDBQueryThread(list_station,i,retrievalnum,ts,tx);
            threads[i].start();
            threads[i].setName(name);
        }
        for (int j = 0; j < threads.length; j++) {
            (threads[j]).join();
        }
        // long stop = System.currentTimeMillis();

        // 计算平均耗时
        //计算平均耗时
        double tts =0;
        double ttx=0;
        String strtts="",strttx="";
        for(int j=0;j< threads.length;j++)
        {
            tts +=ts[j];
            strtts +=String.format("%.2f", ts[j]) +",";
            ttx +=tx[j];
        }
        //String.format("%.2f", tts)
        tts = tts *1.0/threads.length;
        ttx = ttx *1.0/THREAD_RUNNING_TIME_SECOND;


        System.out.println("MultThreadQueryRecords:各个线程耗时："+strtts+",平均耗时："+ String.format("%.2f", tts)+"ms" +"事务吞吐量"+String.format("%.2f", ttx)+" tx/sec");



        System.out.println("---------结束TSDB MultThreadQuery测试----------");
    }

    public static void main(String[] args) throws Exception {
        TSDBPutTest tsdbPutTest = new TSDBPutTest();

        //test for svn
        if ("insert".contentEquals(args[0])) {

            if (args.length==3)
                THREAD_RUNNING_TIME_SECOND = Integer.parseInt(args[2]);
            if (args.length==5){
                THREAD_RUNNING_TIME_SECOND = Integer.parseInt(args[2]);
                START_YAER = Integer.parseInt(args[3]);
                INDEXNUMBER = Integer.parseInt(args[4]);}
            tsdbPutTest.MultThreadInsert(Integer.parseInt(args[1]));


        } else if ("multQuery".contentEquals(args[0])) {

            tsdbPutTest.MultThreadQuery(Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        }

    }

}