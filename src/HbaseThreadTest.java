import ProductData.CountData;
import ProductData.RandomValue;
import ProductData.Station;

import com.sun.org.apache.xpath.internal.operations.Mult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.jruby.RubyProcess;
import sun.awt.SunHints;

public class HbaseThreadTest {

    private static Configuration HBASE_CONFIG = HBaseConfiguration.create();
    //创建300个线程池
    private static ExecutorService pool= Executors.newFixedThreadPool(300);

    public static String tableName = "earthquakedata";
    static Connection conn =null;
    static int THREAD_RUNNING_TIME_SECOND=60*3; //单位秒 测试运行时间
    static int TIME_FLAG=0;

   public static Connection getConn(){
        int i =0;

        String zkHost = "asterix-2:2181,asterix-4:2181,asterix-6:2181,asterix-8:2181,asterix-10:2181";
        HBASE_CONFIG.set("hbase.zookeeper.quorum", zkHost);
        do{
            try {
                conn=ConnectionFactory.createConnection(HBASE_CONFIG, pool);
                if(conn!=null){
                    break;
                }
                Thread.sleep(120);
                i++;
            } catch(InterruptedException e){
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }while(conn==null&&i<5);
    return conn;
    }
    /*
     * Insert Test single thread
     * */
    public static void SingleThreadInsert()throws IOException
    {
        System.out.println("---------开始SingleThreadInsert测试----------");

        //HTableInterface table = null;
        Connection conn = getConn();
        Table table = conn.getTable(TableName.valueOf(tableName),pool);


        //构造测试数据
        List<Put> list = new ArrayList<Put>();
        int count = 10000;
        RandomValue rand = new RandomValue();
        long start = System.currentTimeMillis();
        for(int i=1;i<=count;i++)
        {
            Put put = new Put(String.format("row %d",i).getBytes());
            put.addColumn("Day".getBytes(), "D".getBytes(), rand.data().getBytes());
            //wal=false
            put.setWriteToWAL(false);
            list.add(put);
            if(i%10000 == 0)
            {
                table.put(list);
                put.setWriteToWAL(true);
                list.clear();
            }
        }
        long stop = System.currentTimeMillis();
        //System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);

        System.out.println("插入数据："+count+"共耗时："+ (stop - start)*1.0/1000+"s");

        System.out.println("---------结束SingleThreadInsert测试----------");
    }

    /**
     * 插入单条数据函数
     * @param table rowkey value
     */
    public static double InsertData(HTableInterface table, String rowkey, String value) {
        //String rowkey  = "34003B9130320010101";
        //String value = "84.26";
        long start = System.currentTimeMillis();
        Put put = new Put(rowkey.getBytes());
        put.addColumn("Day".getBytes(),"D".getBytes(),value.getBytes());
        try{
            table.put(put);
            put = null;
        } catch (Exception e){
            e.printStackTrace();
        }
        long end =System.currentTimeMillis();
        return (end-start);

    }


    /**
     * 多线程环境下线程插入函数
     *
     * */
    public static long InsertProcess(int startnum,int endnum,Object[] list_station)throws IOException
    {

        Connection conn = getConn();
        Table table = conn.getTable(TableName.valueOf(tableName),pool);
        table.setWriteBufferSize(24*1024*1024);
        //构造测试数据

        //int count = 10000;
        String rowkey = "13002B91303200010101";
        String value = "32.63";
        int[] start_date = new int[3];
        start_date[0] = 1990;
        start_date[1] = 1;
        start_date[2] = 1;


        RandomValue rand = new RandomValue();
        CountData data = new CountData();
        String ThreadName = Thread.currentThread().getName();
        int ThreadFlag = Integer.parseInt(ThreadName);
        //ThreadName = (ThreadFlag<10)?("0"+1):String.valueOf(ThreadFlag);

        long start = System.currentTimeMillis();
        for(int i=startnum;i<=endnum;i++)
        {
            rowkey = list_station[ThreadFlag].toString()+"3" +data.CountData(i,start_date);
            value= rand.data();

            Put put = new Put(rowkey.getBytes());
            put.addColumn("Day".getBytes(), "D".getBytes(),value.getBytes() );
            //wal=false
            put.setWriteToWAL(false);

            if((endnum-startnum)%10000 == 0)
            {
                table.put(put);
            }
        }


        long stop = System.currentTimeMillis();
        //System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);

      //  System.out.println("线程名:"+Thread.currentThread().getName()+"插入数据："+(endnum-startnum)+"共耗时："+ (stop - start)+"ms");
        return stop-start;
    }

    /*
     * 多线程环境下线程查询单条记录函数
     *
     * */
    public static long QueryRecordProcess(Table table, String rowkey)throws IOException
    {


        Get row = new Get(Bytes.toBytes(rowkey));
        int count =0;
        long start_time = System.currentTimeMillis();
        Result Result = table.get(row);
        Cell[] cells = Result.rawCells();

        //System.out.println("#######rowkey########Family:Qualifier#########value#######TimeStamp#### ");
        for (Cell cell : cells) {
//            System.out.println(
//                    Bytes.toString(CellUtil.cloneRow(cell))+" "+
//                            Bytes.toString(CellUtil.cloneFamily(cell))+":" +
//                            Bytes.toString(CellUtil.cloneQualifier(cell))+" " +
//                            Bytes.toString(CellUtil.cloneValue(cell))+" " +
//                            cell.getTimestamp());
            count++;
        }
        long end_time =  System.currentTimeMillis();


       // System.out.println("线程名:"+Thread.currentThread().getName()+"查询数据："+(count)+"共耗时："+ (end_time - start_time)+"ms");
        return end_time -start_time;
    }
    /*
     * 多线程环境下线程查询多条记录函数
     *
     * */
    public static long QueryProcess(Table table , String startrow, String endrow)throws IOException
    {


        Scan scan = new Scan();

        long start_time = System.currentTimeMillis();

        scan.addColumn(Bytes.toBytes("Day"),Bytes.toBytes("D"));
        scan.setStartRow( Bytes.toBytes(startrow));                   // start key is inclusive
        scan.setStopRow( Bytes.toBytes(endrow +  (char)0));
        int count =0;
        ResultScanner resultscan = table.getScanner(scan);

        for (Result result : resultscan) {
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
//                System.out.println(
//                        Bytes.toString(CellUtil.cloneRow(cell)) + " " +
//                        Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
//                        Bytes.toString(CellUtil.cloneQualifier(cell)) + " " +
//                        Bytes.toString(CellUtil.cloneValue(cell)) + " " +
//                        cell.getTimestamp());
                count++;
            }
        }
        long end_time =System.currentTimeMillis();

       // System.out.println("线程名:"+Thread.currentThread().getName()+"查询数据："+(count)+"共耗时："+ (end_time - start_time)+"ms");
        return end_time -start_time;
    }


    /*
     * 多线程下插入 事务吞吐量和平均响应时间测试
     * */
    public static void MultThreadInsert(int ThreadNumber) throws InterruptedException
    {
        System.out.println("---------开始MultThreadInsert测试----------");
        // long start = System.currentTimeMillis();
        Station station = new Station();
        Object[] list_station = station.query_date(ThreadNumber);

        Thread[] threads=new Thread[ThreadNumber];
        //记录各个线程的内各个操作的平均响应时间测试
        double []ts = new double[ThreadNumber];
        //各个线程的事务吞吐量
        long []tx = new long[ThreadNumber];
        //最后，对所有的线程事务吞吐量和平均响应时间，取平均值为系统的平均事务吞吐量和平均响应时间

        for(int i=0;i<threads.length;i++)
        {
            String name = String.valueOf(i);
            threads[i]= new ImportThread(list_station,i,ts,tx); //运行1分钟
            threads[i].start();
            threads[i].setName(name);
        }
        for(int j=0;j< threads.length;j++)
        {
            (threads[j]).join();
        }
        // long stop = System.currentTimeMillis();

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
        
        System.out.println("MultThreadInsert：各个线程耗时:"+ strtts+",平均响应时间："+ String.format("%.2f", tts)+"ms"+",吞吐量:"+String.format("%.2f", ttx)+" tx/second");

        System.out.println("---------结束MultThreadInsert测试----------");
    }
    /*
     * Mutl thread query Record test 随机读取一条
     * */
    public static void MultThreadQueryRecord(int ThreadNumber) throws InterruptedException
    {
        System.out.println("---------开始MultThreadQueryRecord测试----------");
        Connection conn = getConn();
        long start = System.currentTimeMillis();
        Thread[] threads=new Thread[ThreadNumber];
        //记录各个线程的时间
        double []ts = new double[ThreadNumber];
        Station station = new Station();
        Object[] list_station = station.query_date(ThreadNumber);
        long []tx = new long[ThreadNumber];
        for(int i=0;i<threads.length;i++)
        {
            String name = " Thread "+ String.valueOf(i);
            threads[i]= new ImportQueryRecordThread(conn,list_station,i,ts,tx);
            threads[i].start();
            threads[i].setName(name);
        }
        for(int j=0;j< threads.length;j++)
        {
            (threads[j]).join();
        }
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



        System.out.println("MultThreadQueryRecord:各个线程耗时："+strtts+",平均耗时："+ String.format("%.2f", tts)+"ms" +"事务吞吐量"+String.format("%.2f", ttx)+" tx/sec");
        System.out.println("---------结束MultThreadQueryRecord测试----------");
    }


    /*
     * Mutl thread query Records test
     * */
    public static void MultThreadQueryRecords(int ThreadNumber, int retrievalnum) throws InterruptedException
    {
        System.out.println("---------开始MultThreadQueryRecords测试----------");
        //long start = System.currentTimeMillis();
        Thread[] threads=new Thread[ThreadNumber];
        //记录各个线程的时间
        double []ts = new double[ThreadNumber];
        Station station = new Station();
        Object[] list_station = station.query_date(ThreadNumber);
        //各个线程的事务吞吐量
        long []tx = new long[ThreadNumber];
        Connection conn = getConn();

        for(int i=0;i<threads.length;i++)
        {
            String name =" Thread "+ String.valueOf(i);
            threads[i]= new ImportQueryThread(conn,list_station,i,retrievalnum,ts,tx);
            threads[i].start();
            threads[i].setName(name);
        }
        for(int j=0;j< threads.length;j++)
        {
            (threads[j]).join();
        }
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


        System.out.println("---------结束MultThreadQueryRecords测试----------");
    }


//多线程插入
    public static class ImportThread extends Thread{


        private Object[] list_station;
        private int threadnumber;
        private double [] responsetime; //平均响应时间，用于返回数据
        private long [] tx;   //事务吞吐量，用于返回数据

        public ImportThread(Object[] list_station,int threadnumber, double []responsetime,long []tx){

            this.list_station =list_station;
            this.threadnumber = threadnumber;
            this.responsetime =responsetime;
            this.tx =tx;
        }

        public void run(){
            HTableInterface table =null;
            try{
//                long starttime = System.currentTimeMillis();
//                long endtime = starttime+duration;
                long starttime = System.currentTimeMillis();
                long endtime = starttime+THREAD_RUNNING_TIME_SECOND*1000;

                int txNum =0,count =0 ;
                double avgResponseTime =0;
                String rowkey = "13002B91303200010101";
                String value = "32.63";
                int[] start_date = new int[3];
                start_date[0] = 1980;
                start_date[1] = 0;
                start_date[2] = 1;


  /*              table = (HTableInterface) pool.getTable(tableName);
                table.setAutoFlushTo(false);
                table.setWriteBufferSize(2*1024*1024);*/
                RandomValue Ran = new RandomValue();
                CountData string_data = new CountData();

                int TRANSACTION_RECORDS = 1000; //每TRANSACTION_RECORDS条，提交一次
                System.out.println("start111");
                Connection conn = HBaseUtil.getConnection();

                long start = System.currentTimeMillis();
                System.out.println("Thread " + threadnumber+" produces data: "+  list_station[threadnumber] + "3");
                String family ="Day";
                String str_column,dec_format="" ;
                int column = 1;
                List<Put> puts = new LinkedList<Put>() ;
                switch (TIME_FLAG){
                    case 0:{
                        dec_format="00000";
                        family = "Second";
                        column = 24*60*60;
                    }break;
                    case 1:{
                        dec_format="0000";
                        family = "Minute";
                        column =24*60;
                    }break;
                    case 2:{
                        dec_format="00";
                        family = "Hour";
                        column = 24;
                    }break;
                    case 3:{
                        family = "Day";
                        column = 1;
                    }break;
                }
                while(!Thread.interrupted() && count<10000) {
                    //插入数据单个操作为一个事务,统计每一个事务的耗时，然后求平均值
                    rowkey = list_station[threadnumber] + String.valueOf(TIME_FLAG) + string_data.CountData(count, start_date);
                    //avgResponseTime += InsertData(table,rowkey,value); 可能 会有多线程并发问题
                    Put put = new Put(rowkey.getBytes());
                    for(int i=0;i<column;i++) {
                        value = Ran.data();
                        str_column = column == 1 ? "D" : String.valueOf(new DecimalFormat(dec_format).format(i));
                        put.addColumn(family.getBytes(), str_column.getBytes(), value.getBytes());
                        puts.add(put);
                        txNum ++; //事务计数
                        if (txNum % TRANSACTION_RECORDS ==0)
                        {
                            HBaseUtil.put(conn,tableName,puts);
                            puts.clear();

                        }



                        try {
                            Thread.sleep(1); //避免过度占用CPU
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            System.out.println("sleep error");
                            e.printStackTrace();
                        }
                    }
                    count++;
                    //if (count % 100 ==0) System.out.println(count);

                }
                //table.flushCommits();
                long end =System.currentTimeMillis();
                avgResponseTime = (end -start);

                System.out.println("线程名:"+Thread.currentThread().getName()+"插入数据："+(txNum)+" 耗时："+ (avgResponseTime)+"ms");

                avgResponseTime = avgResponseTime/txNum;
                //avgResponseTime = InsertProcess(startnum,endnum,list_station);  //需要改造

                responsetime[threadnumber] = avgResponseTime;
                tx[threadnumber] = txNum;

            }
            catch(Exception e){
                System.out.println("Thread error:"+e.getMessage());
               // e.printStackTrace();
            }finally{
               // System.gc();
              /*  try
                {
                    if (table != null)
                     table.close();
                }
                catch(Exception e){
                     e.printStackTrace();
                }*/
            }
        }
    }


    public static class ImportQueryRecordThread extends Thread{

        Connection conn1 = null;
        private String rowkey;
        private int threadnumber;
        private double [] responsetime; //平均响应时间，用于返回数据
        private long [] tx;   //事务吞吐量，用于返回数据
        Object[] list_station = null;
        public ImportQueryRecordThread(Connection conn, Object[] list_station,int threadnumber, double []responsetime,long []tx){
            this.threadnumber = threadnumber;
            this.responsetime =responsetime;
            this.list_station = list_station;
            this.tx =tx;
            this.conn1 = conn;
        }

        public void run(){

            long starttime = System.currentTimeMillis();
            long endtime = starttime+THREAD_RUNNING_TIME_SECOND*1000;
            int txNum =0,count =0 ;
            double avgResponseTime =0;
            int[] start_date = new int[3];
            start_date[0] = 1990;
            start_date[1] = 1;
            start_date[2] = 1;
            int randomday = 0;
            CountData string_data = new CountData();
            Random random = new Random();
            try{

                Table table = conn1.getTable(TableName.valueOf(tableName),pool);

                while(!Thread.interrupted() && System.currentTimeMillis() < endtime) {

                    randomday = random.nextInt(20*365);//大概20年时间
                    //随机产生一个rowkey，进行查询操作
                    rowkey = list_station[threadnumber] + "3" + string_data.CountData(randomday, start_date);

                    avgResponseTime  +=  QueryRecordProcess(table,rowkey);
                    //System.out.println("Thread "+threadnumber+ " get rowkey="+rowkey +"");
                    try {
                        Thread.sleep(1); //避免过度占用CPU
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    txNum ++; //事务计数
                }
                System.out.println("Thread "+threadnumber+ " get rowkey="+rowkey +"");
                avgResponseTime = avgResponseTime/txNum;
                responsetime[threadnumber] = avgResponseTime;
                tx[threadnumber] = txNum;

            }
            catch(IOException e){
                e.printStackTrace();
            }finally{
              //  System.gc();
            }
        }
    }

    public static class ImportQueryThread extends Thread{

        Connection conn1 = null;
        private int threadnumber;
         private double [] responsetime; //平均响应时间，用于返回数据
        private long [] tx;   //事务吞吐量，用于返回数据
        Object[] list_station = null;
        int retrievalnum ;

        /**
         *
         * @param list_station 台站 测点 测项信息
         * @param threadnumber 线程 i
         * @param retrievalnum 设置检索记录返回的条数
         * @param responsetime 响应时间
         * @param tx             事务吞吐量，个数
         */
        public ImportQueryThread(Connection conn,Object[] list_station,int threadnumber,int retrievalnum, double []responsetime,long []tx){
            this.list_station = list_station;
            this.threadnumber = threadnumber;
            this.responsetime =responsetime;
            this.tx  = tx;
            this.retrievalnum = retrievalnum;
            this.conn1 =conn;
        }

        public void run(){

            long starttime = System.currentTimeMillis();
            long endtime = starttime+THREAD_RUNNING_TIME_SECOND*1000;
            int txNum =0,count =0 ;
            double avgResponseTime =0;
            int[] start_date = new int[3];
            start_date[0] = 2000;
            start_date[1] = 1;
            start_date[2] = 1;
            int randomday = 10;
            CountData string_data = new CountData();
            Random random = new Random();
             String startrow = "";
             String endrow ="";
            try{

                Table table = conn1.getTable(TableName.valueOf(tableName),pool);

                while(!Thread.interrupted() && System.currentTimeMillis() < endtime) {

                    //randomday = random.nextInt(20*365);//大概20年时间
                    startrow = string_data.CountData(randomday, start_date);
                    endrow = string_data.CountData(randomday+retrievalnum, start_date);
                    //随机产生一个rowkey，进行查询操作
                    startrow = list_station[threadnumber] + "3" + startrow;
                    endrow = list_station[threadnumber] + "3" + endrow;

                    //System.out.println("Thread "+threadnumber+ " get rowkey from"+startrow+" to "+endrow);

                    avgResponseTime  += QueryProcess(table,startrow,endrow);

                    try {
                        Thread.sleep(1); //避免过度占用CPU
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    txNum ++; //事务计数
                }

                System.out.println("Thread "+threadnumber+ " get rowkey from"+startrow+" to "+endrow);
                avgResponseTime = avgResponseTime/txNum;
                responsetime[threadnumber] = avgResponseTime;
                tx[threadnumber] = txNum;
            }
            catch(IOException e){
                e.printStackTrace();
            }finally{
                //System.gc();
            }
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args)  throws Exception{
        // TODO Auto-generated method stub
        //SingleThreadInsert();

        if ("singleInsert".contentEquals(args[0])){
            SingleThreadInsert();
        }
        else if("multInsert".contentEquals(args[0])) {
            if (args.length>2)
                //THREAD_RUNNING_TIME_SECOND = Integer.parseInt(args[2]);
                TIME_FLAG = Integer.parseInt(args[2]);
            //每次测试之前，应先清除所有的记录
            //多线程插入多条记录 args[1]=线程数
            MultThreadInsert(Integer.parseInt(args[1]));

        }
        else if ("MultThreadQuery".contentEquals(args[0])){
            //多线程查询单条记录 args[1] =线程数
            if (args.length>2)
                THREAD_RUNNING_TIME_SECOND = Integer.parseInt(args[2]);
            MultThreadQueryRecord(Integer.parseInt(args[1]));
        }
        else if ("MultThreadQuerys".contentEquals(args[0])){
            if (args.length>3)
                THREAD_RUNNING_TIME_SECOND = Integer.parseInt(args[3]);
            //测试之前，应先插入足够的记录
            //多线程查询多条记录 args[1]=线程数 args[2]为每个线程查询条数
            MultThreadQueryRecords(Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        }

        System.out.println("Usage:"
                +" HbaseThreadTest multInsert ThreadNum  Runtime(second, optional) \r\n"
                +" HbaseThreadTest MultThreadQuery ThreadNum  Runtime(second, optional) \r\n"
                +" HbaseThreadTest MultThreadQuerys ThreadNum retrievalNum  Runtime(second, optional) \r\n"
        );
    }
}