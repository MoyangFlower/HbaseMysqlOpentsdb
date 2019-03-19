import java.io.IOException;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import ProductData.*;

import com.kenai.jaffl.annotations.In;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

/**
 * @author Moyang
 * @create 2018-09-23 测试数据库
 */
public class MysqlPutTest {


	// //mysql驱动包名
	// private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
	// //数据库连接地址
	// private static final String URL =
	// "jdbc:mysql://127.0.0.1:3306/Earthquake";
	// //用户名
	// private static final String USER_NAME = "root";
	// //密码
	// private static final String PASSWORD = "hadoop";
	//
	// public static Connection Connection(){
	// Connection connection = null;
	// try {
	//
	// //加载mysql的驱动类
	// Class.forName(DRIVER_NAME);
	// //获取数据库连接
	// connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
	// } catch (Exception e) {
	// e.printStackTrace();
	//
	// }
	// return connection;
	// }
	static BasicDataSource bs = new BasicDataSource();
	static int THREAD_RUNNING_TIME_SECOND=60*3; //单位秒 测试运行时间
    static int START_YEAR=2000; //单位秒 测试运行时间

	public MysqlPutTest()
	{

		Set set = new TreeSet();
		// 二、设置BasicDataSource属性
		// 1、设置四个属性
		bs.setDriverClassName("com.mysql.jdbc.Driver");
		bs.setUrl("jdbc:mysql://172.17.144.125:3306/Earthquake");
		bs.setUsername("root");
		bs.setPassword("hadoop");
		// 2、设置连接是否默认自动提交
		bs.setDefaultAutoCommit(false);
		// 3、设置初始后连接数
		bs.setInitialSize(200);
		// 4、设置最大的连接数
		bs.setMaxActive(500);
		// 5、设置空闲等待时间，获取连接后没有操作开始计时，到达时间后没有操作回收链接
		bs.setMaxIdle(3000);
	}
	public static Connection GetConnection() throws SQLException {
		// 一、实例化BasicDataSource
		Connection connection = null;
		// 三、测试获取连接
		connection = bs.getConnection();
		return connection;
	}

	public void query_all_data() {

		try {
			Connection connection = GetConnection();
			String sql = "SELECT * FROM DZ_DAY";
			PreparedStatement prst = connection.prepareStatement(sql);
			// 结果集
			long start = System.currentTimeMillis();
			int count = 0;
			ResultSet rs = prst.executeQuery();
			while (rs.next()) {
				// System.out.println(rs.getString("stationID") + " " +
				// rs.getString("pointID") + " " +
				// rs.getString("itemID") + " " +
				// rs.getString("flag") + " " +
				// rs.getString("date") + " " +
				// rs.getString("value")
				// );
				count++;
			}
			long end = System.currentTimeMillis();
			rs.close();
			prst.close();
			connection.close();
			System.out.println("count =" + count + " spend time ="
					+ (end - start));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static long quary_data_by_condition(Connection connection,String quary_string, String enddate) throws IOException {

		long spend_time = 0;
		try {
			String ThreadName = Thread.currentThread().getName();
            // 0300239140 3 20010101
			PreparedStatement psql = connection.prepareStatement("SELECT * FROM DZ_DAY WHERE stationID=? AND pointID=? AND itemID=? AND flag=? AND date BETWEEN ? AND ?");
			psql.setString(1, quary_string.substring(0, 5));
			psql.setString(2, quary_string.substring(5, 6));
			psql.setString(3, quary_string.substring(6, 10));
			psql.setString(4, quary_string.substring(10, 11));
			psql.setString(5, quary_string.substring(11));
			psql.setString(6, enddate);
			// 结果集
			long start = System.currentTimeMillis();
			int count = 0;
			ResultSet rs = psql.executeQuery();
			while (rs.next()) {
				// System.out.println(rs.getString("stationID") + " " +
				// rs.getString("pointID") + " " +
				// rs.getString("itemID") + " " +
				// rs.getString("flag") + " " +
				// rs.getString("date") + " " +
				// rs.getString("value")
				// );
				count++;
			}
			long end = System.currentTimeMillis();
			spend_time = end - start;
			rs.close();
			psql.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return spend_time;
	}

	public static long insert_date(Connection connection, String[] row) {
		long start = System.currentTimeMillis();
		try {
			PreparedStatement psql = connection
					.prepareStatement("insert into DZ_DAY (stationID,pointID,itemID,flag,date,value)"
							+ "values(?,?,?,?,?,?)"); // 用preparedStatement预处理来执行sql语句
			psql.setString(1, row[0]); // 给其五个参量分别“赋值”
			psql.setString(2, row[1]);
			psql.setString(3, row[2]);
			psql.setString(4, row[3]);
			psql.setString(5, row[4]);
			psql.setString(6, row[5]);
			psql.executeUpdate(); // 参数准备后执行语句
			psql.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		// finally {
		// System.out.println("Insert date successful !");
		// }
		return (end - start);
	}

	public static void insert_data_by_loop(Connection connection,
										   int[] start_date, int start_num, int end_num,
										   Object[] list_station, int ThreadFlag) throws IOException {
		String[] row = new String[6];
		CountData string_data = new CountData();
		row[0] = "12001";
		row[1] = "C";
		row[2] = "4111";
		row[3] = "3";
		row[4] = "19900124";
		row[5] = "68.4";

		Random random = new Random();
		DecimalFormat decimalFormat = new DecimalFormat(".00");

		for (int count = start_num; count < end_num; count++) {
			String temp_string = list_station[ThreadFlag].toString();

			row[0] = temp_string.substring(0, 5);
			row[1] = temp_string.substring(5, 6);
			row[2] = temp_string.substring(6, 10);
			row[4] = string_data.CountData(count, start_date);
			row[5] = decimalFormat.format(random.nextFloat() * 100);
			insert_date(connection, row);
		}

	}



	public static class ImportSQLThread extends Thread {

		private Object[] list_station;
		private Connection connection;
		private int threadnumber;
		private double[] responsetime;
		private int duration;
		private long[] tx;

		public ImportSQLThread( Object[] list_station,
								int threadnumber, double[] responsetime, long[] tx, int duration) {
			this.list_station = list_station;
			this.connection = connection;
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
			start_date[0] = START_YEAR;
			start_date[1] = 1;
			start_date[2] = 1;
			RandomValue Ran = new RandomValue();
			CountData string_data = new CountData();
			String ThreadName = Thread.currentThread().getName();
			// int ThreadFlag = Integer.parseInt(ThreadName);

			// Connection

			// 建议TRANSACTION_RECORDS条提交一次，如果数据量太大不宜做为一个事务
			// 插入数据单个操作为一个事务,统计每一个事务的耗时，然后求平均值

			String temp_string = list_station[threadnumber].toString();
			try {
				long start = System.currentTimeMillis();
				connection = GetConnection();
				connection.setAutoCommit(false);
				while (System.currentTimeMillis() < endtime) {

					row[0] = temp_string.substring(0, 5);
					row[1] = temp_string.substring(5, 6);
					row[2] = temp_string.substring(6, 10);
					row[3] = "3";
					row[4] = string_data.CountData(count, start_date);
					row[5] = Ran.data();
					avgResponseTime += insert_date(connection, row);
					try {
						Thread.sleep(1); // 避免过度占用CPU
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					count++;
					txNum++; // 事务计数
				}
				//提交
				connection.commit();
				long end =System.currentTimeMillis();
				avgResponseTime = (end -start);
				avgResponseTime = avgResponseTime/txNum;

			} catch (Throwable e) {
				if (connection != null) {
					try {
						connection.rollback();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				System.gc();
			}

			System.out.println("线程名:" + Thread.currentThread().getName()
					+ "插入数据：" + (count) + " 耗时：" + (avgResponseTime) + "ms");

			responsetime[threadnumber] = avgResponseTime;
			tx[threadnumber] = txNum;

		}
	}

	public static class ImportQueryThread extends Thread {

        private int threadnumber;
        private double [] responsetime; //平均响应时间，用于返回数据
        private long [] tx;   //事务吞吐量，用于返回数据
        Object[] list_station = null;
        int retrievalnum ;

		public ImportQueryThread(Object[] list_station,int threadnumber,int retrievalnum, double []responsetime,long []tx) {
            this.list_station = list_station;
            this.threadnumber = threadnumber;
            this.responsetime =responsetime;
            this.tx  = tx;
            this.retrievalnum = retrievalnum;

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
            int randomday = 0;
            CountData string_data = new CountData();
            Random random = new Random();
            String startrow = "";
            String endrow="",endday ="";
            try{

                Connection connection = GetConnection();
                while(!Thread.interrupted() && System.currentTimeMillis() < endtime) {

                    //randomday = random.nextInt(20*365);//大概20年时间
                    //随机产生一个rowkey，进行查询操作
                    startrow = list_station[threadnumber] + "3" + string_data.CountData(randomday, start_date);
                    endday = string_data.CountData(randomday+retrievalnum, start_date);
                    endrow = list_station[threadnumber] + "3" +endday;
                    //System.out.println("Thread "+threadnumber+ " get rowkey from"+startrow+" to "+endrow);

                    avgResponseTime  += quary_data_by_condition(connection,startrow,endday);

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
            catch(Exception e){
                e.printStackTrace();
            }
        }
	}

	/*
	 * Mutil thread insert test 多客户端并发插入测试
	 */
	public void MultThreadInsert(int ThreadNumber) throws InterruptedException {
		try {
			//Connection connection = GetConnection();
			System.out.println("---------开始MultThreadInsert测试----------");
			Station station = new Station();

			Thread[] threads = new Thread[ThreadNumber];
			// 记录各个线程的内各个操作的平均响应时间测试
			double[] ts = new double[ThreadNumber];
			// 各个线程的事务吞吐量
			long[] tx = new long[ThreadNumber];

			Object[] list_station = station.query_date(ThreadNumber);

			for (int i = 0; i < threads.length; i++) {
				String name = String.valueOf(i);

				threads[i] = new ImportSQLThread(list_station, i, ts, tx, THREAD_RUNNING_TIME_SECOND*1000);
				threads[i].start();
				threads[i].setName(name);
			}
			for (int j = 0; j < threads.length; j++) {
				(threads[j]).join();
			}
			//connection.close();

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


			System.out.println("---------结束MultThreadInsert测试----------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Mutl thread query Record test 测试多线程并发查询MultThreadQuery
	 */
    public void MultThreadQuery(int ThreadNumber, int retrievalnum) throws InterruptedException
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

        for(int i=0;i<threads.length;i++)
        {
            String name =" Thread "+ String.valueOf(i);
            threads[i]= new ImportQueryThread(list_station,i,retrievalnum,ts,tx);
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


	public static void main(String[] args) throws Exception {
		MysqlPutTest mysqlPutTest = new MysqlPutTest();
		mysqlPutTest.MultThreadQuery(200,3650);
		//test for svn
//		if ("insert".contentEquals(args[0])) {
//			//args[1]=线程数
//            if (args.length==3){
//                THREAD_RUNNING_TIME_SECOND = Integer.parseInt(args[2]);
//            }
//			if (args.length==4){
//				THREAD_RUNNING_TIME_SECOND = Integer.parseInt(args[2]);
//                START_YEAR = Integer.parseInt(args[3]);
//			}
//            mysqlPutTest.MultThreadInsert(Integer.parseInt(args[1]));
//
//		} else if ("quary".contentEquals(args[0])) {
//			mysqlPutTest.query_all_data();
//
//		} else if ("multQuery".contentEquals(args[0])) {
//            //多线程查询多条记录 args[1]=线程数 args[2]为每个线程查询条数
//            if (args.length>3)
//                START_YEAR = Integer.parseInt(args[3]);
//            mysqlPutTest.MultThreadQuery(Integer.parseInt(args[1]),Integer.parseInt(args[2]));
//		}

	}

}