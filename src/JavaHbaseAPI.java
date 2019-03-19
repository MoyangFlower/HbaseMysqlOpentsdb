

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * HBase Java API
 * HBase 1.2.1
 * @author
 *
 */
public class JavaHbaseAPI {
    private static Configuration conf = HBaseConfiguration.create();
    //创建300个线程池
    private static ExecutorService poolx=Executors.newFixedThreadPool(300);

    public Connection getConnection(){
        Connection conn = null;
        int i =0;

        // zookeeper连接信息
        conf.set("hbase.zookeeper.quorum", "asterix-2,asterix-4,asterix-6,asterix-8,asterix-10");
        // 建立对hbase的链接


        do{
            try {
                conn=ConnectionFactory.createConnection(conf, poolx);
                if(conn!=null){
                    break;
                }
                Thread.sleep(100);
                i++;
            } catch(InterruptedException e){
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }while(conn==null&&i<5);



        return conn;
    }

    public static void closeConnection(Connection connection){
        try {
            connection.close();
            poolx.shutdownNow();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException{
        Connection conn = getConnection();
        Admin admin = conn.getAdmin();

        //创建一个命名空间
        NamespaceDescriptor nsd = NamespaceDescriptor.create("hbaseAPI").build();
        admin.createNamespace(nsd); // 创建命名空间
        //admin.deleteNamespace("hbaseAPI"); // 删除命名空间

        TableName tableName = TableName.valueOf("earthquakedata"); // 创建一个tableName
        HTableDescriptor htd = new HTableDescriptor(tableName);
        //创建列簇
        HColumnDescriptor colDesc = new HColumnDescriptor("Day");

        htd.addFamily(colDesc); // 给表增加列簇

        admin.createTable(htd); // 创建表

        closeConnection(conn);
    }


    /**
     * 删除表
     * @throws IOException
     */
    @Test
    public void dropTable() throws IOException{
        Connection conn = getConnection();

        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("earthquakedata");

        admin.disableTable(tableName);  //禁用表
        admin.deleteTable(tableName); //删除表

        closeConnection(conn);
    }

    /**
     * 插入数据
     * @throws IOException
     */
    @Test
    public void putDate() throws IOException{
        Connection conn = getConnection();

        TableName tableName = TableName.valueOf("earthquakedata");

        Put put = new Put(Bytes.toBytes("10002"));

        Table table = conn.getTable(tableName);

        HColumnDescriptor[] hcds = table.getTableDescriptor().getColumnFamilies();

        for (HColumnDescriptor hColumnDescriptor : hcds) {
            String hcd = hColumnDescriptor.getNameAsString();

            if("capi".contentEquals(hcd)){
                put.addColumn(Bytes.toBytes(hcd), Bytes.toBytes("name"), Bytes.toBytes("capi_2_zhangs"));
                put.addColumn(Bytes.toBytes(hcd), Bytes.toBytes("age"), Bytes.toBytes("capi_2_18"));
            } else if ("info".equals(hcd)){
                put.addColumn(Bytes.toBytes(hcd), Bytes.toBytes("name"), Bytes.toBytes("info_2_zhangs"));
                put.addColumn(Bytes.toBytes(hcd), Bytes.toBytes("age"), Bytes.toBytes("info_2_18"));
            }

        }

        // 也可以put List
        //List<Put> puts = new ArrayList<Put>();
        //puts.add(put);
        //table.put(puts);

        table.put(put);

        closeConnection(conn);
    }



    /**
     * 删除数据
     * @throws IOException
     */
    @Test
    public void deleteDate() throws IOException{
        Connection conn = getConnection();
        TableName tableName = TableName.valueOf("earthquakedata");

        Table table = conn.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes("10001"));

        //delete.addFamily(Bytes.toBytes("info"));
        //delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        table.delete(delete);  // 删除

        closeConnection(conn);
    }


    /**
     * get单条查询 不支持全表扫面
     * @throws IOException
     */
    @Test
    public void getDate(String rowkey) throws IOException{
        Connection conn = getConnection();
        Get row = new Get(Bytes.toBytes(rowkey));
        Table table = conn.getTable(TableName.valueOf("earthquakedata"));

       long start = new Date().getTime();
        Result Result = table.get(row);
        Cell[] cells = Result.rawCells();

        System.out.println("#######rowkey########Family:Qualifier#########value#######TimeStamp#### ");
        for (Cell cell : cells) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell))+" "+
                    Bytes.toString(CellUtil.cloneFamily(cell))+":" +
                    Bytes.toString(CellUtil.cloneQualifier(cell))+" " +
                    Bytes.toString(CellUtil.cloneValue(cell))+" " +
                    cell.getTimestamp());
        }
        long end = new Date().getTime();
        System.out.println("spend time = "+ (end-start));
        closeConnection(conn);
    }

    /**
     * 批量Get读取
     */


    public List<String> GetDateByList(List<String> rowkeyList) throws IOException{
        Connection conn = getConnection();
        List<Get> getList = new ArrayList();
        List<String> list =new ArrayList();
        String tableName = "earthquakedata";
        Table table = conn.getTable( TableName.valueOf(tableName));// 获取表
        for (String rowkey : rowkeyList){//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }

        long start = new Date().getTime();
        int count =0;
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        for (Result result : results){//对返回的结果集进行操作
            for (Cell kv : result.rawCells()) {
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                list.add(value);
                count++;
            }
        }
        long end = new Date().getTime();
        System.out.println("count ="+count+" spend time = "+ (end-start));
        closeConnection(conn);
        return list;
    }


    /**
     * scan 全表扫描,范围查找
     * @throws IOException
     */
    @Test
    public void scanDate(String startrow,String endrow) throws IOException{
        Connection conn = getConnection();

        TableName tableName = TableName.valueOf("earthquakedata");

        Table table = conn.getTable(tableName);

        Scan scan = new Scan();


        scan.addColumn(Bytes.toBytes("Day"),Bytes.toBytes("D"));
        scan.setStartRow( Bytes.toBytes(startrow));                   // start key is inclusive
        scan.setStopRow( Bytes.toBytes(endrow +  (char)0));

        long start_time = new Date().getTime();
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
        long end_time =new Date().getTime();
        System.out.println("count ="+count+" spend time = " + (end_time-start_time));

        closeConnection(conn);
    }


}
