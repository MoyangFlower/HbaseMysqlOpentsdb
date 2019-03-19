package ProductData;

import com.kenai.jaffl.annotations.In;

import java.sql.*;
import java.util.*;

/**
 * @author Moyang
 * @create 2018-09-23
 * 返回 num_of_record 个 '台站+测点+测项'
 */
public class Station {

    //mysql驱动包名
    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    //数据库连接地址
    private static final String URL = "jdbc:mysql://211.71.233.113:3306/earthquake";
    //用户名
    private static final String USER_NAME = "root";
    //密码
    private static final String PASSWORD = "Dizhen1001@1003";



    public  Object[] query_date(int num_of_record) {

        Connection connection = null;
        Set set =new TreeSet();
        try {

            //加载mysql的驱动类
            Class.forName(DRIVER_NAME);
            //获取数据库连接
            connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);

            String sql = "SELECT * from QZ_DICT_STATION_POINT where STATIONID is not NULL";
            PreparedStatement prst = connection.prepareStatement(sql);
            //结果集

            ResultSet rs = prst.executeQuery();

            while (rs.next() && set.size()<num_of_record) {

                String StationPoint = rs.getString("STATIONID");
                if (StationPoint.length()==4){
                    StationPoint = "0" +StationPoint;
                }
                set.add(StationPoint + rs.getString("POINTID")+ rs.getString("ITEMID"));

            }
            rs.close();
            prst.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();

        }
        System.out.println(set);
        return set.toArray();
    }

    public  Object[] all_date() {

        Connection connection = null;
        Set set =new TreeSet();
        try {

            //加载mysql的驱动类
            Class.forName(DRIVER_NAME);
            //获取数据库连接
            connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);

            String sql = "SELECT * from QZ_DICT_STATION_POINT where STATIONID is not NULL";
            PreparedStatement prst = connection.prepareStatement(sql);
            //结果集

            ResultSet rs = prst.executeQuery();

            while (rs.next()&&set.size()<3000) {

                String StationPoint = rs.getString("STATIONID");
                if (StationPoint.length()==4){
                    StationPoint = "0" +StationPoint;
                }
                set.add(StationPoint + rs.getString("POINTID")+ rs.getString("ITEMID"));

            }
            rs.close();
            prst.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();

        }
        //System.out.println(set);
        return set.toArray();
    }



    public static void main(String[] args){

        //mysql查询语句
        Station ss =new Station();
        Object[] station= ss.all_date();
        String ss1=station[1]+"3"+"949370829406";
        StringBuilder out=new StringBuilder();
        for (int i=0;i<250;i++)
            if (i%10==0) out.append("'"+station[i]+"',");
        System.out.println(out);

    }


}