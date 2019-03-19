package ProductData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.dbcp.BasicDataSource;


public class GetMysqlConnection {
    public Connection connection=null;
    public void GetConnection(int num_of_record) throws SQLException{
        //一、实例化BasicDataSource
        BasicDataSource bs = new BasicDataSource();
        Set set =new TreeSet();
        //二、设置BasicDataSource属性
        //1、设置四个属性
        bs.setDriverClassName("com.mysql.jdbc.Driver");
        bs.setUrl("jdbc:mysql://127.0.0.1:3306/Earthquake");
        bs.setUsername("root");
        bs.setPassword("hadoop");
        //2、设置连接是否默认自动提交
        bs.setDefaultAutoCommit(false);
        //3、设置初始后连接数
        bs.setInitialSize(10);
        //4、设置最大的连接数
        bs.setMaxActive(200);
        //5、设置空闲等待时间，获取连接后没有操作开始计时，到达时间后没有操作回收链接
        bs.setMaxIdle(3000);

        //三、测试获取连接
        connection = bs.getConnection();
        String sql = "SELECT * from DZ_DAY";
        PreparedStatement prst = connection.prepareStatement(sql);
        //结果集

        ResultSet rs = prst.executeQuery();

        while (rs.next() && (set.size()<num_of_record) ) {

            String StationPoint = rs.getString("stationID ");
            if (StationPoint.length()==4){
                StationPoint = "0" +StationPoint;
            }
            System.out.println(StationPoint + rs.getString("pointID")+ rs.getString("itemID"));
            set.add(StationPoint + rs.getString("pointID")+ rs.getString("itemID"));
        }
        rs.close();
        prst.close();

    }
    public void CloseConnection() throws SQLException{
        connection.close();
    }

    public void commit()throws SQLException{
        connection.commit();

    }

    public static void main(String[] args){

        //mysql查询语句
        GetMysqlConnection connection = new GetMysqlConnection();
        try {
            connection.GetConnection(100);
            connection.CloseConnection();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
