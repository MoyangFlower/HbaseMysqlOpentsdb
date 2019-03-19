
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.jruby.RubyProcess;


public class test {
    public static String ProduceRowkey(int count) {

        Date dt = new Date();
        Calendar cld = Calendar.getInstance();
        cld.set(Calendar.YEAR, 2000);
        cld.set(Calendar.MONDAY,1);
        cld.set(Calendar.DATE,1);

        //调用Calendar类中的add()，增加时间量
        cld.add(Calendar.DATE, count);
        dt = cld.getTime();
        SimpleDateFormat formattype = new SimpleDateFormat("yyyyMMdd");
        String time_format = formattype.format(dt);
        String return_time = time_format.substring(6) +time_format.substring(4,6)+ time_format.substring(0,4);
        return (return_time);
    }

    public static void main(String[] args){
//        String timr = ProduceRowkey(50);
//        System.out.println(timr);
//        String str="2014/1/11 12:34:25";
//        Long ts=System.currentTimeMillis();
//        System.out.println(System.currentTimeMillis()+600000);
//        System.out.println("1029");
//        System.out.println(Long.valueOf("20000101"));
//        Long starttimestamp=Long.valueOf("1540866275651");
//        Long endtimestamp=Long.valueOf("1540896275651");
//
//        JSONObject tags = new JSONObject();
//        tags.put("st", 1);
//        tags.put("pt", 2);
//
//
//        JSONObject queries = new JSONObject();
//        queries.put("aggregator", "sum");
//        queries.put("metric", 3);
//        queries.put("tags",tags);
//
//        JSONArray jsonArray = new JSONArray();
//
//        jsonArray.add(0, queries);
//
//
//
//        JSONObject result = new JSONObject();
//        result.put("start", starttimestamp);
//        result.put("end", endtimestamp);
//        result.put("queries", jsonArray);
//
//
//        String dataJson = JSON.toJSONString(result);
//        //显示拼装数据的json,便于监控
//        System.out.println("输入openTSDB的数据是"+dataJson);
//        for (int i=0;i<250;i++)
//            System.out.println(new SimpleDateFormat(""));
        String dec_format="00000";
        System.out.println(new DecimalFormat(dec_format).format(80552));
    }
}