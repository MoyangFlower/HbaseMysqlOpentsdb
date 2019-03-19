package ProductData;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class CountData {
    private SimpleDateFormat formattype = new SimpleDateFormat("yyyyMMdd");
    private SimpleDateFormat formattype_hour = new SimpleDateFormat("yyMMddHH");
    private SimpleDateFormat formattype_minute = new SimpleDateFormat("yyMMddHHmm");
    private SimpleDateFormat formattype_second = new SimpleDateFormat("yyMMddHHmmss");
    private SimpleDateFormat formattype_opentsdb = new SimpleDateFormat("yyyy-MM-dd");
    private  Calendar cld = Calendar.getInstance();
    public String CountData(int count, int[] date) {


        cld.set(Calendar.YEAR, date[0]);
        cld.set(Calendar.MONDAY,date[1]);
        cld.set(Calendar.DATE,date[2]);

        //调用Calendar类中的add()，增加时间量
        cld.add(Calendar.DATE, count);
        Date dt = cld.getTime();
        String time_format = formattype.format(dt);
        return (time_format);
    }

    public String CountData_Hour_min_sec(int count, int[] date,int flag) {


        cld.set(Calendar.YEAR, date[0]);
        cld.set(Calendar.MONDAY,date[1]);
        cld.set(Calendar.DATE,date[2]);
        cld.set(Calendar.HOUR,date[3]);
        cld.set(Calendar.MINUTE,date[4]);
        cld.set(Calendar.SECOND,date[5]);


        String time_format = "";
        switch (flag){
            case 0:{
                //调用Calendar类中的add()，增加时间量
                cld.add(Calendar.SECOND, count);
                Date dt = cld.getTime();
                time_format = formattype_second.format(dt);}
                break;
            case 1:{
                //调用Calendar类中的add()，增加时间量
                cld.add(Calendar.MINUTE, count);
                Date dt = cld.getTime();
                time_format = formattype_minute.format(dt);}
                break;
            case 2:{
                //调用Calendar类中的add()，增加时间量
                cld.add(Calendar.HOUR, count);
                Date dt = cld.getTime();
                time_format = formattype_hour.format(dt);}
                break;
            case 3:{
                //调用Calendar类中的add()，增加时间量
                cld.add(Calendar.DATE, count);
                Date dt = cld.getTime();
                time_format = formattype.format(dt);}
            break;
        }

        return (time_format);
    }

    public Long Timestamp_OpenTSDB(int count, int[] date) {


        cld.set(Calendar.YEAR, date[0]);
        cld.set(Calendar.MONDAY,date[1]);
        cld.set(Calendar.DATE,date[2]);
        cld.set(Calendar.HOUR,0);
        cld.set(Calendar.MINUTE,0);
        cld.set(Calendar.SECOND,0);

        //调用Calendar类中的add()，增加时间量
        cld.add(Calendar.DATE, count);
        Long timestamp = cld.getTimeInMillis();
        return (timestamp);
    }
}