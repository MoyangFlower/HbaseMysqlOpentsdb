import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Writelog {
    public static void writeLog(  String msg) {
        String filePath ="";
        String fileName= "log_of_test_hbase.txt";

        PrintWriter logPrint = null;
        try {
            logPrint = new PrintWriter(
                    new FileWriter(filePath + fileName, true), true);
        } catch (IOException e) {
            (new File(filePath)).mkdir();
            try {
                logPrint = new PrintWriter(new FileWriter(filePath + fileName,
                        true), true);
            } catch (IOException ex) {
                logPrint = new PrintWriter(System.err);
                writerErrorInfo(logPrint, ex, "无法打开日志文件：" + filePath + fileName);
            }
        }
        writerLogInfo(logPrint, msg);
    }

    /**
     * 将文本信息写入日志文件
     *
     * @param msg 日志内容
     */
    private synchronized static void writerLogInfo(PrintWriter logPrint,
                                                   String msg) {
        logPrint.println(new SimpleDateFormat("yyyy-MM-dd  hh:mm:ss")
                .format(new Date())
                + "  " + msg);
    }

    /**
     * 将文本信息与异常写入日志文件
     *
     * @param e
     * @param msg
     */
    private synchronized static void writerErrorInfo(PrintWriter logPrint,
                                                     Throwable e, String msg) {
        logPrint.println(new SimpleDateFormat("yyyy-MM-dd  hh:mm:ss")
                .format(new Date())
                + "  " + msg);
        e.printStackTrace(logPrint);
    }
}
