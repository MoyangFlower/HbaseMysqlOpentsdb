
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import com.alibaba.fastjson.JSONArray;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;








import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import sun.awt.SunHints;

public class TestOpenTSDB {
    static Random random = new Random();
    public static String OPENTSDB_URL_PUT="http://asterix-2:8888/api/put?summary";
    public static String OPENTSDB_URL_QUERY="http://asterix-2:8888/api/query";
	/*
	"{
    "start": 1356998400,
    "end": 1356998460,
    "queries": [
        {
            "aggregator": "sum",
            "metric": "sys.cpu.0",
            "rate": "true",
            "filters": [
                {
                   "type":"wildcard",
                   "tagk":"host",
                   "filter":"*",
                   "groupBy":true
                },
                {
                   "type":"literal_or",
                   "tagk":"dc",
                   "filter":"lga|lga1|lga2",
                   "groupBy":false
                }
            ]
        },
        {
            "aggregator": "sum",
            "tsuids": [
                "000001000002000042",
                "000001000002000043"
            ]
        }
    ]
}";

	*/

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        //测试时需要将该项目用到的jar一起拷贝到目标服务器asterix hadoop用户下的testopentsdb目录，
        //然后执行java -classpath  jar文件 cidp.TestOpenTSDB，可以插入数据到OpenTSDB

        //Long timestamp = (new Date()).getTime()/1000;
        //System.out.println(timestamp);
        List<EarthquakeDataEntity> data = new ArrayList<EarthquakeDataEntity>();
        Date start = new Date();
        Long t = start.getTime();
        Date obsdata;
        System.out.println("ts:"+start);
        Random r =new Random();
        CloseableHttpClient client =HttpClients.createDefault();
        //for (int m =0 ; m <3*24*60 ; m++)
        {
            for (int i =0 ;i < 60 ;i ++)
            {
                EarthquakeDataEntity e = new EarthquakeDataEntity();
                e.setStationId("10001");
                e.setPointId("A");
                e.setItemId("4112");
                e.setObsValue(r.nextInt(100));
                System.out.println("ts second:"+t/1000);
                e.setDate(t);
                t += 1000; //加一秒
                data.add(e);
            }

            try {
                //调用下属方法，将数据存放到opentsdb上
                put2tsdb(client,data);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * 发送post请求
     *
     * @param url
     *
     * @param data
     *            json类型字符串数据
     * @return 响应内容
     */
    public static boolean postReturnBoolean(CloseableHttpClient client,String url, String data)
            throws Exception {
        // 成功响应码开头
        final String successCodeHead = "2";
        // 创建客户端
        // HTTP响应、响应状态码
        CloseableHttpResponse response = null;
        Integer statusCode = null;
        String responseContent = null;

        HttpPost post = new HttpPost(url);
        // 构造请求数据
        StringEntity entity = new StringEntity(data,
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);

        try {
            //System.out.println("send post:\r\n"+post +"\r\ndata="+data);
            // 执行post请求
            response = client.execute(post);
            // 获取响应状态码
            statusCode = response.getStatusLine().getStatusCode();
            //System.out.println("response code:" + statusCode);
            if (!successCodeHead.equals(statusCode.toString().substring(0, 1))) {
                HttpEntity httpEntity = response.getEntity();
                responseContent = EntityUtils.toString(httpEntity, "UTF-8");
                System.out.println("return:\r\n"+responseContent);

                JSONObject error = JSON.parseObject(responseContent);

                String errorMessage = error.getJSONObject("error").getString(
                        "message");
                System.out.println(errorMessage);
                throw new Exception(errorMessage);
            } else {
                return true;
            }
        } catch (ClientProtocolException e) {
            System.out.println("Http Client ClientProtocolException"+ e);
        } catch (IOException e) {
            System.out.println("Http Client IOException"+ e);
        } finally {
            try {
                response.close();
            } catch (Exception e) {
                System.out.println("响应关闭失败"+ e);
            }
        }
        return false;
    }


    public static String postReturnString(CloseableHttpClient client,String url, String data)
            throws Exception {
        // 成功响应码开头
        final String successCodeHead = "2";

        HttpPost post = new HttpPost(url);
        // 构造请求数据
        StringEntity entity = new StringEntity(data, ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        // HTTP响应、响应状态码
        CloseableHttpResponse response = null;
        Integer statusCode = null;
        String responseContent = null;
        try {
            // 执行post请求
            response = client.execute(post);
            // 获取响应状态码
            statusCode = response.getStatusLine().getStatusCode();
            //System.out.println("response code:" + statusCode);
            if (!successCodeHead.equals(statusCode.toString().substring(0, 1))) {
                HttpEntity httpEntity = response.getEntity();
                JSONObject error = JSON.parseObject(EntityUtils.toString(httpEntity, "UTF-8"));

                String errorMessage = error.getJSONObject("error").getString(
                        "message");
                System.out.println(errorMessage);
                throw new Exception(errorMessage);
            } else {
                HttpEntity httpEntity = response.getEntity();
                responseContent = EntityUtils.toString(httpEntity, "UTF-8");


            }
        } catch (ClientProtocolException e) {
            System.out.println("Http Client ClientProtocolException"+ e);
        } catch (IOException e) {
            System.out.println("Http Client IOException"+ e);
        } finally {
            try {
                response.close();
            } catch (Exception e) {
                System.out.println("响应关闭失败"+ e);
            }
        }
        return responseContent;
    }

    //需要引入 fastjson包

    public static boolean put2tsdb(CloseableHttpClient client,List<EarthquakeDataEntity> data) throws Exception {
        List<OpenTsdbDataEntity> putData = new ArrayList<OpenTsdbDataEntity>();
        //转为opentsdb标准数据格式
        for (EarthquakeDataEntity earthquake : data) {
            //获取属性，Mertic
            String itemId = earthquake.getItemId();

            //获取两个其他属性，做tags
            String stationId= earthquake.getStationId();
            String pointId= earthquake.getPointId();
            //时间戳 现在到秒，不是毫秒
            Long timestamp = earthquake.getTimestamp()/1000;
            //值
            Float value = earthquake.getObsValue();
            //转为Optsdb格式
            OpenTsdbDataEntity valueData = new OpenTsdbDataEntity();
            valueData.setMetric(itemId);
            HashMap<String, String> tags = new HashMap<String, String>();
            tags.put("st", stationId);
            tags.put("pt", pointId);
            valueData.setTags(tags);
            valueData.setTimestamp(timestamp);
            valueData.setValue(value);
            putData.add(valueData);
        }

        String dataJson = JSON.toJSONString(putData);
        //显示拼装数据的json,便于监控
        //System.out.println("输入openTSDB的数据是"+dataJson);

        //通过Http API发送数据  URL_PUT=IP:端口/api/put
        boolean isSuccessful = postReturnBoolean(client,OPENTSDB_URL_PUT, dataJson);
        //返回是否发送成功标志
        return isSuccessful;
    }

    public long put2tsdb_by_one(CloseableHttpClient client,EarthquakeDataEntity earthquake) throws Exception {
        long start = System.currentTimeMillis();
        //转为opentsdb标准数据格式
        //获取属性，Mertic
        String itemId = earthquake.getItemId();
        //获取两个其他属性，做tags
        String stationId= earthquake.getStationId();
        String pointId= earthquake.getPointId();
        //时间戳 现在到天，不是秒
        Long timestamp = earthquake.getTimestamp();
        //值
        Float value = earthquake.getObsValue();
        //转为Optsdb格式
        OpenTsdbDataEntity valueData = new OpenTsdbDataEntity();
        valueData.setMetric(itemId);
        HashMap<String, String> tags = new HashMap<String, String>();
        tags.put("st", stationId);
        tags.put("pt", pointId);
        valueData.setTags(tags);
        valueData.setTimestamp(timestamp);
        valueData.setValue(value);
        String dataJson = JSON.toJSONString(valueData);
        //显示拼装数据的json,便于监控
        System.out.println("输入openTSDB的数据是"+dataJson);

        //通过Http API发送数据  URL_PUT=IP:端口/api/put
        boolean isSuccessful = postReturnBoolean(client,OPENTSDB_URL_PUT, dataJson);
        long end = System.currentTimeMillis();
        //返回是否发送成功标志
        if(isSuccessful){
            return(end-start);
        }
        else return 0;
    }

    /**
     * 批量发送某个台站观测数据，
     * @param earthquake
     * @return
     * @throws Exception
     */
    public long put2tsdb_multipoints(CloseableHttpClient client,int host,List<EarthquakeDataEntity> earthquake) throws Exception {
        long start = System.currentTimeMillis();
        //转为opentsdb标准数据格式
        //获取属性，Mertic
        //String itemId = earthquake.getItemId();
        //获取两个其他属性，做tags
        //String stationId= earthquake.getStationId();
        //String pointId= earthquake.getPointId();
        //时间戳 现在到天，不是秒
        // Long timestamp = startDate.getTime();//earthquake.getDate().getTime();
        //值
        List<OpenTsdbDataEntity> list =new ArrayList<OpenTsdbDataEntity>();
        for (int i=0 ;i < earthquake.size() ;i++)
        {
            //转为Optsdb格式
            OpenTsdbDataEntity valueData = new OpenTsdbDataEntity();
            valueData.setMetric( earthquake.get(i).getItemId());
            HashMap<String, String> tags = new HashMap<String, String>();
            tags.put("st",  earthquake.get(i).getStationId());
            tags.put("pt",  earthquake.get(i).getPointId());
            valueData.setTags(tags);
            valueData.setTimestamp( earthquake.get(i).getTimestamp());
            valueData.setValue(earthquake.get(i).getObsValue());
            list.add(valueData);
        }
        String dataJson = JSON.toJSONString(list);
        //显示拼装数据的json,便于监控
        //System.out.println("输入openTSDB的数据是"+dataJson);
        //通过Http API发送数据  URL_PUT=IP:端口/api/put
        OPENTSDB_URL_PUT="http://asterix-"+String.valueOf(host)+":8888/api/put?summary";
        boolean isSuccessful = postReturnBoolean(client,OPENTSDB_URL_PUT, dataJson);
        long end = System.currentTimeMillis();
        //返回是否发送成功标志
        if(isSuccessful){
            return(end-start);
        }
        else
            return -1;
    }

    public long getDataFromTsdb(CloseableHttpClient client ,int host,String quary_string,Long endtime) throws Exception {

        long start = System.currentTimeMillis();
        //转为opentsdb标准数据格式

        String stationId= quary_string.substring(0, 5);
        String pointId= quary_string.substring(5, 6);
        String itemId = quary_string.substring(6, 10);
        String starttime = quary_string.substring(11);
        Long starttimestamp = Long.valueOf(starttime);
        Long endtimestamp = endtime;

        //转为Optsdb格式
/*        OpenTsdbQueryDataEntity valueData = new OpenTsdbQueryDataEntity();

        HashMap<String, String> tags = new HashMap<String, String>();
        tags.put("st", stationId);
        tags.put("pt", pointId);

        HashMap<String, String> queries = new HashMap<String, String>();
        queries.put("aggregator","sum");
        queries.put("metric",itemId);
        queries.putAll(tags);
        valueData.setQueries(queries);
        valueData.setStart(starttimestamp);
        valueData.setEnd(endtimestamp);*/



        //直接生成json
/*   json = {
             "start": 1490586530,
             "end": 1489836195,
             "queries": [
                    {
                    "aggregator": "sum",
                    "metric": "9130",
                    "tags": {"st": "03002","pt": "1"}
                     },
                  ]
             }*/


        JSONObject tags = new JSONObject();
        tags.put("st", stationId);
        tags.put("pt", pointId);


        JSONObject queries = new JSONObject();
        queries.put("aggregator", "avg");
        queries.put("metric", itemId);
        queries.put("Downsampler","10d-avg");
        queries.put("tags",tags);

        JSONArray jsonArray = new JSONArray();

        jsonArray.add(0, queries);



        JSONObject result = new JSONObject();
        result.put("start", starttimestamp);
        result.put("end", endtimestamp);
        result.put("queries", jsonArray);


        String dataJson = JSON.toJSONString(result);
        //显示拼装数据的json,便于监控
        System.out.println("输入openTSDB的数据是"+dataJson);

        //通过Http API发送数据  URL_PUT=IP:端口/api/put
        OPENTSDB_URL_QUERY="http://asterix-"+String.valueOf(host)+":8888/api/query";
        String Content = postReturnString(client,OPENTSDB_URL_QUERY, dataJson);
        //返回是否发送成功标志
        long end = System.currentTimeMillis();
        System.out.println("查询的结果是：:"+Content);
        return end-start;
    }



}
