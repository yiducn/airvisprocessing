package org.duyi;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yidu on 9/15/15.
 * procedure:
 * geoCodingGoogle()
 * geoEncodingBaidu()
 * preProcess() from pm25original to pm_preProcess
 * then generate pmdata_year month day
 */
public class AllParsers {
    static final String PATH = "/Users/yidu/dev/airvisprocessing/data/";
    static final String API_KEY_GOOGLE = "AIzaSyBLUzY64m0XvjJVb6nDS8m_KRJy3niuYAc";
    static final String API_KEY_BAIDU = "YoV0MPZh0xZKucqPM1gA19Zp";
    static final String PATH_METEOROLOGICAL_STATIONS = "/Users/yidu/快盘/dataset/气象数据/中国地面气象站逐小时观测资料/stations.csv";
    static final String PATH_CITY_CODE = "/Users/yidu/快盘/dataset/路网数据/citycode.csv";

    public static void main(String[] args){
//        preProcess();
//        removeDuplicate();
//        meteorologicalStationsToMongo();
//        geoCodingGoogle();
        insertCityCode();
    }

    /**
     * 根据城市名添加城市代码
     * 此方法应该在geocoding之后调用
     * https://zh.wikipedia.org/wiki/%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E8%A1%8C%E6%94%BF%E5%8C%BA%E5%88%92%E4%BB%A3%E7%A0%81_(5%E5%8C%BA)
     * note:使用的citycode文件,手动添加了一些城市站点,
     * 存在这样的情况,不同的地方,使用相同的standardcode,例如胶州和胶南
     */
    private static void insertCityCode(){
        //get city codes
        HashMap<String, Integer> nameCode = new HashMap<String, Integer>();
        try {
            List<String> list = FileUtils.readLines(new File(PATH_CITY_CODE));

            for(int i = 1; i < list.size(); i ++){
                StringTokenizer st = new StringTokenizer(list.get(i), ",");
                int code = Integer.parseInt(st.nextToken());
                String name = st.nextToken();
                if(name.endsWith("市"))
                    name = name.substring(0, name.length()-1);
                nameCode.put(name, code);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
//        int index = 0;
//        for(String key : nameCode.keySet()){
//            System.out.println(key + ":"+nameCode.get(key)+ ":"+ (index++));
//
//        }
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection locWithLL = db.getCollection("loc_ll_google");
        MongoCursor cursor = locWithLL.find().iterator();
        Document d;
        while(cursor.hasNext()){
            d = (Document)cursor.next();
            Integer code = nameCode.get(d.getString("city"));
            if(code == null){
                System.out.println(d.getString("city"));
                continue;
            }else{
                locWithLL.updateOne(new Document("_id", d.get("_id")),
                        new Document("$set", new Document("standardcode", code.intValue())));
            }

        }
    }

    /**
     * 将气象站数据处理后写入数据库
     */
    private static void meteorologicalStationsToMongo(){
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection weatherStation = db.getCollection("weather_station");
        try {
            List<String> stations = FileUtils.readLines(new File(PATH_METEOROLOGICAL_STATIONS));
            StringTokenizer st;
            Document d ;
            for(int i = 1; i < stations.size(); i ++){
                String stationInfo = stations.get(i);
                st = new StringTokenizer(stationInfo, ",");
//                System.out.println(stationInfo);
                d = new Document().append("province", st.nextToken())
                        .append("id", st.nextToken())
                        .append("name", st.nextToken())
                        .append("type", getTypeMeteorologicalStations(st.nextToken()))
                        .append("lat", Double.parseDouble(st.nextToken()))
                        .append("lon", Double.parseDouble(st.nextToken()))
                        .append("altPressure", Double.parseDouble(st.nextToken()))
                        .append("altObservation", Double.parseDouble(st.nextToken()));
                weatherStation.insertOne(d);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static int getTypeMeteorologicalStations(String type){
        if(type.equals("基本站"))
            return 1;
        else if(type.equals("基准站"))
            return 2;
        else if(type.equals("一般站"))
            return 3;
        else
            return 0;
    }
    /**
     * This method geocode all stations using Google Geocode API,
     * then store the result into loc_ll_google.
     * This method rely on the collection location,
     * which has all the stations' name and city
     */
    private static void geoCodingGoogle(){
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection location = db.getCollection("location");
        MongoCollection locWithLL = db.getCollection("loc_ll_google");
        MongoCursor cursor = location.find().iterator();
        GeoApiContext context = new GeoApiContext().setApiKey(API_KEY_GOOGLE);
        Document d;
        while(cursor.hasNext()){
            d = new Document();
            try {
                Document obj = (Document)cursor.next();
                GeocodingResult[] results = GeocodingApi.geocode(context,
                        obj.get("city") + " " + obj.get("name")).await();
                d.append("_id",obj.get("_id")).
                        append("city",obj.get("city")).
                        append("name",obj.get("name")).
                        append("code",obj.get("code"));
                d.append("lat", results[0].geometry.location.lat);
                d.append("lon", results[0].geometry.location.lng);
                locWithLL.insertOne(d);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * This method tries to geocode using Baidu Geocode API,
     * This method rely on the collection location,
     * which has all the stations' name and city
     */
    private static void geoEncodingBaidu(){
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection location = db.getCollection("location");
        MongoCollection locWithLL = db.getCollection("loc_ll_g_b");
        MongoCursor cursor = location.find().iterator();
        Document d;
        JSONObject resultLL;
        CloseableHttpClient http = HttpClients.createDefault();
        while(cursor.hasNext()){
            try {
                Document obj = (Document)cursor.next();
                URI url = new URIBuilder().setScheme("http")
                        .setHost("api.map.baidu.com")
                        .setPath("/geocoder/v2/")
                        .setParameter("address",obj.get("city")+" "+obj.get("name"))
                        .setParameter("output", "json")
                        .setParameter("ak", API_KEY_BAIDU).build();
                HttpGet httpget = new HttpGet(url);
                CloseableHttpResponse response = http.execute(httpget);
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String s = EntityUtils.toString(entity);
                    JSONObject result = new JSONObject(s);
                    JSONObject resultAll = result.getJSONObject("result");
                    resultLL = resultAll.getJSONObject("location");
                    d = new Document();
                    d.append("_id",obj.get("_id")).
                            append("city", obj.get("city")).
                            append("name", obj.get("name")).
                            append("code", obj.get("code"));
                    d.append("lat", resultLL.getDouble("lat"));
                    d.append("lon", resultLL.getDouble("lng"));
                    locWithLL.insertOne(d);
                    EntityUtils.consume(entity);
                    response.close();
                }
            }catch(URISyntaxException e2){
                e2.printStackTrace();
            }catch(IOException e){
                e.printStackTrace();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private static void preProcess(){
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection original = db.getCollection("pm25inoriginal");
        MongoCollection preprocess = db.getCollection("pm_preProcess");
        MongoCursor cur = original.find().iterator();
        Document insert;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z' Z");
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT+08:00"));
        Date d;
//        int count = 0;
        while(cur.hasNext()){
            Document obj = (Document)cur.next();
            insert = new Document();
            insert.append("_id", obj.get("_id"))
                    .append("aqi", obj.get("aqi"))
                    .append("co", obj.get("co"))
                    .append("no2", obj.get("no2"))
                    .append("o3", obj.get("o3"))
                    .append("pm10",obj.get("pm10"))
                    .append("pm25",obj.get("pm2_5"))
                    .append("so2",obj.get("so2"))
                    .append("code", obj.get("station_code"));
            try {
                d = df.parse(obj.get("time_point").toString()+" +0800");
                insert.append("time", d);
                //insert day of week by gmt+8
                c.setTime(d);
//                        * @see #SUNDAY 1
//                          * @see #MONDAY
//                        * @see #TUESDAY
//                        * @see #WEDNESDAY
//                        * @see #THURSDAY
//                        * @see #FRIDAY
//                        * @see #SATURDAY
                insert.append("dayofweekbj", c.get(Calendar.DAY_OF_WEEK));
                insert.append("hourofdayjb", c.get(Calendar.HOUR_OF_DAY));
                insert.append("dayofmonthbj", c.get(Calendar.DAY_OF_MONTH));
                insert.append("dayofyearbj", c.get(Calendar.DAY_OF_YEAR));
                insert.append("weekofyearbj", c.get(Calendar.WEEK_OF_YEAR));
                insert.append("weekofmonthbj", c.get(Calendar.WEEK_OF_MONTH));

            } catch (ParseException e) {
                e.printStackTrace();
                continue;
            }
            preprocess.insertOne(insert);
        }

    }

    /**
     * removeDuplicate
     * by add a unique index
     */
    private static void removeDuplicate(){
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection preprocess = db.getCollection("pm_preProcess");
        MongoCollection process = db.getCollection("pmProcess");
        IndexOptions option = new IndexOptions();
        option.unique(true);
        process.createIndex(new Document().append("time",1).append("code",1), option);
        MongoCursor cur = preprocess.find().iterator();

        while(cur.hasNext()){
            Document obj = (Document)cur.next();
            try {
                process.insertOne(obj);
            }catch(MongoWriteException e){
                //duplicate error
                System.out.println(obj.toString());
            }
        }
    }

    /**
     * write station city, name, code to db
     */
    private static void writeStationToDB(){
        try {
            FileReader fr = new FileReader(PATH+"station_names.json");
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();

            MongoClient client = new MongoClient("127.0.0.1");
            MongoDatabase db = client.getDatabase("pm");
            MongoCollection location = db.getCollection("location");
            Document doc = null;
            ArrayList<String> station = new ArrayList<String>();

            JSONArray array = new JSONArray(line);
            for(int i = 0; i < array.length(); i ++) {
                JSONObject oneObj = (JSONObject)array.get(i);
                doc = new Document("city", oneObj.get("city").toString());
                JSONArray oneArray = oneObj.getJSONArray("stations");
                for(int j = 0; j < oneArray.length(); j ++){
                    if(!station.contains(((JSONObject)(oneArray.get(j))).get("station_code").toString())) {
                        doc.append("code", ((JSONObject) (oneArray.get(j))).get("station_code").toString());
                        doc.append("name", ((JSONObject) (oneArray.get(j))).get("station_name").toString());
                        location.insertOne(doc);
                        doc = new Document("city", oneObj.get("city").toString());
                        station.add(((JSONObject)(oneArray.get(j))).get("station_code").toString());
                    }
                }
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    ////////following are used to generate all station code////////////
    /**
     * 1022A 天津 梅江小区
     * 1289A 厦门 湖里
     * 1892A 攀枝花 金江
     */
    public static void compareStationCodes(){
        try {
            List<String> s1 = FileUtils.readLines(new File(PATH+"allStationCodeCurrentDB"));
            List<String> s2 = FileUtils.readLines(new File(PATH+"allStationCodeFromPM25"));
            boolean hasSame = false;
            for(String ss1 : s1){
                for(String ss2 : s2){
                    if(ss2.equals(ss1))
                        hasSame = true;
                }
                if(hasSame == false) System.out.println(ss1);
                hasSame = false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    /**
     *
     */
    private static HashMap<String, String> parseLocationNames(){
        try {
            FileReader fr = new FileReader(PATH+"station_names.json");
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            HashMap<String, String> codeAndName = new HashMap<String, String>();

            int countStation = 0;
            ArrayList<String> station = new ArrayList<String>();

            JSONArray array = new JSONArray(line);
            for(int i = 0; i < array.length(); i ++) {
                JSONObject oneObj = (JSONObject)array.get(i);
                JSONArray oneArray = oneObj.getJSONArray("stations");
                for(int j = 0; j < oneArray.length(); j ++){
                    if(!station.contains(((JSONObject)(oneArray.get(j))).get("station_code").toString())){
                        try {
                            FileUtils.writeStringToFile(new File(PATH+"allStationCodeFromPM25"),
                                    ((JSONObject)(oneArray.get(j))).get("station_code").toString()+"\n",
                                    true);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        station.add(((JSONObject)(oneArray.get(j))).get("station_code").toString());
                        codeAndName.put(((JSONObject)(oneArray.get(j))).get("station_code").toString(),
                                ((JSONObject)(oneArray.get(j))).get("station_name").toString());
                    }
                }
            }
            System.out.println("city count:"+array.length());
            System.out.println("station count:" + station.size());
            return codeAndName;
        }catch(IOException e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * save all stations to a file allStationCodeCurrentDB
     * @return
     */
    private static ArrayList<String> getAllStationFromDB(){
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection pm = db.getCollection("pm25inoriginal");
        MongoCursor cursor = pm.distinct("station_code", String.class).iterator();
        ArrayList<String> result = new ArrayList<String>();
        String temp = null;
        while(cursor.hasNext()){
            temp = cursor.next().toString();
            result.add(temp);
            try {
                FileUtils.writeStringToFile(new File(PATH+"allStationCodeCurrentDB"), temp+"\n", true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }


    private static void preprocess(){
        HashMap<String, String> codeAName = parseLocationNames();

        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("pm");
        MongoCollection pm = db.getCollection("pm25inoriginal");
        MongoCollection pm_preProcess = db.getCollection("pm_preProcess");
        MongoCursor cursor = pm.find().iterator();
        String stationCode = "";
        String stationName = "";
        System.out.println(cursor.hasNext());
        while(cursor.hasNext()){
            Document info = (Document) cursor.next();
            stationCode = info.getString("station_code");
            if(!codeAName.containsKey(stationCode)){
                System.out.println("notcontainkey:"+info.toString());
                continue;
            }
            stationName = codeAName.get(stationCode);
            System.out.println();
            if(info.getString("position_name").equals(stationName)){
                pm_preProcess.insertOne(info);
            }else{
                System.out.println("position error:"+stationName+":"+info.toString());
                continue;
            }
        }
        client.close();
    }
}
