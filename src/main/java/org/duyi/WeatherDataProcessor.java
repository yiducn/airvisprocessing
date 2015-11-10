package org.duyi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by yidu on 11/10/15.
 *
 */
public class WeatherDataProcessor {
    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd Z");
    static ArrayList<String> header;
    static MongoClient client;
    static MongoDatabase db;

    public static void main(String[] args){
        client = new MongoClient("127.0.0.1");
        db = client.getDatabase("pm");
        loadAndStore();
        preProcess();
    }

    /**
     * parse date and other digital
     */
    private static void preProcess() {
        try {
            MongoCollection weatherRaw = db.getCollection("weatherRaw");
            MongoCollection weather = db.getCollection("weather");
            MongoCursor cursor = weatherRaw.find().iterator();
            while (cursor.hasNext()) {
                Document insert = new Document();
                Document info = (Document) cursor.next();
                String year = info.getString("Year");
                String month = info.getString("Mon");
                String day = info.getString("Day");
                insert.append("station", Integer.parseInt(info.getString("Station_Id_C")));
                insert.append("time", df.parse(year + "-" + month + "-" + day + " +0000"));
                for(String h : header){
                    if(h.equals("Year") || h.equals("Mon") || h.equals("Day"))
                        continue;
                    insert.append(h, Double.parseDouble(info.getString(h)));
                }
                weather.insertOne(insert);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * read file and store weather data to mongodb
     */
    private static void loadAndStore() {
        try {
            MongoClient client = new MongoClient("127.0.0.1");
            MongoDatabase db = client.getDatabase("pm");
            MongoCollection weather = db.getCollection("weatherRaw");
            Document doc;

            File dir = new File("/Users/yidu/Downloads/airData/");
            File[] files = dir.listFiles();
            for (File f : files) {
                if(!f.getName().startsWith("S2015"))
                    continue;
                System.out.println(f.getName());
                List<String> lines = IOUtils.readLines(new FileInputStream(f));
                StringTokenizer st = new StringTokenizer(lines.get(0));
                //get header
                header = new ArrayList<String>();
                while(st.hasMoreTokens()){
                    header.add(st.nextToken());
                }

                for(int i = 1; i < lines.size(); i ++){
                    JSONObject obj = new JSONObject();
                    StringTokenizer st2 = new StringTokenizer(lines.get(i));
                    doc = new Document();
                    for(int j = 0; j < header.size(); j ++){
                        doc.append(header.get(j), st2.nextToken());
                    }
                    weather.insertOne(doc);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}
