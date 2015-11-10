package org.duyi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by yidu on 11/10/15.
 * read file and store weather data to mongodb
 */
public class WeatherDataProcessor {
    public static void main(String[] args){
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
                ArrayList<String> header = new ArrayList<String>();
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
