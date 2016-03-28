package org.duyi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yidu on 3/27/16.
 * 使用新的station数据来替换之前使用geocoding的数据
 */
public class StationParser {
    public static void main(String[] args){
        try {
            MongoClient client = new MongoClient("127.0.0.1");
            MongoCollection collection = client.getDatabase("airdb").getCollection("pm_stations");
            BufferedReader br = new BufferedReader(new FileReader("/Users/yidu/Downloads/a.csv"));
            br.readLine();
            int count = 0;
            String s = br.readLine();
            while(s != null){
                System.out.println(s +":"+(count++));
                StringTokenizer st = new StringTokenizer(s, ",");
                Document doc = new Document();
                doc.put("province", st.nextToken());
                doc.put("city", st.nextToken());
                doc.put("standardcode", st.nextToken());
                doc.put("name", st.nextToken());
                doc.put("code", st.nextToken());
                doc.put("lon", Double.parseDouble(st.nextToken()));
                doc.put("lat", Double.parseDouble(st.nextToken()));
                collection.insertOne(doc);
                s = br.readLine();
            }

        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
