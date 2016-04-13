package org.duyi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yidu on 3/28/16.
 * 从数据库中任意提取一个月的数据,进行decompose实验
 */
public class MonthlyExporter {
    public static void main(String[] args){
        try {
            MongoClient client = new MongoClient("127.0.0.1");
            MongoDatabase db = client.getDatabase("airdb");
            MongoCollection data = db.getCollection("pm_data");
            Document find = new Document();
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH Z");
            Document timeFilter = new Document();
            timeFilter.put("$lt", (df.parse("2015-03-31 23 +0800")));
            timeFilter.put("$gt", df.parse("2015-03-01 00 +0800"));
            Document pmFilter = new Document();
            pmFilter.put("$ne", 0);

            find.put("time", timeFilter);
            find.put("pm25", pmFilter);
            find.put("code", "1007A");//1007A海淀挽留
            System.out.println(find.toString());

            Document sort = new Document("time", 1);
            MongoCursor c = data.find(find).sort(sort).iterator();
            Document d;
            Date formerDate = null;
            int formerPm = 0;
            while (c.hasNext()) {

                d = (Document)c.next();

                if((formerDate != null && d.getDate("time").getTime()-formerDate.getTime() > 60*60*1000) ){
//                    System.out.println(d.getDate("time").getTime()-formerDate.getTime());
                    for(int i = 0; i < (d.getDate("time").getTime()-formerDate.getTime())/(60*60*1000)-1; i ++){
                        System.out.println("new time"+"\t" + (formerPm + (i+1)*(d.getInteger("pm25") - formerPm)/((d.getDate("time").getTime()-formerDate.getTime())/(60*60*1000))-1));
                    }
                }
                System.out.println(d.get("time") + "\t" + d.getInteger("pm25"));
                formerDate = d.getDate("time");
                formerPm = d.getInteger("pm25");

            }
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
