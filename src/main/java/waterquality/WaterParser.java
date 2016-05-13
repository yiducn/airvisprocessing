package waterquality;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.FileUtils;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yidu on 5/12/16.
 */
public class WaterParser {
    static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

    static final String PATH = "/Users/yidu/Downloads/water";

    public static void main(String[] args){
        /**
         * 读取水质数据,保存到数据库中
         */
        String[] extension = {"csv"};
        try {
            Iterator<File> files =  FileUtils.listFiles(new File(PATH), extension, true).iterator();
            MongoClient client = new MongoClient("127.0.0.1");
            MongoDatabase db = client.getDatabase("airdb");
            MongoCollection water = db.getCollection("water");

            while(files.hasNext()){
                File f = files.next();
                List<String> list = FileUtils.readLines(f);
                for(int i = 1; i < list.size(); i ++){
                    Document d = new Document();
                    StringTokenizer st = new StringTokenizer(list.get(i), ",");
                    d.append("time", df.parse(st.nextToken()+"+0800"));
                    d.append("code", Integer.parseInt(st.nextToken()));
                    d.append("name", st.nextToken());
                    d.append("river", st.nextToken());
                    d.append("type", st.nextToken());
                    d.append("ph", Double.parseDouble(st.nextToken()));
                    d.append("phlevel", st.nextToken());
                    d.append("o2", Double.parseDouble(st.nextToken()));
                    d.append("o2level", st.nextToken());
                    d.append("no", Double.parseDouble(st.nextToken()));
                    d.append("nolevel", st.nextToken());
                    st.nextToken();
                    st.nextToken();
                    d.append("c", Double.parseDouble(st.nextToken()));
                    d.append("clevel", st.nextToken());
//                    System.out.println(d.toString());
                    water.insertOne(d);
                }
            }

        }catch(IOException e){
            e.printStackTrace();
        }catch(ParseException e2){
            e2.printStackTrace();
        }
    }
}
