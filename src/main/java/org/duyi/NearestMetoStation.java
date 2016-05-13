package org.duyi;

import com.mongodb.MongoClient;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.bson.Document;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.GeodeticCalculator;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Created by yidu on 4/23/16.
 * 根据clusterid,找到最近气象站点,并将站点写入数据库
 */
public class NearestMetoStation {
    public static void main(String[] args) {
        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("airdb");
        MongoCollection coll = db.getCollection("cluster");
//        MongoCursor cur = coll.find().iterator();
        JSONObject oneStation;
        ArrayList<JSONObject> filtered = new ArrayList<JSONObject>();
        MongoCollection collMeto = db.getCollection("meteo_stations");

        MongoCollection clusterMeto = db.getCollection("clusterMeteo");

        //找到所有的cluster
        MongoCursor cur = coll.distinct("clusterid", String.class).iterator();
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        while (cur.hasNext()) {
            String id = (String) cur.next();
            MongoCursor stations = coll.find(new Document("clusterid", id)).iterator();
            double sumX = 0, sumY = 0;
            int count = 0;
            while (stations.hasNext()) {
                Document d = (Document) stations.next();
                Coordinate coord = new Coordinate(d.getDouble("lon"), d.getDouble("lat"), 0);
                Point point = geometryFactory.createPoint(coord);
                sumX += point.getX();
                sumY += point.getY();
                count++;
            }
            double x = sumX / count;
            double y = sumY / count;
            Coordinate coord = new Coordinate(y, x);
            Point center = geometryFactory.createPoint(coord);
            System.out.println("center:"+x+":"+y);

            MongoCursor curMeteo = collMeto.find().iterator();
            int meteoId = 0;
            double minDis = Double.MAX_VALUE;
            while (curMeteo.hasNext()) {
                Document d = (Document) curMeteo.next();
                coord = new Coordinate(d.getDouble("lon"), d.getDouble("lat"), 0);
                Point point = geometryFactory.createPoint(coord);
                double dist = (x - point.getX())*(x - point.getX()) + (y - point.getY())*(y-point.getY());//point.distance(center);
                if (dist < minDis) {
                    minDis = dist;
                    meteoId = d.getInteger("usaf");
                    System.out.print("change:"+meteoId+":"+dist);
                }
            }
            System.out.println(meteoId);
            Document result = new Document("clusterid", id);
            result.put("usaf", meteoId);
            clusterMeto.insertOne(result);
        }
    }
}
