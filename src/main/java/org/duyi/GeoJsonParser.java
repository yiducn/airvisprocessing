package org.duyi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.vividsolutions.jts.geom.*;
import org.bson.Document;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.GeodeticCalculator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.Feature;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

/**
 * Created by yidu on 3/30/16.
 */
public class GeoJsonParser {

    public static void main(String[] args){
        try {
            //input : codeList  distance
            //output : cluster
            //计算codeList的中心,可以在前端计算好
            //循环,找到所有满足要求的点
            //循环feature,对每一个feature,判断点是否在范围内,如果在,聚类

            MongoClient client = new MongoClient("127.0.0.1");
            MongoDatabase db = client.getDatabase("airdb");
            MongoCollection coll = db.getCollection("pm_stations");
            MongoCursor cur = coll.find().iterator();
            JSONObject oneStation;
            ArrayList<JSONObject> filtered = new ArrayList<JSONObject>();
            double maxDistance = 603153.1314;//最大距离约束
            //中心 经度\纬度116°23′17〃，北纬：39°54′27;116.5, 40
            String[] codes = {"1001A", "1002A"};
            Document d;
            while(cur.hasNext()){
                d = (Document)cur.next();
                double lon = d.getDouble("lon");
                double lat = d.getDouble("lat");
                GeodeticCalculator calc = new GeodeticCalculator();
                // mind, this is lon/lat
                calc.setStartingGeographicPoint(lon, lat);
                calc.setDestinationGeographicPoint(116.4, 40);
                double distance = calc.getOrthodromicDistance();

                //距离在最大距离之外的去除
                if(distance > maxDistance)
                    continue;

                //去除自己
                boolean self = false;
                for(int i = 0; i < codes.length; i ++) {
                    if (d.get("code").equals(codes[i]))
                        self = true;
                }
                if(self)
                    continue;


                oneStation = new JSONObject();
                GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
                try {
                    oneStation.put("city", d.getString("city"));
                    oneStation.put("station", d.getString("name"));
                    oneStation.put("longitude", d.getDouble("lon"));
                    oneStation.put("latitude", d.getDouble("lat"));
                    oneStation.put("code", d.getString("code"));
                    Coordinate coord = new Coordinate(d.getDouble("lon"), d.getDouble("lat"));
                    Point point = geometryFactory.createPoint(coord);
                    oneStation.put("point", point);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                filtered.add(oneStation);
            }

            FeatureJSON fj = new FeatureJSON();
            FeatureCollection fc = fj.readFeatureCollection(new FileInputStream(new File("/Users/yidu/dev/airvisprocessing/src/main/java/org/duyi/china_cities.json")));
            FeatureIterator iterator = fc.features();
            ArrayList<Geometry> cityArea = new ArrayList<Geometry>();
            ArrayList cluster = new ArrayList();

            try {
                while( iterator.hasNext() ){
                    Feature feature = iterator.next();
                    Geometry value = (Geometry)feature.getDefaultGeometryProperty().getValue();
                    cityArea.add(value);
                }

                for(int i = 0; i < cityArea.size(); i ++){
                    ArrayList oneCluster = new ArrayList();
                    for(int j = 0; j < filtered.size(); j ++){
                        if(cityArea.get(i).contains((Geometry)filtered.get(j).get("point"))){
                            oneCluster.add(filtered);
                        }
                    }
                    if(!oneCluster.isEmpty())
                        cluster.add(oneCluster);
                }
            }
            finally {
                iterator.close();
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
