package org.duyi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.valid.IsValidOp;
import org.bson.Document;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.GeodeticCalculator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opengis.feature.Feature;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

/**
 * Created by yidu on 4/22/16.
 */
public class CityClustering {
    private static final String CITY_PATH = "/Users/yidu/dev/airvisprocessing/src/main/java/org/duyi/china_cities.json";
    public static void main(String[] args) {
        FeatureJSON fj = new FeatureJSON();
        ArrayList<Geometry> cityArea = new ArrayList<Geometry>();
        FeatureCollection fc = null;
        try {
            fc = fj.readFeatureCollection(new FileInputStream(new File(CITY_PATH)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        FeatureIterator iterator = fc.features();
        while (iterator.hasNext()) {
            Feature feature = iterator.next();
            Geometry value = (Geometry) feature.getDefaultGeometryProperty().getValue();
            cityArea.add(value);
        }


        MongoClient client = new MongoClient("127.0.0.1");
        MongoDatabase db = client.getDatabase("airdb");
        MongoCollection coll = db.getCollection("pm_stations");
        MongoCursor cur = coll.find().iterator();
        JSONObject oneStation;
        ArrayList<JSONObject> filtered = new ArrayList<JSONObject>();

        //中心 经度\纬度116°23′17〃，北纬：39°54′27;116.5, 40
        try {
            Document d;
            //该操作将所有点先找出来
            while (cur.hasNext()) {
                d = (Document) cur.next();
                double lon = d.getDouble("lon");
                double lat = d.getDouble("lat");
                GeodeticCalculator calc = new GeodeticCalculator();
                // mind, this is lon/lat
                calc.setStartingGeographicPoint(lon, lat);

                oneStation = new JSONObject();
                GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
                oneStation.put("city", d.getString("city"));
                oneStation.put("station", d.getString("name"));
                oneStation.put("longitude", d.getDouble("lon"));
                oneStation.put("latitude", d.getDouble("lat"));
                oneStation.put("code", d.getString("code"));
                Coordinate coord = new Coordinate(d.getDouble("lon"), d.getDouble("lat"), 0);
                Point point = geometryFactory.createPoint(coord);
                oneStation.put("point", point);
                filtered.add(oneStation);
            }

            JSONObject cluster;
            JSONArray result = new JSONArray();
            //北京天津上海重庆特殊处理,先各聚在一起,然后在分解
            JSONArray bjCluster = new JSONArray();
            JSONArray tjCluster = new JSONArray();
            JSONArray shCluster = new JSONArray();
            JSONArray cqCluster = new JSONArray();
            for (int j = 0; j < filtered.size(); j++) {
                if(filtered.get(j).getString("city").startsWith("北京")){
                    bjCluster.put(filtered.get(j));
                }else if(filtered.get(j).getString("city").startsWith("天津")){
                    tjCluster.put(filtered.get(j));
                }else if(filtered.get(j).getString("city").startsWith("上海")) {
                    shCluster.put(filtered.get(j));
                }else if(filtered.get(j).getString("city").startsWith("重庆")){
                    cqCluster.put(filtered.get(j));
                }
            }
            cluster = new JSONObject();
            cluster.put("cluster", bjCluster);
            result.put(cluster);
            cluster = new JSONObject();
            cluster.put("cluster", tjCluster);
            result.put(cluster);
            cluster = new JSONObject();
            cluster.put("cluster", shCluster);
            result.put(cluster);
            cluster = new JSONObject();
            cluster.put("cluster", cqCluster);
            result.put(cluster);

            for (int i = 0; i < cityArea.size(); i++) {
                //判断是否正确图形
                IsValidOp isValidOp = new IsValidOp(cityArea.get(i));
                if (!isValidOp.isValid())
                    continue;
                cluster = new JSONObject();
                JSONArray oneCluster = new JSONArray();
//                if()
                for (int j = 0; j < filtered.size(); j++) {
                    if(filtered.get(j).getString("city").startsWith("北京") ||
                            filtered.get(j).getString("city").startsWith("天津") ||
                            filtered.get(j).getString("city").startsWith("上海") ||
                            filtered.get(j).getString("city").startsWith("重庆"))
                        continue;
                    if (cityArea.get(i).contains((Geometry) filtered.get(j).get("point"))) {
                        oneCluster.put(filtered.get(j));
                    }
                }
                if (oneCluster.length() != 0) {
                    cluster.put("cluster", oneCluster);
                    result.put(cluster);
                }
            }

            JSONArray finalCluster   = new JSONArray();

            for(int i = 0; i < result.length(); i ++){
                JSONArray oneCluster = result.getJSONObject(i).getJSONArray("cluster");
                //计算各个station之间的相关性

            }
            System.out.println(result.toString());

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
