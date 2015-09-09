//按照年份分析各数据特征
package org.duyi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class yearPmData {

	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		Mongo mg71 = new Mongo("192.168.16.71");
		for (String name : mg71.getDatabaseNames()) {
			System.out.println("dbName:" + name);
		}
		DB db71 = mg71.getDB("pmdata_2014");
		for (String name : db71.getCollectionNames()) {
			System.out.println("collectionName: " + name);
		}
		DBCollection users71 = db71.getCollection("pm_2014");
		DBCollection users_year = db71.getCollection("pmdata_year");
		BasicDBObject cond = new BasicDBObject();
		try {
			String encoding = "utf8";
			File file = new File("D:\\data.txt");
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(
						new FileInputStream(file), encoding);// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					String[] line = lineTxt.split(",");
					cond.put("area", line[0]);
					cond.put("position_name", line[1]);

					DBCursor infoList = users71.find(cond);
					ArrayList<Integer> aqiList = new ArrayList<Integer>();
					ArrayList<Integer> pm25List = new ArrayList<Integer>();
					ArrayList<Integer> no2List = new ArrayList<Integer>();
					ArrayList<Integer> o3List = new ArrayList<Integer>();
					ArrayList<Integer> pm10List = new ArrayList<Integer>();
					ArrayList<Integer> so2List = new ArrayList<Integer>();
					ArrayList<Double> coList = new ArrayList<Double>();
					String time = null;
					String subTime = null;
					String station_code = null;
					String longitude = null;
					String latitude = null;
					while (infoList.hasNext()) {
						BasicDBObject info = (BasicDBObject) infoList.next();
						// System.out.println("原始信息：" +info);
						String id = info.get("_id").toString();
						if (id == null || id == "") {
							continue;
						}
						Integer aqi = info.getInt("aqi");
						Integer pm25 = info.getInt("pm2_5");
						Integer no2 = info.getInt("no2");
						Integer o3 = info.getInt("o3");
						Integer pm10 = info.getInt("pm10");
						Integer so2 = info.getInt("so2");
						double co = info.getInt("co");
						station_code = (String) info.get("station_code");
						longitude = (String) info.get("longitude");
						latitude = (String) info.get("latitude");

						if (aqi > 0) {
							aqiList.add(aqi);
						}
						if (pm25 > 0) {
							pm25List.add(pm25);
						}
						if (no2 > 0) {
							no2List.add(no2);
						}
						if (o3 > 0) {
							o3List.add(o3);
						}
						if (pm10 > 0) {
							pm10List.add(pm10);
						}
						if (so2 > 0) {
							so2List.add(so2);
						}
						if (co > 0) {
							coList.add(co);
						}
					}
					// System.out.println(aqiList);
					Integer aqiMax = null;
					Integer aqiMin = null;
					Double aqiAve = null;
					if (!aqiList.isEmpty()) {
						aqiMax = Collections.max(aqiList);
						aqiMin = Collections.min(aqiList);
						aqiAve = getAverage(aqiList);
					} else {
						aqiMax = 0;
						aqiMin = 0;
						aqiAve = 0.0;
					}

					Integer pm25Max = null;
					Integer pm25Min = null;
					Double pm25Ave = null;
					if (!pm25List.isEmpty()) {
						pm25Max = Collections.max(pm25List);
						pm25Min = Collections.min(pm25List);
						pm25Ave = getAverage(pm25List);
					} else {
						pm25Max = 0;
						pm25Min = 0;
						pm25Ave = 0.0;
					}

					Integer no2Max = null;
					Integer no2Min = null;
					Double no2Ave = null;
					if (!no2List.isEmpty()) {
						no2Max = Collections.max(no2List);
						no2Min = Collections.min(no2List);
						no2Ave = getAverage(no2List);
					} else {
						no2Max = 0;
						no2Min = 0;
						no2Ave = 0.0;
					}

					Integer o3Max = null;
					Integer o3Min = null;
					Double o3Ave = null;
					if (!o3List.isEmpty()) {
						o3Max = Collections.max(o3List);
						o3Min = Collections.min(o3List);
						o3Ave = getAverage(o3List);
					} else {
						o3Max = 0;
						o3Min = 0;
						o3Ave = 0.0;
					}

					Integer pm10Max = null;
					Integer pm10Min = null;
					Double pm10Ave = null;
					if (!pm10List.isEmpty()) {
						pm10Max = Collections.max(pm10List);
						pm10Min = Collections.min(pm10List);
						pm10Ave = getAverage(pm10List);
					} else {
						pm10Max = 0;
						pm10Min = 0;
						pm10Ave = 0.0;
					}

					Integer so2Max = null;
					Integer so2Min = null;
					Double so2Ave = null;
					if (!so2List.isEmpty()) {
						so2Max = Collections.max(so2List);
						so2Min = Collections.min(so2List);
						so2Ave = getAverage(so2List);
					} else {
						so2Max = 0;
						so2Min = 0;
						so2Ave = 0.0;
					}

					Double coMax = null;
					Double coMin = null;
					Double coAve = null;
					if (!coList.isEmpty()) {
						coMax = Collections.max(coList);
						coMin = Collections.min(coList);
						coAve = getAverage1(coList);
					} else {
						coMax = 0.0;
						coMin = 0.0;
						coAve = 0.0;
					}

					DBObject doc = null;
					doc = new BasicDBObject();
					doc.put("time_point", "2014");
					doc.put("area", line[0]);
					doc.put("position_name", line[1]);
					doc.put("station_code", station_code);
					doc.put("longitude", longitude);
					doc.put("latitude", latitude);
					doc.put("aqi_max", aqiMax);
					doc.put("aqi_min", aqiMin);
					doc.put("aqi_ave", aqiAve);
					doc.put("pm25_max", pm25Max);
					doc.put("pm25_min", pm25Min);
					doc.put("pm25_ave", pm25Ave);
					doc.put("no2_max", no2Max);
					doc.put("no2_min", no2Min);
					doc.put("no2_ave", no2Ave);
					doc.put("o3_max", o3Max);
					doc.put("o3_min", o3Min);
					doc.put("o3_ave", o3Ave);
					doc.put("pm10_max", pm10Max);
					doc.put("pm10_min", pm10Min);
					doc.put("pm10_ave", pm10Ave);
					doc.put("so2_max", so2Max);
					doc.put("so2_min", so2Min);
					doc.put("so2_ave", so2Ave);
					doc.put("co_max", coMax);
					doc.put("co_min", coMin);
					doc.put("co_ave", coAve);
					System.out.println("新信息：" + doc);
					users_year.save(doc);
				}
				read.close();
			} else {
				System.out.println("找不到指定的文件");
			}
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}
	}

	// 各地站点list去重,去除list里空字符
	public static ArrayList<String> removeDuplicate(List list) {
		HashSet h = new HashSet(list);
		list.clear();
		list.addAll(h);
		for (int i = 0; i < list.size(); i++) {
			if ("" == list.get(i) || null == list.get(i)) {
				list.remove(i);
			}
		}
		System.out.println(list);
		return (ArrayList<String>) list;
	}

	// 求平均值
	public static Double getAverage(List<Integer> list) {
		Double sum = (double) 0;
		for (int i = 1; i < list.size() - 1; i++) {
			sum += list.get(i).doubleValue();
		}
		Double d;
		d = Double.valueOf(sum / (list.size()));
		DecimalFormat df = new DecimalFormat("######0.000");
		return Double.parseDouble(df.format(d));
	}

	public static Double getAverage1(List<Double> list) {
		Double sum = (double) 0;
		for (int i = 1; i < list.size() - 1; i++) {
			sum += list.get(i).doubleValue();
		}
		Double d;
		d = Double.valueOf(sum / (list.size()));
		DecimalFormat df = new DecimalFormat("######0.000");
		return Double.parseDouble(df.format(d));
	}
}
