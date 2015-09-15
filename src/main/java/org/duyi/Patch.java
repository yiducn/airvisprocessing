//补全192.168.16.71数据库里站点缺失经纬度
package org.duyi;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Patch {

	public static void main(String[] args) throws UnknownHostException,
			MongoException {
		// TODO Auto-generated method stub
		MongoClient client = new MongoClient("127.0.0.1");
		MongoDatabase db = client.getDatabase("pm");
		MongoCollection pm = db.getCollection("pm25original");
		MongoCollection pmWithLocation = db.getCollection("pm_with_loc");
		MongoCursor objpm = pm.find().iterator();

		while (objpm.hasNext()) {
			BasicDBObject info = (BasicDBObject) objpm.next();
			String position = info.get("position_name").toString();
			String area = info.get("area").toString();
			try {
				String encoding = "utf8";
				File file = new File("D:\\city.txt");
				if (file.isFile() && file.exists()) { // 判断文件是否存在
					InputStreamReader read = new InputStreamReader(
							new FileInputStream(file), encoding);// 考虑到编码格式
					BufferedReader bufferedReader = new BufferedReader(read);
					String lineTxt = null;
					while ((lineTxt = bufferedReader.readLine()) != null) {
						String[] line = lineTxt.split(",");
						String cityTxt = line[0];
						String positionTxt = line[1];
						// 通过城市和站点两个条件匹配
						if (positionTxt.equals(position)
								&& cityTxt.equals(area)) {
							info.put("longitude", line[2]);
							info.put("latitude", line[3]);
						}
					}
					read.close();
				} else {
					System.out.println("找不到指定的文件");
				}
			} catch (Exception e) {
				System.out.println("读取文件内容出错");
				e.printStackTrace();
			}
			//pmWithLocation.save(info);
			//System.out.println("修改后信息：" + info);
		}

	}
}
