package org.duyi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by yidu on 9/15/15.
 */
public class DBCityParser {

    public static void main(String[] args){
        try {
            BufferedReader br = new BufferedReader(
                    new FileReader("/Users/yidu/dev/airvisprocessing/data"));
            FileWriter fw = new FileWriter("/Users/yidu/dev/airvisprocessing/data/", false);

            String line = br.readLine();
            while(line != null){
                line = line.replaceAll(" ", "");
                line = line.replaceAll("\t", "");
                line = line.replaceAll("\"", "");
                line = line.replaceAll(",", "");
                if(!line.isEmpty())
                    fw.write(line+"\n");
                line = br.readLine();
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }
}
