package org.duyi;

import java.text.SimpleDateFormat;

/**
 * Created by yidu on 4/18/16.
 */
public class Test {
    public static void main(String[] args){
        try {
            SimpleDateFormat df = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss");
            df.parse("Tue Mar 10 2015 13:39:20 GMT-0400 (EDT)");
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
