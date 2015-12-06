package io.saagie.demo.extract.senseit.dto;

import java.util.Date;

/**
 * Created by youen on 06/12/2015.
 */
public class History {

    public String date;
    public String data;

    public Double getDataInDouble() {
        Double result=null;
        try {
            if (data.length() > 0) {
                String[] tokens = data.split(":");
                return Double.parseDouble(tokens[0]);
            }
        } catch (Exception e) { }
       return result;
    }
}
