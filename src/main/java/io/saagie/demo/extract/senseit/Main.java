package io.saagie.demo.extract.senseit;


import au.com.bytecode.opencsv.CSVWriter;
import com.github.kevinsawicki.http.HttpRequest;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.saagie.demo.extract.senseit.dto.History;
import io.saagie.demo.extract.senseit.dto.SenseIT;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Date;
import java.util.Iterator;


/**
 * Created by youen on 02/12/2015.
 */
public class Main {


   private static final Logger logger = Logger.getLogger(Main.class);
   public static final char SEP = ';';

   public static void main(String[] args) throws Exception {
      if (args.length<4) {
         System.out.println("4 args are required :\n\t- deviceid\n\t- sensorid\n\t- accesstoken\n\t- hdfsmasteruri (8020 port)");
         System.exit(128);
      }
      logger.info("Initializing...");
      String deviceid=args[0];
      String sensorid=args[1];
      String accesskey=args[2];
      String hdfsuri = args[3];


      SenseIT senseIT=null;
      logger.info("Calling SenseIT API...");
      HttpRequest res=HttpRequest.get("https://api.sensit.io/v1/devices/"+deviceid+"/sensors/"+sensorid)
              .authorization("Bearer "+accesskey)
              .accept("application/json");
      String body=res.body();

      senseIT=new Gson().fromJson(body, new TypeToken<SenseIT>() { }.getType());
      String jsondata=new Gson().toJson(senseIT.data);


      String csv = buildCSV(deviceid, sensorid, senseIT);

      ByteArrayOutputStream csv_os = buildCSV2(deviceid, sensorid, senseIT);

      // Create Directories
      logger.debug("Create directory");
      FileSystem fs = HdfsUtils.getFileSystemFromUri(hdfsuri);
      Path directoryraw = HdfsUtils.createDirectory(fs, "/iot/sigfox/senseit/raw");
      Path directorycsv = HdfsUtils.createDirectory(fs, "/iot/sigfox/senseit/csv");
      Path directorycsv2 = HdfsUtils.createDirectory(fs, "/iot/sigfox/senseit/csv2");


      String filename=deviceid+"-"+sensorid+"-"+senseIT.data.sensor_type+"-"+(new Date()).getTime();

      logger.debug("Create file");
      // Creating a file in HDFS
      HdfsUtils.createFile(fs, directoryraw, filename+".json", jsondata);

      logger.info("Import done of raw file : "+filename+".json");

      HdfsUtils.createFile(fs, directorycsv, filename+".csv", csv.getBytes());
      HdfsUtils.createFile(fs, directorycsv, filename+".csv", csv_os.toByteArray());

      logger.info("Import done of csv file : "+filename+".csv");


      logger.info("Done.");


   }

   private static String buildCSV(String deviceid, String sensorid, SenseIT senseIT) {
      logger.info("Building CSV...");
      String csv="device;sensor;sensor_type;date;data;value\n";

      Iterator<History> it_his=senseIT.data.history.iterator();
      while(it_his.hasNext()) {
         History his=it_his.next();
         csv+=deviceid+ SEP +sensorid+ SEP +senseIT.data.sensor_type+ SEP +his.date+ SEP +his.data+ SEP +his.getDataInDouble()+"\n";
      }
      return csv;
   }

   private static ByteArrayOutputStream buildCSV2(String deviceid, String sensorid, SenseIT senseIT) throws IOException {
      logger.info("Building CSV...");

      ByteArrayOutputStream os = new ByteArrayOutputStream();
      CSVWriter writer = new CSVWriter(new OutputStreamWriter(os,"UTF-8"));
      writer.writeNext(new String[]{"device","sensor","sensor_type","date","data","value"});

      Iterator<History> it_his=senseIT.data.history.iterator();
      while(it_his.hasNext()) {
         History his=it_his.next();
         writer.writeNext(new String[]{deviceid,sensorid,senseIT.data.sensor_type,his.date,his.data,his.getDataInDouble().toString()});
      }
      writer.flush();
      return os;
   }
}
