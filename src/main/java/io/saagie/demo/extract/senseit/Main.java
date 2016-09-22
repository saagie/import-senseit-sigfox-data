package io.saagie.demo.extract.senseit;

import com.github.kevinsawicki.http.HttpRequest;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.saagie.demo.extract.senseit.dto.History;
import io.saagie.demo.extract.senseit.dto.SenseIT;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;


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

      int i=1;
      while(getDataAndStoretoHdfs(i,deviceid, sensorid, accesskey, hdfsuri)) { i++;};

      logger.info("Done.");


   }

   private static boolean getDataAndStoretoHdfs(int page,String deviceid, String sensorid, String accesskey, String hdfsuri) throws Exception {
      SenseIT senseIT=null;
      logger.info("Calling SenseIT API for page "+page);
      HttpRequest res=HttpRequest.get("https://api.sensit.io/v1/devices/"+deviceid+"/sensors/"+sensorid+"?page="+page)
              .authorization("Bearer "+accesskey)
              .accept("application/json");
       //Do NOT reproduce at home - bad ssl config on senseit api
       res.trustAllCerts();
       res.trustAllHosts();

      String body=res.body();

      logger.debug(body);

      senseIT=new Gson().fromJson(body, new TypeToken<SenseIT>() { }.getType());
      String jsondata=new Gson().toJson(senseIT.data);


      if (senseIT.data.history != null && senseIT.data.history.size()>0) {
         String csv = buildCSV(deviceid, sensorid, senseIT);

         // Create Directories
         logger.debug("Create directory");
         FileSystem fs = HdfsUtils.getFileSystemFromUri(hdfsuri);
         Path directoryraw = HdfsUtils.createDirectory(fs, "/iot/sigfox/senseit/raw");
         Path directorycsv = HdfsUtils.createDirectory(fs, "/iot/sigfox/senseit/csv");

         String filename = deviceid + "-" + sensorid + "-" + senseIT.data.sensor_type + "-"+page+"-" + (new Date()).getTime();

         logger.debug("Create file");
         // Creating a file in HDFS
         HdfsUtils.createFile(fs, directoryraw, filename + ".json", jsondata);

         logger.info("Import done of raw file : " + filename + ".json");

         HdfsUtils.createFile(fs, directorycsv, filename + ".csv", csv.getBytes());

         logger.info("Import done of csv file : " + filename + ".csv");
         return true;
      } else {
         logger.info("No more data.");
         return false;
      }

   }

   private static String buildCSV(String deviceid, String sensorid, SenseIT senseIT) {
      logger.info("Building CSV...");
      String csv="device;sensor;sensor_type;date;data;value\n";

      SimpleDateFormat sdfin=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
      SimpleDateFormat sdfout=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      Iterator<History> it_his=senseIT.data.history.iterator();
      while(it_his.hasNext()) {
         History his=it_his.next();
         String date=null;
         try {
            date=sdfout.format(sdfin.parse(his.date));
         } catch (ParseException e) {
            e.printStackTrace();
         }

         csv+=deviceid+ SEP +sensorid+ SEP +senseIT.data.sensor_type+ SEP +date+ SEP +his.data+ SEP +his.getDataInDouble()+"\n";
      }
      //logger.debug(csv);
      return csv;
   }

}
