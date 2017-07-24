package com.github.randerzander;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;

import java.util.Properties;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

import java.io.FileReader;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import java.sql.*;

public class SaltSplitter {

  public static void main(String[] args){
    HashMap<String, String> props = getPropertiesMap(args[0]);
    List<String> splitPoints = new ArrayList<String>();

    try {
      System.out.println("Opening connection to: " + props.get("jdbc.url"));
      java.sql.Connection dbConnection = DriverManager.getConnection(props.get("jdbc.url"), "", "");
      String query = new String(Files.readAllBytes(Paths.get(props.get("query"))), StandardCharsets.UTF_8);
      System.out.println("Executing query to get split points..:\n" + query);
      ResultSet res = dbConnection.createStatement().executeQuery(query);
      while (res.next()){
        System.out.println(res.getString(1) + ", " + res.getString(2) + ", " + Integer.toString(res.getInt(3)) + ", " + Integer.toString(res.getInt(4)));
        splitPoints.add(res.getString(2));
      }
    } catch (Exception ex) {
      System.out.println("Error: unable to connect to Hive and generate splits: " + ex.toString());
      System.exit(1);
    }

    try {
      System.out.println("Initiating HBase connection..");
      Configuration config = HBaseConfiguration.create();
      config.addResource(new Path(props.get("hbaseConfDir"), "hbase-site.xml"));
      config.addResource(new Path(props.get("hadoopConfDir"), "core-site.xml"));
      Connection hConnection = ConnectionFactory.createConnection(config);
      Admin admin = hConnection.getAdmin();

      TableName tableName = TableName.valueOf(props.get("hbaseTable"));
      int saltBuckets = Integer.parseInt(props.get("saltBuckets"));
      int i = 0;
      for (String split : splitPoints){
        ByteBuffer b = ByteBuffer.allocate(4);
        b.order(ByteOrder.LITTLE_ENDIAN);
        byte[] saltBytes = b.putInt(i++ % saltBuckets).array();
        byte[] splitPoint = new byte[3 + split.length()];
        //Phoenix uses THREE bytes for salt bucket ids
        System.arraycopy(saltBytes, 0, splitPoint, 0, 3);
        System.arraycopy(split.getBytes(), 0, splitPoint, 3, split.length());
        System.out.println("Asynchronously splitting table " + tableName + " at salt bucket " + Integer.toString((i-1)%saltBuckets) + " and key bytes: " + split);
        admin.split(tableName, splitPoint);
        //Splitting temporarily takes the affected region offline, sleeping a few seconds prevents region unavailable errors
        Thread.sleep(Integer.parseInt(props.get("sleepTime")));
      }
    }
    catch (Exception ex) {
      System.out.println("Error while splitting regions: " + ex.toString());
      System.exit(1);
    }

  }

  public static HashMap<String, String> getPropertiesMap(String file){
    Properties props = new Properties();
    try{ props.load(new FileReader(file)); }
    catch(Exception e){ e.printStackTrace(); System.exit(-1); }

    HashMap<String, String> map = new HashMap<String, String>();
    for (final String name: props.stringPropertyNames()) map.put(name, (String)props.get(name));
    return map;
  }

  public static List<String> getRecords(String file){
    List<String> lines = new ArrayList<String>();
    try{
      Scanner sc = new Scanner(new File(file));
      while (sc.hasNextLine()) {
        lines.add(sc.nextLine());
      }
    }
    catch (java.io.FileNotFoundException ex){
      System.out.println("Error reading splitPoints from file: " + ex.toString());
      System.exit(1);
    }
    return lines;
  }

}
