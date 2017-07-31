package com.github.randerzander;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.NotServingRegionException;

import java.util.Properties;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import java.sql.*;

public class SaltSplitter {

  private static String query;
  private static Connection hConnection;
  private static Admin admin;

  public static void main(String[] args){
    HashMap<String, String> props = getPropertiesMap(args[0]);
    List<String> splitPoints = new ArrayList<String>();

    //Read Hive query text for generating splits
    String query = "";
    try {
      query = new String(Files.readAllBytes(Paths.get(props.get("query"))), StandardCharsets.UTF_8);
    } catch (IOException ex){
      System.out.println("Error: unable to read query text for generating splits: " + ex.toString());
      System.exit(1);
    }

    //Issuing Hive query to calculate splits
    try {
      System.out.println("Opening connection to: " + props.get("jdbc.url"));
      String user = props.get("jdbc.user");
      String pass = props.get("jdbc.password");
      java.sql.Connection dbConnection = DriverManager.getConnection(props.get("jdbc.url"), user, pass);
      System.out.println("Executing query from " + props.get("query") + " to calculate split points..\n");
      ResultSet res = dbConnection.createStatement().executeQuery(query);
      while (res.next()){
        System.out.println("New splitPoint: " + res.getString(2));
        splitPoints.add(res.getString(2));
      }
    } catch (SQLException ex) {
      System.out.println("Error: unable to connect to Hive and generate splits: " + ex.toString());
      System.exit(1);
    }

    System.out.println("Generated " + splitPoints.size() + " splitPoints from query results. Initiating HBase connection..");

    //Configure HBase connection
    Configuration config = HBaseConfiguration.create();
    config.addResource(new Path(props.get("hbaseConfDir"), "hbase-site.xml"));
    config.addResource(new Path(props.get("hadoopConfDir"), "core-site.xml"));
    if (props.get("kerberized").equals("true")) {
      System.setProperty("java.security.krb5.conf", props.get("krb5Conf"));
      System.setProperty("sun.security.krb5.debug", props.get("krb5Debug"));
      String principal = System.getProperty("kerberosPrincipal", props.get("hbasePrincipal"));
      String keytabLocation = System.getProperty("kerberosKeytab", props.get("hbaseKeytab"));

      UserGroupInformation.setConfiguration(config);
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
      } catch (IOException ex){
        System.out.println("Error: unable to authenticate with Kerberos keytab: " + ex.toString());
        System.exit(1);
      }
    }

    //Connect to HBase
    try{
      hConnection = ConnectionFactory.createConnection(config);
      admin = hConnection.getAdmin();
    } catch (IOException ex){
      System.out.println("Error: unable to connect to HBase: " + ex.toString());
      System.exit(1);
    }

    TableName tableName = TableName.valueOf(props.get("hbaseTable"));
    int saltBuckets = Integer.parseInt(props.get("saltBuckets"));
    int i = 0;
    //Apply every split to every salt bucket
    for (String split : splitPoints){
      for (int j = 0; j < saltBuckets; j++){
        ByteBuffer b = ByteBuffer.allocate(4);
        b.order(ByteOrder.LITTLE_ENDIAN);
        byte[] saltBytes = b.putInt(j).array();
        byte[] splitPoint = new byte[3 + split.getBytes().length];
        //Phoenix uses THREE bytes for salt bucket ids
        System.arraycopy(saltBytes, 0, splitPoint, 0, 3);
        System.arraycopy(split.getBytes(), 0, splitPoint, 3, split.length());
        System.out.println("Considering splitting " + tableName + " at salt: " + Integer.toString(j) +  " bytes: " + hex(splitPoint));

        //Iterate through all region start keys for the table
        boolean splitAlreadyExists = false;
        try{
          for (byte[] startKey : hConnection.getRegionLocator(tableName).getStartKeys()){
            if (Arrays.equals(startKey, splitPoint)){
              splitAlreadyExists = true;
              break;
            }
          }
        } catch (IOException ex){
          System.out.println("Error: unable to get region metadata: " + ex.toString());
          System.exit(1);
        }

        if (splitAlreadyExists)
          System.out.println("Skipping dupe salt: " + Integer.toString(j) + ", key: " + split);
        else{
          try{
            System.out.println("Splitting at salt: " + Integer.toString(j) + " key: " + split);
            admin.split(tableName, splitPoint);
          }
          catch (NotServingRegionException ex){
            System.out.println("Region moved before attempting to split, skipping split.");
          }
          catch (IOException ex){
            System.out.println("Error issuing split: " + ex.toString());
          }
        }
      }

      //Wait for all regions to be online
      System.out.println("Waiting for all regions to come back online..");
      try{
        boolean allOnline = false;
        while(!allOnline){
          Thread.sleep(100);
          allOnline = true;
            List<HRegionInfo> regions = admin.getTableRegions(tableName);
            for (HRegionInfo region : regions)
              if (region.isOffline()){
                allOnline = false;
                break;
              }
        }
      }
      catch (InterruptedException | IOException ex){
        System.out.println("Error obtaining region information: " + ex.toString());
        System.exit(1);
      }
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

  private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
  public static String hex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for ( int j = 0; j < bytes.length; j++ ) {
        int v = bytes[j] & 0xFF;
        hexChars[j * 2] = hexArray[v >>> 4];
        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }

}
