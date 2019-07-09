package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;

@Log4j2
public class ComToo {

    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(bytes[i] & 0xFF);
            if(hex.length() < 2){
                sb.append(0);
            }
            sb.append(hex);
        }
        return sb.toString();
    }

    public static String hashAlgo(String str)  {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(str.getBytes());
            byte b[] = md.digest();
            return bytesToHex(b);
        } catch (NoSuchAlgorithmException e) {
            log.error(e);
            return null;
        }
    }

    public static Map<String, SpcCon> generateSpcConfig(String path) {
        Map<String, SpcCon> _r = new HashMap<>();
        int count = 0;
        int uplimitIndex = 0, lowlimitIndex = 0, norminalIndex = 0;
        int spcNameIndex = 0, stationIndex = 0;
        int indexCount = 0;
        String[] headers;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String str;
            while ((str = reader.readLine()) != null) {
                if (count == 0) {
                    headers = str.split(",");
                    for (int i = 0; i < headers.length; i++) {
                        switch (headers[i]) {
                            case "low_limit":
                                lowlimitIndex = i;
                                indexCount++;
                                break;
                            case "up_limit":
                                uplimitIndex = i;
                                indexCount++;
                                break;
                            case "norminal":
                                norminalIndex = i;
                                indexCount++;
                                break;
                            case "spc":
                                spcNameIndex = i;
                                indexCount++;
                                break;
                            case "station":
                                stationIndex = i;
                                indexCount++;
                                break;
                        }
                        if (indexCount == 5) break;
                    }
                } else {
                    String[] line = str.split(",");
                    try {
                        SpcCon spcCon = new SpcCon(Double.parseDouble(line[norminalIndex]),
                                Double.parseDouble(line[uplimitIndex]),
                                Double.parseDouble(line[lowlimitIndex]));
                        _r.put(line[stationIndex] + ":" + line[spcNameIndex], spcCon);
                    } catch (Exception e) {
                        log.error("parse int error, continue -> " + e);
                    }
                }
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//        _r.forEach((key, val) -> {
//            System.out.println(key + " \t: " + val.getUplimit() + ";\t" + val.getDownlimit() + ";\t" + val.getNormal()+ ";");
//        });
        return _r;
    }

    public static List<String> parseArrayString(String orig) {
        List<String> terms = new ArrayList<>();
        StringBuilder ss = new StringBuilder();
        // split
        boolean openCollect = false;
        for (int i = 0; i < orig.length(); i++) {
            if (orig.charAt(i) == '"') {
                if (openCollect) {
                    terms.add(ss.toString());
                    ss.delete(0, ss.length());
                }
                openCollect = !openCollect;
                continue;
            }
            if (openCollect) {
                ss.append(orig.charAt(i));
            }
        }
        return terms;
    }

    public static long timestampToLong(String time, String format) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = simpleDateFormat.parse(time);
        return date.getTime();
    }

    public static boolean isDouble(String n) {
        try {
            Double.parseDouble(n);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * CURD of database
     */
    public static void updateMongo(MongoClient mongoClient, String database, String col, String key, String val, Document doc) {
        mongoClient.getDatabase(database).getCollection(col).findOneAndUpdate(eq(key, val), new Document("$set", doc));
    }

    public static void upsertMongo(MongoClient mongoClient, String database, String col, String key, String val, Document doc) {
        mongoClient.getDatabase(database).getCollection(col).replaceOne(eq(key, val), doc, new UpdateOptions().upsert(true));
    }

    public static Document findOneMongo(MongoClient mongoClient, String database, String col, String key, String val) {
        MongoCursor<Document> mongoCursor = mongoClient.getDatabase(database).getCollection(col).find(eq(key, val)).iterator();
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        } else {
            return null;
        }
    }

    public static void insert(MongoClient mongoClient, String database, String coll, Document doc) {
        mongoClient.getDatabase(database).getCollection(coll).insertOne(doc);
    }

}
