package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class ComToo {
    public static Map<String, String> generateColorDict(String path) {
        Map<String, String> _r = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String str = reader.readLine();
            while (str != null) {
                String[] line = str.split(",");
                _r.put(line[0], line[1]);
                str = reader.readLine();
            }
        } catch (IOException e) {
            log.error(e);
        }
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
                    if (ss.length() != 0) {
                        terms.add(ss.toString());
                    }
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
}
