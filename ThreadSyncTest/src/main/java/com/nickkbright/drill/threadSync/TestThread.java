package com.nickkbright.drill.threadSync;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestThread extends Thread{
    private String targetUrl;



    public void run() {
        try {
            URL url = new URL("http://www.google.com/");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.connect();
            int code = con.getResponseCode();
            con.disconnect();
            System.out.println(code);

        } catch (Exception e) {
            System.out.println("Something's wrong");
        }

    }
}


