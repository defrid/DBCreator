/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package VKWorkers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 *
 * @author pavel
 */
public class Downloader {
    
    protected static final String BASE_URI = "https://api.vk.com/method/";
    protected String id;
    
    public Downloader(String _id) {
        id = _id;
    }
    
    protected String requestGET(String address) {
        String result = "";
        try {
            URL url = new URL(address);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("User-Agent", "Java bot");
            conn.connect();
            int code = conn.getResponseCode();
            if (code == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        conn.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    result += inputLine;
                }
                in.close();
            }
            conn.disconnect();
            conn = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
    
    protected String getPersons(String type) {
        String response = requestGET(BASE_URI + type + ".get?uid=" + id);
        int start = response.indexOf('[');
        int end = response.indexOf(']');
        return response.substring(start + 1, end);
    }
    
    public String[] getPersonFriends() {
        return getPersons("friends").split(",");
    }

    public String getPersonXMLData() {
        StringBuilder request = new StringBuilder(BASE_URI);
        request = request.append("users.get.xml?uid=");
        request = request.append(id);
        request = request.append("&fields=");
        request = request.append("sex");
        request = request.append(",bdate");
        request = request.append(",city");
        request = request.append(",can_post");
        request = request.append(",status");
        request = request.append(",relation");
        request = request.append(",nickname");
        request = request.append(",relatives");
        request = request.append(",activities");
        request = request.append(",interests");
        request = request.append(",movies");
        request = request.append(",tv");
        request = request.append(",books");
        request = request.append(",games");
        request = request.append(",about");
        request = request.append(",personal");
        request = request.append(",counters");
        String response = requestGET(request.toString());
        return response;
    }
}
