/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package VKWorkers;

/**
 *
 * @author pavel
 */
public class DownloaderGroup extends Downloader {
    
    public DownloaderGroup(String gid) {
        super(gid);
    }
    
    public String[] getGroupMembers() {
        return getPersons("groups").split(",");
    }
    
}
