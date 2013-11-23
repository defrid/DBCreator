/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package VKWorkers;

/**
 * Класс работы с VK API ориентированный на группы
 * @author pavel
 */
public class DownloaderGroup extends Downloader {
    
    public DownloaderGroup(String gid) {
        super(gid);
    }
    
    /**
     * Возвращает список членов группы
     * @return Массив ID членов группы
     */
    public String[] getGroupMembers() {
        return getPersons("groups.getMembers").split(",");
    }
    
}
