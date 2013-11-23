/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package DBWorkers;

import VKWorkers.DownloaderGroup;
import org.neo4j.graphdb.Transaction;

/**
 *
 * @author pavel
 */
public class DBFillerGroup extends DBFiller {
    
    public DBFillerGroup (String groupId) {
        super(groupId);
        registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        nodeIndex = graphDb.index().forNodes("uids");
        tx.success();
        tx.close();
    }   
    
    @Override
    public void fillDB() {
        Transaction tx = graphDb.beginTx();
        fillFromGroup();
        tx.success();
        tx.close();
    }
    
    /**
     * Заполняет БД людьми из группы
     */
    private void fillFromGroup() {
        DownloaderGroup group = new DownloaderGroup(baseId);
        String[] ids = group.getGroupMembers();
        for(String id : ids){
            fillDB_Snowball(id);
        }
    }
    
}
