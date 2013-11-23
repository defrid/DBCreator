/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package DBWorkers;

import Creator.DBCreator;
import VKWorkers.Downloader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.tooling.GlobalGraphOperations;
import org.xml.sax.InputSource;

/**
 *
 * @author pavel
 */

public class DBFiller {
    
    protected static final String DB_PATH = "/home/pavel/workspace/neo4j/data/graph.db";
    protected static final String FRIEND_LIST_KEY = "friend_list";
    protected static final GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
    protected static Index<Node> nodeIndex;
    protected static final int MAX_LEVEL = 2;
    protected String baseId;
    
    protected static enum RelTypes implements RelationshipType {

        FRIEND
    }

    protected void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
    
    public DBFiller (String startId) {
        baseId = startId;
        registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        nodeIndex = graphDb.index().forNodes("uids");
        tx.success();
        tx.close();
    }    

    protected String getOneField(String name, Element e) {
        if (e.getChild(name) != null) {
            String temp = e.getChild(name).getText().replaceAll("\'", "");
            if (!temp.equals("")) {
                return temp;
            }
        }
        return "?";
    }
    
    protected HashMap<String, Object> parsePersonXML(String xml) {
        HashMap<String, Object> personData = new HashMap<String, Object>();
        try {
            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(new InputSource(new StringReader(xml)));
            Element root = doc.getRootElement();
            Element user = root.getChild("user");

            personData.put("uid", getOneField("uid", user));
            personData.put("first_name", getOneField("first_name", user));
            personData.put("last_name", getOneField("last_name", user));
            personData.put("sex", getOneField("sex", user));
            personData.put("bdate", getOneField("bdate", user));
            personData.put("city", getOneField("city", user));
            personData.put("can_post", getOneField("can_post", user));
            personData.put("status", getOneField("status", user));
            personData.put("relation", getOneField("relation", user));
            personData.put("nickname", getOneField("nickname", user));
            personData.put("interests", getOneField("interests", user));
            personData.put("movies", getOneField("movies", user));
            personData.put("tv", getOneField("tv", user));
            personData.put("books", getOneField("books", user));
            personData.put("games", getOneField("games", user));
            personData.put("about", getOneField("about", user));
            personData.put("counters", getOneField("counters", user));
        } catch (JDOMException ex) {
            Logger.getLogger(DBCreator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DBCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
        return personData;
    }

    protected void setRelationship(Node tempPerson, Node parentPerson) {
        Iterable<Relationship> relationships = tempPerson.getRelationships(Direction.BOTH);
        boolean relationAlreadyExist = false;
        for (Relationship tempRel : relationships) {
            if (tempRel.getOtherNode(tempPerson).getId() == parentPerson.getId()) {
                relationAlreadyExist = true;
                break;
            }
        }
        if (!relationAlreadyExist) {
            Transaction tx = graphDb.beginTx();
            try {
                parentPerson.createRelationshipTo(tempPerson, RelTypes.FRIEND);
                tx.success();
            } finally {
                tx.close();
            }
        }
    }

    protected Node addPersonToDB(HashMap<String, Object> personProperties) {
        Transaction tx = graphDb.beginTx();
        Node tempPerson = null;
        Node existingNode = nodeIndex.get("uid", personProperties.get("uid")).getSingle();
        try {
            if (existingNode == null) {
                tempPerson = graphDb.createNode();
                Iterator<String> keySetIterator = personProperties.keySet().iterator();
                while (keySetIterator.hasNext()) {
                    String key = keySetIterator.next();
                    tempPerson.setProperty(key, personProperties.get(key));
                }
                nodeIndex.add(tempPerson, "uid", personProperties.get("uid"));
            }
            tx.success();
        } finally {
            tx.close();
        }
        return tempPerson;
    }

    protected Node addPersonToDB(HashMap<String, Object> personProperties, String parentUid) {
        Node tempPerson = addPersonToDB(personProperties);
        Node parentPerson = nodeIndex.get("uid", parentUid).getSingle();
        Transaction tx = graphDb.beginTx();
        try {
            if (tempPerson == null) {
                tempPerson = nodeIndex.get("uid", personProperties.get("uid")).getSingle();
            }
            setRelationship(tempPerson, parentPerson);
            tx.success();
        } finally {
            tx.close();
        }
        return tempPerson;
    }
    
    public void fillDB() {
        Transaction tx = graphDb.beginTx();
        fillDB_Snowball();
        tx.success();
        tx.close();
    }
    
    protected void fillDB_Snowball() {
        Downloader person = new Downloader(baseId);
        HashMap<String, Object> personData = parsePersonXML(person.getPersonXMLData());
        String[] friendUids = person.getPersonFriends();
        personData.put(FRIEND_LIST_KEY, friendUids);
        addPersonToDB(personData);
        for (String tempFriend : friendUids) {
            recFillingDB(tempFriend, baseId, 1);
        }
    }

    protected void recFillingDB(String curUid, String parentUid, int lvl) {
        if (lvl == MAX_LEVEL) {
            return;
        }
        Downloader person = new Downloader(curUid);
        System.out.println("Parsing uid = " + curUid + " parentUid = " + parentUid);
        String[] friendsUids = person.getPersonFriends();
        Node existingNode = nodeIndex.get("uid", curUid).getSingle();
        if (existingNode == null) {
            HashMap<String, Object> personData = parsePersonXML(person.getPersonXMLData());
            System.out.println(" first_name = " + personData.get("first_name") 
                    + " last_name = " + personData.get("last_name") + " lvl = " + lvl);
            personData.put(FRIEND_LIST_KEY, friendsUids);
            existingNode = addPersonToDB(personData, parentUid);
        } else {
            setRelationship(existingNode, nodeIndex.get("uid", parentUid).getSingle());
        }
        for (String tempFriend : friendsUids) {
            recFillingDB(tempFriend, curUid, lvl + 1);
        }
    }
    
    protected void setOneNodeRelations(Node tempNode) {
        if (tempNode.hasProperty(FRIEND_LIST_KEY)) {
            String[] friendsUidList = (String[]) tempNode.getProperty(FRIEND_LIST_KEY);
            for (String tempFriendUid : friendsUidList) {
                Node tempFriend = nodeIndex.get("uid", tempFriendUid).getSingle();
                if (tempFriend == null) {
                    continue;
                }
                Iterable<Relationship> relationships = tempFriend.getRelationships(Direction.BOTH);
                boolean relationAlreadyExist = false;
                for (Relationship tempRel : relationships) {
                    if (tempRel.getEndNode().getId() == tempNode.getId()) {
                        relationAlreadyExist = true;
                        break;
                    }
                }
                if (!relationAlreadyExist) {
                    tempNode.createRelationshipTo(tempFriend, RelTypes.FRIEND);
                }
            }
        }
    }
    
    public void setAllRelations() {
        Transaction tx = graphDb.beginTx();
        try {
            GlobalGraphOperations glOper = GlobalGraphOperations.at(graphDb);
            Iterator<Node> allNodes = glOper.getAllNodes().iterator();
            while (allNodes.hasNext()) {
                setOneNodeRelations(allNodes.next());
            }
            tx.success();
        } finally {
            tx.close();
        }
    }
}
