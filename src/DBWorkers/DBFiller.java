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
 * This class creates and fills DB
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

    /**
     * Регистрирует "выключатель" обращения к графу в базе данных
     * @param graphDb Граф
     */
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
    /**
     * Возвращает одно конкретное поле
     * @param name Имя поля
     * @param e xml-элемент
     * @return Поле xml
     */
    protected String getOneField(String name, Element e) {
        if (e.getChild(name) != null) {
            String temp = e.getChild(name).getText().replaceAll("\'", "");
            if (!temp.equals("")) {
                return temp;
            }
        }
        return "?";
    }
    
    /**
     * Возвращает HashMap из Xml
     * @param xml
     * @return Hash map из Xml
     */
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

    /**
     * Создает связь между двумя узлами
     * @param curPerson Текущий узел
     * @param parentPerson Родительский узел
     */
    protected void setRelationship(Node curPerson, Node parentPerson) {
        Iterable<Relationship> relationships = curPerson.getRelationships(Direction.BOTH);
        boolean relationAlreadyExist = false;
        for (Relationship tempRel : relationships) {
            if (tempRel.getOtherNode(curPerson).getId() == parentPerson.getId()) {
                relationAlreadyExist = true;
                break;
            }
        }
        if (!relationAlreadyExist) {
            Transaction tx = graphDb.beginTx();
            try {
                parentPerson.createRelationshipTo(curPerson, RelTypes.FRIEND);
                tx.success();
            } finally {
                tx.close();
            }
        }
    }

    /**
     * Добавляет человека в базу данных
     * @param personProperties Поля базы данных
     * @return Добавленый узел
     */
    protected Node addPersonToDB(HashMap<String, Object> personProperties) {
        Transaction tx = graphDb.beginTx();
        Node curPerson = null;
        Node existingNode = nodeIndex.get("uid", personProperties.get("uid")).getSingle();
        try {
            if (existingNode == null) {
                curPerson = graphDb.createNode();
                Iterator<String> keySetIterator = personProperties.keySet().iterator();
                while (keySetIterator.hasNext()) {
                    String key = keySetIterator.next();
                    curPerson.setProperty(key, personProperties.get(key));
                }
                nodeIndex.add(curPerson, "uid", personProperties.get("uid"));
            }
            tx.success();
        } finally {
            tx.close();
        }
        return curPerson;
    }

    /**
     * Добавляет человека в базу данных с занесением связи с родителем
     * @param personProperties Поля базы данных
     * @param parentUid ID родителя
     * @return Добавленый узел
     */
    protected Node addPersonToDB(HashMap<String, Object> personProperties, String parentUid) {
        Node curPerson = addPersonToDB(personProperties);
        Node parentPerson = nodeIndex.get("uid", parentUid).getSingle();
        Transaction tx = graphDb.beginTx();
        try {
            if (curPerson == null) {
                curPerson = nodeIndex.get("uid", personProperties.get("uid")).getSingle();
            }
            setRelationship(curPerson, parentPerson);
            tx.success();
        } finally {
            tx.close();
        }
        return curPerson;
    }
    
    /**
     * Заполняет базу данных
     */
    public void fillDB() {
        Transaction tx = graphDb.beginTx();
        fillDB_Snowball(baseId);
        tx.success();
        tx.close();
    }
    
    /**
     * Заполнение БД от текущего центра
     * @param id ID центра
     */
    protected void fillDB_Snowball(String id) {
        Downloader person = new Downloader(id);
        HashMap<String, Object> personData = parsePersonXML(person.getPersonXMLData());
        String[] friendUids = person.getPersonFriends();
        personData.put(FRIEND_LIST_KEY, friendUids);
        addPersonToDB(personData);
        for (String tempFriend : friendUids) {
            recFillingDB(tempFriend, id, 1);
        }
    }
    
    /**
     * Рекурсивное заполнение до нижнего уровня от текущего узла и добавление связей между узлами
     * @param curUid Текущий узел
     * @param parentUid Родительский узел
     * @param lvl Текущий уровень
     */
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
    
    /**
     * Добавление связей между узлом и его друзьями
     * @param curNode Текущий узел
     */
    protected void setOneNodeRelations(Node curNode) {
        if (curNode.hasProperty(FRIEND_LIST_KEY)) {
            String[] friendsUidList = (String[]) curNode.getProperty(FRIEND_LIST_KEY);
            for (String tempFriendUid : friendsUidList) {
                Node tempFriend = nodeIndex.get("uid", tempFriendUid).getSingle();
                if (tempFriend == null) {
                    continue;
                }
                Iterable<Relationship> relationships = tempFriend.getRelationships(Direction.BOTH);
                boolean relationAlreadyExist = false;
                for (Relationship tempRel : relationships) {
                    if (tempRel.getEndNode().getId() == curNode.getId()) {
                        relationAlreadyExist = true;
                        break;
                    }
                }
                if (!relationAlreadyExist) {
                    curNode.createRelationshipTo(tempFriend, RelTypes.FRIEND);
                }
            }
        }
    }
    
    /**
     * Расставляет недостающие связи между узлами
     */
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
