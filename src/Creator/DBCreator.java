package Creator;

import DBWorkers.DBFiller;

/**
 * Main Class
 * @author pavel
 */
public class DBCreator {
    public static void main(String[] args) {
        DBFiller db = new DBFiller("86030925");
        db.fillDB();
        db.setAllRelations();
    }
}
