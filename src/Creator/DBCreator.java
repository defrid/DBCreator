package Creator;

import DBWorkers.DBFiller;

public class DBCreator {
    
    public static void main(String[] args) {
        DBFiller db = new DBFiller("86030925");
        db.fillDB();
        db.setAllRelations();
        //calculateMetrics();
        //ExecutionEngine engine = new ExecutionEngine(graphDb);
        //ExecutionResult result = engine.execute("start n=node(*), m = node(*) where id(n) <> 0 and id(m) <> 0 and n.uid = m.uid return n.id, m.id");
        //System.out.println(result.toString());
    }
}
