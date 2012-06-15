/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4jtestclient;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;

/**
 *
 * @author avardhami
 */
public class Neo4JTestClient {


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException{
        
        Importer importer = new Importer("/neoDB");
        importer.runImport();     
    }
    
    
    private static void InitializeGraph()
    {
        //DBClass newDB = new DBClass("d:\\batch-import\\target\\db", true);

        try
        {
            //BuildNewDB(newDB);
            //TraverseDB(newDB);
            //BClass.PrintPath(DBClass.GetPath());

            
            String propertyName = "Text";
            String textToSearch = "oth*";
            
            
            
            //String textToSearch = "prem*";
            //Iterable<Node> nodes = SearchLucene.SearchLucene(newDB, propertyName, textToSearch);
            //Iterable<Node> nodes = SearchLucene.SearchLuceneMultiLevel2(newDB, propertyName, textToSearch);
            //SearchLucene.PrintLuceneResults(nodes, propertyName);
        }
        finally
        {
            //if (newDB.GraphDb != null)
            //    newDB.GraphDb.shutdown();
        }      
    }
    
    private static void BuildNewDB(DBClass newDB)
    {
        TreeNodeGenerator.AddNodes(newDB);
    }
    
    private static void AddToDB(DBClass newDB)
    {
        for (int i = 0; i < 1000; i++)
            TreeNodeGenerator.AddMoreNodes(newDB, 100000);
    }

    private static void TraverseDB(DBClass newDB)
    {
        Node node = newDB.GraphDb.getNodeById(1);
        System.out.println(node);
        newDB.PrintPath(newDB.GetPath(node));
    }
    

}
