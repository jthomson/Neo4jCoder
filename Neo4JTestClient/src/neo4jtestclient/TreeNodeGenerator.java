/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4jtestclient;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 *
 * @author avardhami
 */
public class TreeNodeGenerator {

    // define level breadth size
    //private static int[] levelBreadth = new int[]{ 30, 300, 3000, 30000, 300000};
    private static int[] levelBreadth = new int[]{ 30, 300, 3000, 10000, 80000};
    //private static int[] levelBreadth = new int[]{ 30, 300};
    private static int counterId = 1;
    private static DBClass dbInstance;
            
    public static void AddNodes(DBClass newInstance)
    {
        dbInstance = newInstance;
        
        // start Tx
        Transaction tx = dbInstance.GraphDb.beginTx();

        try
        {
            // start the recursion
            AddNodesToParent(newInstance.RootNode, 0, levelBreadth[0]);
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            System.out.println("Transaction failure:"+e.toString());
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
    }
    
    private static void AddNodesToParent(Node parentNode, int level, int nodesPerParent)
    {
        // 1. generate all nodes pertaining to parent
        //System.out.println(level);
        int nextLevel = level + 1;
        boolean hasReachedTheEnd = (nextLevel >= levelBreadth.length);
        // 2. determine their children nodesPerParent
        int nextNodesPerParent = hasReachedTheEnd ? 0 : levelBreadth[nextLevel] / levelBreadth[level];
        
        for (int i = 0; i < nodesPerParent; i++)
        {
            Node node = dbInstance.AddNode(parentNode, MyStaticRelTypes.ISDIRECTPARENT, counterId++);

            // terminate if reached the end of the tree
            if (!hasReachedTheEnd)
                // 3. call recursively to add children to those nodes
                AddNodesToParent(node, nextLevel, nextNodesPerParent);

            dbInstance.SetTextProperty(node);
        }
    }
    
    public static void AddMoreNodes(DBClass newInstance, long numberOfNodes)
    {
        dbInstance = newInstance;
        int lastLevelBreadth = levelBreadth[levelBreadth.length-1];
        
        long nextNodesPerParent = numberOfNodes / lastLevelBreadth;
        
        if (nextNodesPerParent < 1)
        {
            lastLevelBreadth = (int)numberOfNodes;
            nextNodesPerParent = 1;
        }
        
        // start Tx
        Transaction tx = dbInstance.GraphDb.beginTx();

        try
        {
            // start the recursion
            AddMoreNodes(lastLevelBreadth, nextNodesPerParent);
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            System.out.println("Transaction failure:"+e.toString());
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
    }  
    
    private static void AddMoreNodes(int lastLevelBreadth, long nodesPerParent)
    {
        for (int k = 0; k < lastLevelBreadth; k++)
        {
            Node parentNode = dbInstance.GraphDb.getNodeById(k);
            for (long i = 0; i < nodesPerParent; i++)
            {
                dbInstance.AddNode(parentNode, MyStaticRelTypes.ISDIRECTPARENT, counterId++);
            }
        }
    }    
}
