/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4jtestclient;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexHits;

/**
 *
 * @author avardhami
 */
public class SearchLucene {
    
    public static Iterable<Node> SearchLucene(DBClass newDB, String propertyName, String textToSearchOn)
    {
        return newDB.FindInFullTextIndex(propertyName, textToSearchOn);
    }
    
    public static Iterable<Node> SearchLuceneMultiLevel1(DBClass newDB, String propertyName, String textToSearchOn)
    {
        // DESC: does multi-level searching on the same text
        // NOTE1 : substantial hash checks required : O(n^2)
        // NOTE2 : substantial traversers required : O(n)
        // GIVEN (n) lucene results
        HashSet<Node> nodes = new HashSet<Node>();
        HashSet<Node> inMem = new HashSet<Node>();
        IndexHits<Node> ftNodes = newDB.FindInFullTextIndex(propertyName, textToSearchOn);
        
        for (Node node : ftNodes)
            inMem.add(node);
 
        for (Node node : inMem)
        {
            Traverser trav = node.traverse( Traverser.Order.BREADTH_FIRST,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE,
            MyStaticRelTypes.ISDIRECTPARENT, Direction.INCOMING );
            
            Collection<Node> tNodes = trav.getAllNodes();
            
            boolean foundAny = false;
            for (Node t : tNodes)
            {
                if (inMem.contains(t) && t.hasProperty(propertyName))
                {
                    if (!nodes.contains(t))
                        nodes.add(t);
                    foundAny = true;
                }
            }
                     
            // add the start node if none other
            if (foundAny && !nodes.contains(node))
                nodes.add(node);
        }
        
        return nodes;
    }    
    
    
    public static Iterable<Node> SearchLuceneMultiLevel2(DBClass newDB, final String propertyName, String textToSearchOn)
    {
        // NOTE: does multi-level searching on the same text
        // Simplified code by overriding traverser
        Set<Node> nodes = new HashSet<Node>();
        final HashSet<Node> inMem = new HashSet<Node>();
        IndexHits<Node> ftNodes = newDB.FindInFullTextIndex(propertyName, textToSearchOn);
        
        for (Node node : ftNodes)
            inMem.add(node);
 
        for (Node node : inMem)
        {
            Traverser trav = node.traverse( Traverser.Order.BREADTH_FIRST,
            StopEvaluator.END_OF_GRAPH, 
                new ReturnableEvaluator()
            {
                @Override
                public boolean isReturnableNode(
                        final TraversalPosition currentPos )
                {
                    return 
                            //!currentPos.isStartNode() &&
                            inMem.contains(currentPos.currentNode()) && 
                            currentPos.currentNode().hasProperty(propertyName);
                }
            },
            MyStaticRelTypes.ISDIRECTPARENT, Direction.INCOMING );

            Collection<Node> tNodes = trav.getAllNodes();
            //if (!tNodes.isEmpty() && !nodes.contains(node))
            //    nodes.add(node);
            
            if (tNodes.size() > 1)
                for (Node t : tNodes)
                    if (!nodes.contains(t))
                        nodes.add(t);
        }
        
        return nodes;
    }     
    
    public static void PrintLuceneResults(Iterable<Node> nodesInFT, String propertyName)
    {
        int numNodes = 0;
        
        Iterator itr = nodesInFT.iterator();
        
       // while(itr.hasNext()) {

        //    Node node = (Node)itr.next();
        //    System.out.println(node);
        //    System.out.println(node.getProperty(propertyName));
            //NOTE: to get the lucene score - not too great
         //   System.out.println(nodesInFT.currentScore());        
        //    numNodes++;
        //} 


        for ( Node node : nodesInFT )
        {
            System.out.println(node);
            System.out.println(node.getProperty(propertyName));
            numNodes++;
        }
        System.out.println(numNodes);        
    }    
}
