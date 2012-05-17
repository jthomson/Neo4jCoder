/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4jtestclient;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;

/**
 *
 * @author avardhami
 */
public class DBClass {
        
    public GraphDatabaseService GraphDb;
    public Node RootNode;
    public Index<Node> fulltextEnglishTerms;
    public IndexManager index;

    public DBClass(String dir, boolean createIndex)
    {
        // directory based db (one per)
        GraphDb = new EmbeddedGraphDatabase(dir);
        
        RootNode = GraphDb.getReferenceNode();
        if (RootNode == null)
            RootNode = GraphDb.createNode();
        
        if (createIndex)
            CreateIndex();
    }
    
    private void CreateIndex()
    {
        index = GraphDb.index();
        fulltextEnglishTerms = index.forNodes( "englishTerms-fulltext",
                MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
        
        //fulltextMovies.add( theMatrix, "Text", "The Matrix" );
        //fulltextMovies.add( theMatrixReloaded, "Text", "The Matrix Reloaded" );
        // search in the fulltext index
        //Node found = fulltextMovies.query( "Text", "reloAdEd" ).getSingle();
    }
    
    private void AddToFullTextIndex(Node node, String propertyText)
    {
        if (fulltextEnglishTerms != null)
            fulltextEnglishTerms.add( node, propertyText, node.getProperty( propertyText) );
    }
    
    public IndexHits<Node> FindInFullTextIndex(String propertyText, String key)
    {
        if (fulltextEnglishTerms != null)
            return fulltextEnglishTerms.query( propertyText, key );
        else
            return null;
    }
    
    
    public Node AddNode(Node startNode, MyStaticRelTypes reltype, int id)
    {
        Node newNode = GraphDb.createNode();
        startNode.createRelationshipTo(newNode, reltype);
        if (id > 0)
            assignId(startNode, id);
        
        return newNode;
    }    
    
    public Node AddNode(Node startNode, MyStaticRelTypes reltype)
    {
        return AddNode(startNode, reltype, -1);
    }
    
    private void assignId(Node node, int id)
    {
        NodeProperty prop = new NodeProperty();
        prop.Name = "Id";
        prop.Value = id;
        SetProperty(node, prop);
    }
    
    public void SetTextProperty(Node node)
    {
        NodeProperty prop = new NodeProperty();
        prop.Name = "Text";
        prop.Value = TermTexts.GetRandomText();
        SetProperty(node, prop);
        if (fulltextEnglishTerms != null)
            AddToFullTextIndex(node, "Text");
    }
    
    public void SetProperty(Node node, NodeProperty property)
    {
        node.setProperty(property.Name, property.Value);
    }
    
    public static Traverser GetPath( final Node term )
    {
        return term.traverse( Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, MyStaticRelTypes.ISDIRECTPARENT,
                Direction.OUTGOING );
    }
    
    public static void PrintPath(Traverser termsInPath)
    {
        int numNodes = 0;
        for ( Node friendNode : termsInPath )
        {
           numNodes++;
           if (friendNode.hasProperty("Id"))
           //System.out.println(friendNode);
           System.out.println(
                       termsInPath.currentPosition().depth() +
                       " => " +
                       friendNode.getProperty( "Id" ));
        }
        System.out.println(numNodes);
    }
}

enum MyStaticRelTypes implements RelationshipType
{
    ISDIRECTPARENT
}