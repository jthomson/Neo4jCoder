package neo4jtestclient;

/**
 *
 * @author JThomson
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import javax.activity.InvalidActivityException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import org.neo4j.index.impl.lucene.Cache;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Importer {
    private static Importer.Report m_Report;
    private EmbeddedGraphDatabase m_Db;
    private Node m_RootNode; 
    private Transaction m_Tran;

    private String m_DbLocation;
    
    private Index<Node> m_EnglishTextIndex;
    private int m_NumAddedEnglishNodes;
    
    private Index<Node> m_CodeIndex;
    private int m_NumAddedCodeNodes;
    
    private int m_NumAddedTermComponentRelationships;
    private int m_NumMissingCodesinRelationships;
    private int m_NumMissingComponentsinRelationships;
    
    private int m_NumDictCodeRels;
    private int m_NumCodeCodeRels;
    private int m_NumSameCodeRels;

    private static String s_Delimiter = ";";
    private static String s_TopLevelKeyword = "Top";
    private static int s_ResultLimit = 25;
    
        private int m_CommitSize = 10000;

    public Importer(String dbLocation, boolean startNew) {
        
        if (dbLocation == null || dbLocation.length() == 0) {
            throw new IllegalArgumentException("dbLocation must be provided!");
        }     
            
        m_DbLocation = dbLocation;
        m_Db = new EmbeddedGraphDatabase(m_DbLocation);
        registerShutdownHook(m_Db);
        
        m_RootNode = m_Db.getReferenceNode();
        if (m_RootNode == null) m_RootNode = m_Db.createNode();
        
        m_EnglishTextIndex = m_Db.index().forNodes("EnglishText", LuceneIndexImplementation.FULLTEXT_CONFIG );
        // TODO: to_lower_case=true in englishtext.
        
        m_NumAddedEnglishNodes = 0;
        
        m_CodeIndex = m_Db.index().forNodes("Code", EXACT_CONFIG);
        m_NumAddedCodeNodes = 0;
        
        m_NumAddedTermComponentRelationships = 0;
        m_NumMissingCodesinRelationships = 0;
        m_NumMissingComponentsinRelationships = 0;
        m_NumDictCodeRels = 0;
        m_NumCodeCodeRels = 0;
        m_NumSameCodeRels = 0;
        
        if (startNew) {
            File file = new File(m_DbLocation);
            deleteFileOrDirectory(file);
            if (!file.exists()) file.mkdirs(); 
            
            setupDictionaryBase();
        }
         
        
        m_Report = new Importer.Report(100000, 100);       
        
    }
    
    private void clearCache() {
        
//        Cache cache = m_Db.getManagementBean(Cache.class);
//        cache.clear();
    }
    
    private Map<String, String> getConfig() {
        return stringMap(
            "dump_configuration", "true",
            "cache_type", "none",
            "neostore.propertystore.db.index.keys.mapped_memory", "10M",
            "neostore.propertystore.db.index.mapped_memory", "10M",
            "neostore.nodestore.db.mapped_memory", "400M",
            "neostore.relationshipstore.db.mapped_memory", "150M",
            "neostore.propertystore.db.mapped_memory", "400M",
            "neostore.propertystore.db.strings.mapped_memory", "200M");
    }
    
    private void setupDictionaryBase() {
        
        m_Tran = m_Db.beginTx();
        createDictionaryBase();

        m_Tran.success();
        m_Tran.finish();       
    }
   
    public void importFile(FileType type, String fileLocation) throws IOException {
  
        try {   
            File file = new File(fileLocation);

            if (file == null || file.length() == 0) {
                throw new IllegalArgumentException("fileLocation must be provided!");
            }

            m_Tran = m_Db.beginTx();

            switch (type)
            {
                case Terms:
                    importTermDataNodes(file);
                    break;
                
                case TermRels:
                    importTermRelationships(file);
                    break;
              
                case Components:
                    importComponentDataNodes(file);
                    break;
             
                case TermComponentRels:
                    importTermComponentRelationships(file);
                    break;
            }

            m_Tran.success();
            outputResults(type);
            
        } finally {
            finish();
        }
    }
    
    private void importTermComponentRelationships(File file) throws IOException {

        BufferedReader bf = new BufferedReader(new FileReader(file));
        try 
        {
            final Importer.Data data = new Importer.Data(bf.readLine(), s_Delimiter, 0);
            String line, component, code, componentType, level;
            Map<String,Object> map;
            m_Report.reset();
            int counter = 0;
            while ((line = bf.readLine()) != null) {

                counter++;

                // skip blank lines
                if (line.trim().length() == 0) {
                    continue;
                }

                map = map(data.update(line));

                component = map.get("Component").toString();
                componentType = map.get("ComponentType").toString();
                code = map.get("Code").toString();
                level = map.get("Level").toString();

                addTermComponentRelationship(componentType, component, code, level);

                ensureRegularCommit(counter);

                m_Report.dots();
            }
        }
        finally
        {
            if (bf != null) bf.close();
        }
        m_Report.finishImport("TermComponentRelationships");   
        if (m_NumMissingCodesinRelationships > 0) {
            System.out.println("Missing Codes in Relationship Input: " + m_NumMissingCodesinRelationships);
        }
        if (m_NumMissingComponentsinRelationships > 0) {
            System.out.println("Missing Components in Relationship Input: " + m_NumMissingComponentsinRelationships);
        }
    }
    
    private void addTermRelationship(String childCode, String parentCode, String level) {
        
        Node childNode = getCodeNodeByCode(childCode);
        if (childNode == null) {
            String a = childCode;
            String b = parentCode;
        }
        
        
        if (s_TopLevelKeyword.equals(parentCode)) {
            m_RootNode.createRelationshipTo(childNode, DictRelType.valueOf(level));
            m_NumDictCodeRels++;
        }
        // create new child code node if code is the same across levels.  
        // Don't add it to the index.
        else if (childCode == parentCode) {
            Node parentNode = getCodeNodeByCode(parentCode);
            Node newChildNode = addCodeNode(childCode, false);
            parentNode.createRelationshipTo(newChildNode, DictRelType.valueOf(level));
            m_NumSameCodeRels++;
        }
        else {
            Node parentNode = getCodeNodeByCode(parentCode);
            parentNode.createRelationshipTo(childNode, DictRelType.valueOf(level));
            m_NumCodeCodeRels++;
        }
    }
    
    private void addTermComponentRelationship(String componentType, String component, String code, String level) throws InvalidActivityException {
        
        Node componentNode = getEnglishNodeByText(component);
        
        Node codeNode = getCodeNodeByCode(code);
        
        if (componentNode == null) {
            m_NumMissingComponentsinRelationships++;
            System.out.println(String.format("componentNode not found. code=%s, componentType=%s, component=%s, level=%s", code, componentType, component, level));
            return;
            
            //throw new InvalidActivityException("Component is null in addTermComponentRelationship!");        
        }
        if (codeNode == null) {
            m_NumMissingCodesinRelationships++;
            System.out.println(String.format("codeNode not found. code=%s, componentType=%s, component=%s, level=%s", code, componentType, component, level));
            return;
            
            //throw new InvalidActivityException("codeNode not retrieved in addTermComponentRelationship!");
        }
        
        codeNode.createRelationshipTo(componentNode, DictRelType.valueOf(componentType));
        m_NumAddedTermComponentRelationships++;
             
    }
    
     private void importComponentDataNodes(File file) throws IOException 
    {     
        BufferedReader bf = new BufferedReader(new FileReader(file));
        try 
        {
            final Importer.Data data = new Importer.Data(bf.readLine(), s_Delimiter, 0);
            String line, componentName;
            Map<String,Object> map;
            m_Report.reset();
            int counter = 0;
            while ((line = bf.readLine()) != null) {

                counter++;

                // skip blank lines
                if (line.trim().length() == 0) {
                    continue;
                }

                map = map(data.update(line));
                
                componentName = map.get("ComponentName").toString();
                addComponentNode(componentName);

                ensureRegularCommit(counter);

                m_Report.dots();
            }
        }
        finally {
            if (bf != null) bf.close();
        }
        m_Report.finishImport("ComponentNodes");
    }
    
    private void ensureRegularCommit(int counter) {

        if (counter % m_CommitSize == 0) {
            m_Tran.success();
            m_Tran.finish();

            m_Tran = null;
            m_Tran = m_Db.beginTx();
        }
    }

    public void runCypherTests() {
        
        testCypherSearch();           
    }
    
    public void runLuceneTests() {
        
        String antiHyperTensives = "start terms=node:EnglishText(" + Constants.Property.EnglishText + "='ANTIHYPERTENSIVES') return terms";
        stringSearch(antiHyperTensives);

        englishTermLevelSearch("ANTIHYPERTENSIVES");
        
        String twoWildcard = "start terms=node:EnglishText(\"EnglishText:A* AND EnglishText:D*\") return terms";
        stringSearch(twoWildcard);
        
        //String termsAtLevel = "start terms=node:EnglishText(\"EnglishText:A*\") match terms-[:ComponentEnglish]-a where a.ComponentName='DRUGRECORDNUMBER' return a";
        //stringSearch(termsAtLevel);   
        
        String allTerms = "start terms=node:EnglishText(\"EnglishText:*\") return terms";
        stringSearch(allTerms);
        
    }
    
    public void englishTermSearch(String text) {
        
        String searchString = "start terms=node:EnglishText(\"" + Constants.Property.EnglishText + ":" + text + "\") match terms-[:TermEnglish]-a return count(distinct terms)";
        stringSearch(searchString);     
    }
        
    public void englishLevelTermSearch(String text) {
        
        String searchString = "start levels=node:Level(LevelN = \"*\") match levels-[:LevelCode]-a-[:TermEnglish]-terms where terms.EnglishText = '" + text + "' return count(distinct terms)";
        stringSearch(searchString);     
    }    

    public void englishTermLevelSearch(String text) {
        
        String searchString = "start terms=node:EnglishText(\"" + Constants.Property.EnglishText + ":" + text + "\") match terms-[:TermEnglish]-a-[:LevelCode]-level return count(distinct terms)";
        stringSearch(searchString);     
    }
    public void englishTermLevelSearchTwoEndpoints(String text) {
        
        String searchString = "start levels=node:Level(\"LevelN:*\"), terms=node:EnglishText(\"" + Constants.Property.EnglishText + ":" 
                    + text + "\") match levels-[:LevelCode]-a-[:TermEnglish]-terms return count(distinct terms)";
        stringSearch(searchString);      
    }
            
    public void searchTermLevel(String text, String level) {
              
        String searchString = "start terms=node:EnglishText(\"EnglishText:" + text 
                + "\") match terms<-[:TermEnglish]-a-[:LevelCode]->level where level." + Constants.Property.LevelName + "='" + level + "' return count(distinct terms)";
        stringSearch(searchString);          
    }
    
    public void countTermsByLevelNoDirection(String text, String level) {
              
        String searchString = "start terms=node:EnglishText(\"EnglishText:" + text 
                + "\") match terms-[:TermEnglish]-a-[:LevelCode]-level where level." + Constants.Property.LevelName + "='" + level + "' return count(distinct terms)";
        stringSearch(searchString);          
    }
    
    public void getTermsByLevelNoDirection(String text, String level) {
              
        String searchString = "start terms=node:EnglishText(\"EnglishText:" + text 
                + "\") match terms-[:TermEnglish]-a-[:LevelCode]-level where level." + Constants.Property.LevelName + "='" + level + "' return distinct terms";
        stringSearch(searchString);          
    }    
    
    public void getTermsByLevelNoDirectionSorted(String text, String level) {
              
        String searchString = "start terms=node:EnglishText(\"EnglishText:" + text 
                + "\") match terms-[:TermEnglish]-a-[:LevelCode]-level where level." + Constants.Property.LevelName + "='" + level +
                "' return distinct terms order by terms." + Constants.Property.EnglishText + " asc limit " + s_ResultLimit;
        stringSearch(searchString);          
    }   
    
    
    public void getTermComponent(String term, String dictionaryLevel, String component, String componentType) {
    
        String searchString = "start compTextNodes=node:EnglishText(\"EnglishText:" + component + "\")"
                + " match compTextNodes-[:" + componentType + "]-baseTerm-[:TermEnglish]-termText, baseTerm-[:LevelCode]-level " 
                + "where level.LevelName='"+ dictionaryLevel +"' and termText." + Constants.Property.EnglishText + "='" + term + "' return distinct baseTerm";
        stringSearch(searchString);                   
    }
    
    
    
    
    
    public void stringSearch(String query) {
       
        System.out.println("Query: " + query);
        ExecutionEngine engine = new ExecutionEngine( m_Db );
        ExecutionResult result = engine.execute(query);   
        System.out.println(result);
    }
    
    public void testCypherSearch() {
         
        String rootNode="start n=node(0) return n";
        stringSearch(rootNode);
        
        String test1 = "start n=node(0) return n, n." + Constants.Property.DictionaryName;
        stringSearch(test1);
        
        String rootNodeChildren="start root=node(0) match root -[:DictionaryLevel]->level return level";       
        stringSearch(rootNodeChildren);
        
        String rootNodeChildrenWithOrdering="start root=node(0) match root -[:DictionaryLevel]->levelordered return levelordered order by levelordered.LevelName asc";
        stringSearch(rootNodeChildrenWithOrdering);
            
    }
    
    public void outputResults(FileType type) {
               
        System.out.println("~~Results for data load~~");
        
        switch (type)
        {
            case Terms:
                System.out.println("Term English text nodes created: " + m_NumAddedEnglishNodes);
                System.out.println("Code nodes created: " + m_NumAddedCodeNodes);
                break;

            case TermRels:
                System.out.println("Dict-Code Relationships created: " + m_NumDictCodeRels);
                System.out.println("Code->Code Relationships created: " + m_NumCodeCodeRels);
                System.out.println("Same Code Relationships/Child Nodes created: " + m_NumSameCodeRels);
                
                break;

            case Components:
                System.out.println("Component English text nodes created: " + m_NumAddedEnglishNodes);
                break;

            case TermComponentRels:
                System.out.println("TermComponent relationships created: " + m_NumAddedTermComponentRelationships);
                break;
        }            
    }
    
    /*
     * Manually create whodrug dictionary structure for now.
    */
    private void createDictionaryBase() {
        
        m_RootNode.setProperty(Constants.Node.Type, Constants.Node.Dictionary);
        m_RootNode.setProperty(Constants.Property.DictionaryName, "WhoDrugC");
        
    }
    
    private static void registerShutdownHook(final GraphDatabaseService graphDb)
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    
    private void importTermRelationships(File file) throws IOException {
         
        BufferedReader bf = new BufferedReader(new FileReader(file));
        try 
        {
            final Importer.Data data = new Importer.Data(bf.readLine(), s_Delimiter, 0);
            String line, childCode, parentCode, level;
            Map<String,Object> map;
            m_Report.reset();
            int counter = 0;
            while ((line = bf.readLine()) != null) {

                counter++;

                // skip blank lines
                if (line.trim().length() == 0) {
                    continue;
                } 

                map = map(data.update(line));

                childCode = map.get("ChildCode").toString();
                parentCode = map.get("ParentCode").toString();
                level = map.get("Level").toString();

                addTermRelationship(childCode, parentCode, level);

                ensureRegularCommit(counter);

                m_Report.dots();
            }
        }
        finally {
            if (bf!=null) bf.close();
        }
        m_Report.finishImport("TermRelationships");
    }
    
    private void importTermDataNodes(File file) throws IOException {
         
        BufferedReader bf = new BufferedReader(new FileReader(file));
        try 
        {
            final Importer.Data data = new Importer.Data(bf.readLine(), s_Delimiter, 0);
            String line, term, code;
            Map<String,Object> map;
            m_Report.reset();
            int counter = 0;
            while ((line = bf.readLine()) != null) {

                counter++;

                // skip blank lines
                if (line.trim().length() == 0) {
                    continue;
                } 

                map = map(data.update(line));

                term = map.get("Term").toString();
                code = map.get("Code").toString();

                addTermNode(term, code);

                ensureRegularCommit(counter);

                m_Report.dots();
            }
        }
        finally {
            if (bf!=null) bf.close();
        }
        m_Report.finishImport("TermNodes");
    }

    /*
     * Adds Component node, creating english component text node if needed.
     * 
     * Level is assumed to exist.
     */
    private void addComponentNode(String componentName)
    {
        if (componentName==null || componentName.trim().length()==0) {    
            throw new IllegalArgumentException("name must be provided!");
        }
        
       // get english node or create if it doesn't exist
       retrieveCreateEnglishNode(componentName); 
    }

            
    private Node retrieveCreateEnglishNode(String text) {
        
       Node node = getEnglishNodeByText(text);
       if (node == null) {
           node = addEnglishNode(text);
       }     
       return node;
    }
    
    /*
     * Adds Term node, creating english text and code nodes if needed.
     * 
     * Level is assumed to exist.
     */
    private void addTermNode(String term, String code) throws InvalidActivityException
    {
        if (term==null || term.trim().length()==0) {    
            throw new IllegalArgumentException("term must be provided!");
        }
        if (code==null || code.trim().length()==0) {    
            throw new IllegalArgumentException("code must be provided!");
        }
        
       // get english and code nodes or create if they don't exist
       Node englishNode = retrieveCreateEnglishNode(term);
       
       Node codeNode = getCodeNodeByCode(code);
       if (codeNode == null) {
           codeNode = addCodeNode(code);
       }
       
       codeNode.createRelationshipTo(englishNode,DictRelType.TermEnglish);      
    }
    
    private Node addEnglishNode(String value) {
        
        Node node = m_Db.createNode();
        node.setProperty(Constants.Node.Type, Constants.Node.English);
        node.setProperty(Constants.Property.EnglishText, value);
        
        m_EnglishTextIndex.add(node, Constants.Property.EnglishText, value);
        m_NumAddedEnglishNodes++;
         
        return node;
    }
    
    private Node addCodeNode(String code) {
        
        return addCodeNode(code, true);
    }
    
    private Node addCodeNode(String code, boolean addToIndex) {
        
        Node codeNode = m_Db.createNode();
        codeNode.setProperty(Constants.Node.Type, Constants.Node.Base);
        codeNode.setProperty(Constants.Property.Code, code);
        
        if (addToIndex) m_CodeIndex.add(codeNode, Constants.Property.CodeNodeKey, code);
        
        m_NumAddedCodeNodes++;
        
        return codeNode;       
    }
    
    private Node getEnglishNodeByText(String text) {
     
        return m_EnglishTextIndex.get(Constants.Property.EnglishText, text).getSingle();   
    }
    
    /***
     * Get a code node by code.
     * @param code
     * @return Node or null if not found
     * Throw exception if more than one node is returned
     */
    private Node getCodeNodeByCode(String code) {
        
        return m_CodeIndex.get(Constants.Property.CodeNodeKey, code).getSingle();
    }
    
    /*
    private Node getCodeNodeByCodeAndLevel(String code, String level) {
        
        IndexHits<Node> hits = m_CodeIndex.get(Constants.Property.CodeNodeKey, code);
        try
        {
            for ( Node node : hits )
            {
                if (doesNodeHaveLevelRelationship(node, level)) return node;
            }
            
            return null;
        }
        finally
        {
            hits.close();
        }
    
    }
    */
    
    private boolean doesNodeHaveLevelRelationship(Node node, String level) {
        
        Traverser traverser = node.traverse(Order.BREADTH_FIRST,
                StopEvaluator.DEPTH_ONE,
                new ReturnableEvaluator()
                {
                    @Override
                    public boolean isReturnableNode(
                            final TraversalPosition currentPos )
                    {
                        return !currentPos.isStartNode()
                        && currentPos.lastRelationshipTraversed()
                        .isType(DictRelType.LevelCode);
                    }
                },
                DictRelType.LevelCode,
                Direction.BOTH );
        for ( Node travNode : traverser )
        {
            Object prop = travNode.getProperty(Constants.Property.LevelName, null);
            if (prop != null && level.equals(prop)) {
                return true;
            }
        }
/*        
        for (Relationship rel : node.getRelationships()) {
            String relType = rel.getType().toString();
            
            Node endNode = rel.getEndNode();
            String endNodeType = node.getProperty("Type").toString();
        }
*/
        return false;
    }
   
    private void finish() {
        
        if (m_Tran != null) m_Tran.finish(); 
        m_Tran=null;
        
        m_Db.shutdown();
        m_Db = null;
        
        deleteTranLogFiles();      
    }
    
    private void deleteTranLogFiles() {
        File file = new File (m_DbLocation + "\\tm_tx_log.1");
        if (file.exists()) {
            file.delete();
        }
    }
        
    public static void deleteFileOrDirectory(final File file)
    {
        if (!file.exists())
        {
            return;
        }

        if (file.isDirectory())
        {
            for (File child : file.listFiles())
            {
                deleteFileOrDirectory(child);
            }
        }
        else
        {
            file.delete();
        }
    }
      
    // report class from batch implemntor used for import time reporting
    static class Report {
        private final long batch;
        private final long dots;
        private long count;
        private long time, batchTime, totalTime;

        public Report(long batch, int dots) {
            this.batch = batch;
            this.dots = batch / dots;
        }

        public void reset() {
            count = 0;
            totalTime = 0;
            batchTime = time = System.currentTimeMillis();
        }

        public void dots() {
            if ((++count % dots) != 0) return;
            System.out.print(".");
            if ((count % batch) != 0) return;
            
            long now = System.currentTimeMillis();
            long timeTaken = now - batchTime;
            System.out.println(timeTaken + " ms for " + batch);     
            if ((count % 1000000)==0) {
                System.out.println(count + " items have taken " + totalTime + " ms." );
            }
            batchTime = now;
            totalTime+=timeTaken;
        }

        public void finishImport(String type) {

            System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
            System.out.println("Thousands of total " + type + " created/second: " + count/(double)totalTime);                        
        }
    }

    static class Type implements RelationshipType {
        String name;

        public Importer.Type update(Object value) {
            this.name = value.toString();
            return this;
        }

        public String name() {
            return name;
        }
    }

    private long id(Object id) {
        return Long.parseLong(id.toString());
    }
    
    static class Data 
    {
        private final Object[] data;
        private final int offset;
        private final String delim;
        private String[] fields;
        private String[] values;

        public Data(String header, String delim, int offset) {
            this.offset = offset;
            this.delim = delim;
            
            // strip out BOM char.
            header = header.replace("\ufeff","");
            
            this.fields = header.split(delim);
            data = new Object[(fields.length - offset) * 2];
            for (int i = 0; i < fields.length - offset; i++) {
                data[i * 2] = fields[i + offset];
            }
        }

        public Object[] update(String line, Object... header) {
            values = line.split(delim);
            if (header.length > 0) {
                System.arraycopy(values, 0, header, 0, header.length);
            }
            for (int i = 0; i < values.length - offset; i++) {
                data[i * 2 + 1] = values[i + offset];
            }
            return data;
        }
    }
}