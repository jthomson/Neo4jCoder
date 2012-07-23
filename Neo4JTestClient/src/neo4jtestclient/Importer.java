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
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Importer {
    private static Importer.Report m_Report;
    private EmbeddedGraphDatabase m_Db;
    private Node m_RootNode; 
    private Transaction m_Tran;
    private boolean m_Init;
    private String m_DbLocation;
    
    private Index<Node> m_EnglishTextIndex;
    private int m_NumAddedEnglishNodes;
    
    private Index<Node> m_CodeIndex;
    private int m_NumAddedCodeNodes;
    
    private int m_NumAddedTermComponentRelationships;
    
    private Index<Node> m_LevelIndex;

    private static String s_Delimiter = ";";
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
        m_NumAddedEnglishNodes = 0;
        
        m_CodeIndex = m_Db.index().forNodes("Code", EXACT_CONFIG);
        m_NumAddedCodeNodes = 0;
        
        m_NumAddedTermComponentRelationships = 0;
        
        m_LevelIndex = m_Db.index().forNodes("Level", EXACT_CONFIG);
//        m_ComponentLevelIndex = m_Db.index().forNodes("ComponentLevel", EXACT_CONFIG);
        
       // m_Components = new HashMap();
        //m_Levels = new HashMap();
        
        if (startNew) {
            File file = new File(m_DbLocation);
            deleteFileOrDirectory(file);
            if (!file.exists()) file.mkdirs(); 
            
            setupDictionaryBase();
        }
         
        
        m_Report = new Importer.Report(100000, 100);       
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
        createDictionaryAndLevels();

        m_Tran.success();
        m_Tran.finish();
        m_Init = false;
        
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
              
                case Components:
                    importComponentDataNodes(file);
                    break;
             
                case TermComponents:
                    importTermComponentRelationships(file);
                    break;
            }

            m_Tran.success();
            outputResults();
            
        } finally {
            finish();
        }
    }
    
    private void importTermComponentRelationships(File file) throws IOException {

        BufferedReader bf = new BufferedReader(new FileReader(file));
        try 
        {
            final Importer.Data data = new Importer.Data(bf.readLine(), s_Delimiter, 0);
            String line, component, term, componentType;
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
                term = map.get("Code").toString();

                addTermComponentRelationship(componentType, component, term);

                ensureRegularCommit(counter);

                m_Report.dots();
            }
        }
        finally
        {
            if (bf != null) bf.close();
        }
        m_Report.finishImport("TermComponentRelationships");   
    }
    
    private void addTermComponentRelationship(String componentType, String component, String term) {
        
        Node componentNode = getEnglishNodeByText(term)
        
        Node termNode = getEnglishNodeByText(term);
        
        if (termNode == null) {
            String a = "b";
        }
        
        
    }
    
    private void importComponentDataNodes(File file) throws IOException 
    {     
        BufferedReader bf = new BufferedReader(new FileReader(file));
        try 
        {
            final Importer.Data data = new Importer.Data(bf.readLine(), s_Delimiter, 0);
            String line, level, type, value;
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

                level = map.get("Level").toString();
                type = map.get("Type").toString();
                value = map.get("Value").toString();

                addComponentNode(level, type, value);

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

        englishTermLuceneSearch("ANTIHYPERTENSIVES");
        
        String twoWildcard = "start terms=node:EnglishText(\"EnglishText:A* AND EnglishText:D*\") return terms";
        stringSearch(twoWildcard);
        
        //String termsAtLevel = "start terms=node:EnglishText(\"EnglishText:A*\") match terms-[:ComponentEnglish]-a where a.ComponentName='DRUGRECORDNUMBER' return a";
        //stringSearch(termsAtLevel);   
        
        String allTerms = "start terms=node:EnglishText(\"EnglishText:*\") return terms";
        stringSearch(allTerms);
        
    }
    
    private void englishTermLuceneSearch(String queryString) {
        
        String searchString = "start terms=node:EnglishText(\"" + Constants.Property.EnglishText + ":" + queryString + "\") return terms";
        stringSearch(searchString);     
    }
            
    private void stringSearch(String query) {
        
        ExecutionEngine engine = new ExecutionEngine( m_Db );
        ExecutionResult result = engine.execute(query);   
        System.out.println("Query: " + query);
        System.out.println(result);
    }
    
    private void testCypherSearch() {
         
        String rootNode="start n=node(0) return n";
        stringSearch(rootNode);
        
        String test1 = "start n=node(0) return n, n." + Constants.Property.DictionaryName;
        stringSearch(test1);
        
        String rootNodeChildren="start root=node(0) match root -[:DictionaryLevel]->level return level";       
        stringSearch(rootNodeChildren);
        
        String rootNodeChildrenWithOrdering="start root=node(0) match root -[:DictionaryLevel]->levelordered return levelordered order by levelordered.LevelName asc";
        stringSearch(rootNodeChildrenWithOrdering);

        String drugRecordNumberComponentLevel = "start components=node:ComponentLevel(" + Constants.Node.BaseComponent + "='DRUGRECORDNUMBER') return components";
        stringSearch(drugRecordNumberComponentLevel);

        String drugRecordNumberComponents = "start components=node:ComponentLevel(" + Constants.Node.BaseComponent + "='DRUGRECORDNUMBER') match components-[:ComponentEnglish]->englishnodes return englishnodes";
        stringSearch(drugRecordNumberComponents);
        
        
    }
    
    public void outputResults() {
               
        System.out.println("~~Results for data load:");
        System.out.println("English text nodes created: " + m_NumAddedEnglishNodes);
        System.out.println("Base nodes created: " + m_NumAddedCodeNodes);
    }
    
    /*
     * Manually create whodrug dictionary structure for now.
    */
    private void createDictionaryAndLevels() {
        
        m_RootNode.setProperty(Constants.Node.Type, Constants.Node.Dictionary);
        m_RootNode.setProperty(Constants.Property.DictionaryName, "WhoDrugC");
        
        addLevelNodeAndRelationship(Constants.DictionaryLevel.ATC1);
        addLevelNodeAndRelationship(Constants.DictionaryLevel.ATC2);
        addLevelNodeAndRelationship(Constants.DictionaryLevel.ATC3);
        addLevelNodeAndRelationship(Constants.DictionaryLevel.ATC4);
        addLevelNodeAndRelationship(Constants.DictionaryLevel.Ingredient);
        addLevelNodeAndRelationship(Constants.DictionaryLevel.MP);
        
    }
   
/*
    private void createComponentTypeNodes() {
        
        addComponentTypeNode("DRUGRECORDNUMBER", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("SEQUENCENUMBER1", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("SEQUENCENUMBER2", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("SEQUENCENUMBER3", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("SEQUENCENUMBER4", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("GENERIC", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("NAMESPECIFIER", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("MARKETINGAUTHORIZATIONNUMBER", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("MARKETINGAUTHORIZATIONDATE", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("MARKETINGAUTHORIZATIONWITHDRAWALDATE", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("COUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("COMPANY", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("COMPANYCOUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("MARKETINGAUTHORIZATIONHOLDER", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("MARKETINGAUTHORIZATIONHOLDERCOUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("SOURCEYEAR", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("SOURCE", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("SOURCECOUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("PRODUCTTYPE", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("PRODUCTGROUP", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("PRODUCTGROUPDATERECORDED", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("CREATEDATE", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("DATECHANGED", Constants.DictionaryLevel.Ingredient);
        addComponentTypeNode("INGREDIENTCREATEDATE", Constants.DictionaryLevel.MP);
        addComponentTypeNode("QUANTITY", Constants.DictionaryLevel.MP);
        addComponentTypeNode("QUANTITY2", Constants.DictionaryLevel.MP);
        addComponentTypeNode("UNIT", Constants.DictionaryLevel.MP);
        addComponentTypeNode("PHARMACEUTICALFORM", Constants.DictionaryLevel.MP);
        addComponentTypeNode("ROUTEOFADMINISTRATION", Constants.DictionaryLevel.MP);
        addComponentTypeNode("NUMBEROFINGREDIENTS", Constants.DictionaryLevel.MP);
        addComponentTypeNode("PHARMACEUTICALFORMCREATEDATE", Constants.DictionaryLevel.MP);
        addComponentTypeNode("SUBSTANCE", Constants.DictionaryLevel.MP);
        addComponentTypeNode("CASNUMBER", Constants.DictionaryLevel.MP);
        addComponentTypeNode("LANGUAGECODE", Constants.DictionaryLevel.MP);
    }
    
    private void addComponentTypeNode(String name, String level) {
        
        Node componentTypeNode = m_Db.createNode();
        componentTypeNode.setProperty(Constants.Node.Type, Constants.Node.BaseComponent);
        componentTypeNode.setProperty(Constants.Property.ComponentName, name);
        
        Node levelNode = getLevelNode(level);
        levelNode.createRelationshipTo(componentTypeNode, DictRelType.LevelComponent);
        
        m_ComponentLevelIndex.add(componentTypeNode, Constants.Node.BaseComponent, name);       
    }
  
    private Node getComponentLevelNode(String component) {
    
        return m_ComponentLevelIndex.get(Constants.Node.BaseComponent, component).getSingle();
    }
*/    
    private Node getLevelNode(String level) {
        
        return m_LevelIndex.get(Constants.Node.Level, level).getSingle();     
    }
    
    private Node addLevelNodeAndRelationship(String levelName) {
        
        Node levelNode = m_Db.createNode();
        levelNode.setProperty(Constants.Node.Type, Constants.Node.Level);
        levelNode.setProperty(Constants.Property.LevelName, levelName);
        
        m_RootNode.createRelationshipTo(levelNode, DictRelType.DictionaryLevel);
        m_LevelIndex.add(levelNode, Constants.Node.Level, levelName);
        
        return levelNode;
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
    
    /* 
    * 
    */
    private void importTermDataNodes(File file) throws IOException {
         
        BufferedReader bf = new BufferedReader(new FileReader(file));
        try 
        {
            final Importer.Data data = new Importer.Data(bf.readLine(), s_Delimiter, 0);
            String line, level, term, code;
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

                level = map.get("Level").toString();
                term = map.get("Term").toString();
                code = map.get("Code").toString();

                addTermNode(level, term, code);

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
    private void addComponentNode(String level, String componentType, String name)
    {
        if (level==null || level.trim().length()==0) {    
            throw new IllegalArgumentException("level must be provided!");
        }
        if (componentType==null || componentType.trim().length()==0) {    
            throw new IllegalArgumentException("componentType must be provided!");
        }
        if (name==null || name.trim().length()==0) {    
            throw new IllegalArgumentException("name must be provided!");
        }
        
       // get english node or create if it doesn't exist
       Node englishNode = getEnglishNodeByText(name);
       if (englishNode == null) {
           englishNode = addEnglishNode(name);
       }
 
/*       
       Node componentLevelNode = getComponentLevelNode(componentType);
       componentLevelNode.createRelationshipTo(englishNode,DictRelType.ComponentEnglish);
*/     
    }
    
    /*
     * Adds Term node, creating english text and code nodes if needed.
     * 
     * Level is assumed to exist.
     */
    private void addTermNode(String level, String term, String code)
    {
        if (level==null || level.trim().length()==0) {    
            throw new IllegalArgumentException("level must be provided!");
        }
        if (term==null || term.trim().length()==0) {    
            throw new IllegalArgumentException("term must be provided!");
        }
        if (code==null || code.trim().length()==0) {    
            throw new IllegalArgumentException("code must be provided!");
        }
        
       // get english and code nodes or create if they don't exist
       Node englishNode = getEnglishNodeByText(term);
       if (englishNode == null) {
           englishNode = addEnglishNode(term);
       }
       
       Node codeNode = getCodeNodeByCodeAndLevel(code, level);
       if (codeNode == null) {
           codeNode = addCodeNode(code);
       }
       
       Node levelNode = getLevelNode(level);
       if (levelNode == null) {
           int a = 4;
       }
       levelNode.createRelationshipTo(codeNode,DictRelType.LevelCode);
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
        
        Node codeNode = m_Db.createNode();
        codeNode.setProperty(Constants.Node.Type, Constants.Node.Base);
        codeNode.setProperty(Constants.Property.Code, code);
        
        m_CodeIndex.add(codeNode, Constants.Property.CodeNodeKey, code);
        m_NumAddedCodeNodes++;
        
        return codeNode;      
    }
    
    private Node getEnglishNodeByText(String text) {
     
        return m_EnglishTextIndex.get(Constants.Property.EnglishText, text).getSingle();   
    }
    
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
    
    private boolean doesNodeHaveLevelRelationship(Node node, String level) {
        
        Traverser traverser = node.traverse(Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH,
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
                Direction.OUTGOING );
            for ( Node travNode : traverser )
            {
                if (travNode.getProperty(Constants.Property.LevelName) == level)
                    return true;
            }
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
        File file = new File ("D:\\neoDB\\tm_tx_log.1");
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
        private long time, batchTime;

        public Report(long batch, int dots) {
            this.batch = batch;
            this.dots = batch / dots;
        }

        public void reset() {
            count = 0;
            batchTime = time = System.currentTimeMillis();
        }

        public void dots() {
            if ((++count % dots) != 0) return;
            System.out.print(".");
            if ((count % batch) != 0) return;
            long now = System.currentTimeMillis();
            System.out.println((now - batchTime) + " ms for "+batch);
            batchTime = now;
        }

        public void finishImport(String type) {
            
            System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
            System.out.println("Thousands of total " + type + " created/second: " + count*1000/(double)time);
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