package neo4jtestclient;

/**
 *
 * @author JThomson
 */

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.batchinsert.*;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;

import java.util.Map;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

import java.lang.Exception;
import java.math.BigDecimal;
import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Importer {
    private static Importer.Report m_Report;
    private EmbeddedGraphDatabase m_Db;
    private Node m_RootNode; 
    private Transaction m_Tran;
    private boolean m_Init;
    private String m_DbLocation;
    
    //private HashMap m_Components;
    //private HashMap m_Levels;
    
    private Index<Node> m_EnglishTextIndex;
    private int m_NumAddedEnglishNodes;
    
    private Index<Node> m_CodeIndex;
    private int m_NumAddedCodeNodes;
    
    private Index<Node> m_LevelIndex;
    private Index<Node> m_ComponentIndex;

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
        
        m_LevelIndex = m_Db.index().forNodes("Level", EXACT_CONFIG);
        m_ComponentIndex = m_Db.index().forNodes("Component", EXACT_CONFIG);
        
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

    public void runImport() throws IOException
    {
        try {      
            
                      
            
            outputResults();
            
        } finally {
            finish();
        }
     
    }
    
    private void setupDictionaryBase() {
        
        m_Tran = m_Db.beginTx();
        createDictionaryAndLevels();
        createComponentLevelNodes();
        m_Tran.success();
        m_Tran.finish();
        m_Init = false;
        
    }
   
    public void importComponentFile(String nodeFileLocation) throws IOException
    {   
        try {   
            File nodeFile = new File(nodeFileLocation);

            if (nodeFile == null || nodeFile.length() == 0) {
                throw new IllegalArgumentException("nodeFile must be provided!");
            }

            m_Tran = m_Db.beginTx();

            importComponentDataNodes(nodeFile);

            m_Tran.success();
            outputResults();
            
        } finally {
            finish();
        }

    }
    
    private void importComponentDataNodes(File file) throws IOException 
    {     
        BufferedReader bf = new BufferedReader(new FileReader(file));
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

            // save memory.
            if (counter % m_CommitSize == 0) {
                m_Tran.success();
                m_Tran.finish();

                m_Tran = null;
                m_Tran = m_Db.beginTx();
            }

            m_Report.dots();
        }
        m_Report.finishImport("Nodes");
    }
    
    public void importTermFile(String nodeFileLocation) throws IOException
    {   
        try
        {
            File nodeFile = new File(nodeFileLocation);

            if (nodeFile == null || nodeFile.length() == 0) {
                throw new IllegalArgumentException("nodeFile must be provided!");
            }

            m_Tran = m_Db.beginTx();
            importTermDataNodes(nodeFile);
            m_Tran.success();
            
        } finally {
            finish();
        }

    }

    private void createRandomTermComponentRelationshipByLevel(String level) {
        
    }
    
    public void outputResults() {
        
        long totalNodeCount = m_Db.getConfig().getGraphDbModule().getNodeManager().getNumberOfIdsInUse(Node.class);
              
        System.out.println("~~Results for data load:");
        System.out.println("English text nodes created: " + m_NumAddedEnglishNodes);
        System.out.println("Base nodes created: " + m_NumAddedCodeNodes);
        System.out.println("Total Node count: " + totalNodeCount);
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
    
    private void createComponentLevelNodes() {
        
        addComponentLevelNode("DRUGRECORDNUMBER", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("SEQUENCENUMBER1", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("SEQUENCENUMBER2", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("SEQUENCENUMBER3", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("SEQUENCENUMBER4", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("GENERIC", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("NAMESPECIFIER", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("MARKETINGAUTHORIZATIONNUMBER", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("MARKETINGAUTHORIZATIONDATE", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("MARKETINGAUTHORIZATIONWITHDRAWALDATE", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("COUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("COMPANY", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("COMPANYCOUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("MARKETINGAUTHORIZATIONHOLDER", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("MARKETINGAUTHORIZATIONHOLDERCOUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("SOURCEYEAR", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("SOURCE", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("SOURCECOUNTRY", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("PRODUCTTYPE", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("PRODUCTGROUP", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("PRODUCTGROUPDATERECORDED", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("CREATEDATE", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("DATECHANGED", Constants.DictionaryLevel.Ingredient);
        addComponentLevelNode("INGREDIENTCREATEDATE", Constants.DictionaryLevel.MP);
        addComponentLevelNode("QUANTITY", Constants.DictionaryLevel.MP);
        addComponentLevelNode("QUANTITY2", Constants.DictionaryLevel.MP);
        addComponentLevelNode("UNIT", Constants.DictionaryLevel.MP);
        addComponentLevelNode("PHARMACEUTICALFORM", Constants.DictionaryLevel.MP);
        addComponentLevelNode("ROUTEOFADMINISTRATION", Constants.DictionaryLevel.MP);
        addComponentLevelNode("NUMBEROFINGREDIENTS", Constants.DictionaryLevel.MP);
        addComponentLevelNode("PHARMACEUTICALFORMCREATEDATE", Constants.DictionaryLevel.MP);
        addComponentLevelNode("SUBSTANCE", Constants.DictionaryLevel.MP);
        addComponentLevelNode("CASNUMBER", Constants.DictionaryLevel.MP);
        addComponentLevelNode("LANGUAGECODE", Constants.DictionaryLevel.MP);
    }
    
    private void addComponentLevelNode(String name, String level) {
        
        Node componentLevelNode = m_Db.createNode();
        componentLevelNode.setProperty(Constants.Node.Type, Constants.Node.BaseComponent);
        componentLevelNode.setProperty(Constants.Property.ComponentName, name);
        
        Node levelNode = getLevelNode(level);
        levelNode.createRelationshipTo(componentLevelNode, DictRelTypes.LevelComponent);
        
        m_ComponentIndex.add(componentLevelNode, Constants.Node.BaseComponent, name);
        //m_Components.put(name, componentLevelNode);
        
    }
    
    private Node getComponentLevelNode(String component) {
    
        return m_ComponentIndex.get(Constants.Node.BaseComponent, component).getSingle();
        
        //return (Node)m_Components.get(component);
    }
    
    private Node getLevelNode(String level) {
        
        return m_LevelIndex.get(Constants.Node.Level, level).getSingle();
        
        //return (Node)m_Levels.get(level);
      
    }
    private Node addLevelNodeAndRelationship(String levelName) {
        
        Node levelNode = m_Db.createNode();
        levelNode.setProperty(Constants.Node.Type, Constants.Node.Level);
        levelNode.setProperty(Constants.Property.LevelName, levelName);
        
        m_RootNode.createRelationshipTo(levelNode, DictRelTypes.DictionaryLevel);
        
        //m_Levels.put(levelName, levelNode);
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
            
            // save memory.
            if (counter % m_CommitSize == 0) {
                m_Tran.success();
                m_Tran.finish();
                
                m_Tran = null;
                m_Tran = m_Db.beginTx();
            }
           
            m_Report.dots();
        }
        m_Report.finishImport("Nodes");
    }

    // TODO: what about level-component type relationships?
    // creating them for now...
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
       
       Node componentLevelNode = getComponentLevelNode(componentType);
       componentLevelNode.createRelationshipTo(englishNode,DictRelTypes.ComponentEnglish);
     
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
           codeNode = addCodeNode(code, level);
       }
       
       Node levelNode = getLevelNode(level);
       if (levelNode == null) {
           int a = 4;
       }
       levelNode.createRelationshipTo(codeNode,DictRelTypes.LevelCode);
       codeNode.createRelationshipTo(englishNode,DictRelTypes.TermEnglish);
        
    }
    
    private Node addEnglishNode(String term) {
        
        Node termNode = m_Db.createNode();
        termNode.setProperty(Constants.Node.Type, Constants.Node.English);
        termNode.setProperty(Constants.Property.EnglishText, term);
        
        m_EnglishTextIndex.add(termNode, Constants.Property.EnglishText, term);
        m_NumAddedEnglishNodes++;
         
        return termNode;
    }
    
    private Node addCodeNode(String code, String level) {
        
        Node codeNode = m_Db.createNode();
        codeNode.setProperty(Constants.Node.Type, Constants.Node.Base);
        codeNode.setProperty(Constants.Property.Code, code);
        codeNode.setProperty(Constants.Property.LevelName, level);
        
        String key = getCodeNodeKey(code, level);
        m_CodeIndex.add(codeNode, Constants.Property.CodeNodeKey, key);
        m_NumAddedCodeNodes++;
        
        return codeNode;      
    }
    
    private String getCodeNodeKey(String code, String level) {
        return String.format("%s%s%s", code, s_Delimiter, level);
    }
    
    private Node getEnglishNodeByText(String text) {
     
        return m_EnglishTextIndex.get(Constants.Property.EnglishText, text).getSingle();   
    }
    
    private Node getCodeNodeByCodeAndLevel(String code, String level) {
        
        return m_CodeIndex.get(Constants.Property.CodeNodeKey, getCodeNodeKey(code, level)).getSingle(); 
    }
   
    private void finish() {
        
        if (m_Tran != null) m_Tran.finish();       
        m_Db.shutdown();
        
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
      
    enum DictRelTypes implements RelationshipType
    {
        DictionaryLevel,
        LevelCode,
        TermEnglish,
        ChildTerm,
        TermComponent,
        ComponentEnglish,
        LevelComponent
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
            
            // strip out BOM char
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