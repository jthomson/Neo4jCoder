package neo4jtestclient;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.batchinsert.*;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.lang.Exception;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class Importer {
    private static Importer.Report m_Report;
    private BatchInserterImpl m_Db;
    private BatchInserterIndexProvider m_Provider;
    private BatchInserterIndex m_NodeIndex;
    private BatchInserterIndex m_RelationshipIndex;

    private static String m_Delimiter = ";";

    public Importer(File graphDb) {
        final Map<String, String> config = getConfig();
        m_Db = new BatchInserterImpl(graphDb.getAbsolutePath(), config);
        m_Report = new Importer.Report(100000, 100);
        m_Provider = new LuceneBatchInserterIndexProvider(m_Db);
        m_NodeIndex = m_Provider.nodeIndex("terms", org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG);
        m_RelationshipIndex = m_Provider.relationshipIndex("terms", stringMap("provider", "lucene", "type", "exact"));     
    }

    private Map<String, String> getConfig() {
        return stringMap(
            "dump_configuration", "true",
            "cache_type", "none",
            "neostore.propertystore.db.index.keys.mapped_memory", "5M",
            "neostore.propertystore.db.index.mapped_memory", "5M",
            "neostore.nodestore.db.mapped_memory", "50M",
            "neostore.relationshipstore.db.mapped_memory", "250M",
            "neostore.propertystore.db.mapped_memory", "200M",
            "neostore.propertystore.db.strings.mapped_memory", "100M");
    }

    public static void RunImport(String dbLocation, String nodeFileLocation, String relFileLocation) throws IOException
    {
        if (nodeFileLocation==null || nodeFileLocation.trim().length()==0)
        {    
            throw new IllegalArgumentException("nodeFileLocation must be provided!");
        }
        if (relFileLocation==null || relFileLocation.trim().length()==0)
        {    
            throw new IllegalArgumentException("relFileLocation must be provided!");
        }
        
        File nodes = new File(nodeFileLocation);
        File rels = new File(relFileLocation);
        
        RunImport(dbLocation, nodes, rels);  
    }
    
    public static void RunImport(String dbLocation, File nodeFile, File relationshipFile) throws IOException
    {
        if (dbLocation==null || dbLocation.trim().length()==0) {    
            throw new IllegalArgumentException("dbLocation must be provided!");
        }
        if (nodeFile == null) {
            throw new IllegalArgumentException("nodeFile must be provided!");
        }
        if (relationshipFile == null) {
            throw new IllegalArgumentException("relationshipFile must be provided!");
        }
        
        File graphDb = new File(dbLocation);
        deleteFileOrDirectory(graphDb);
        if (!graphDb.exists()) graphDb.mkdirs();
        Importer importBatch = new Importer(graphDb);
        try {
            if (nodeFile.exists()) importBatch.importNodes(nodeFile);
            if (relationshipFile.exists()) importBatch.importRelationships(relationshipFile);
        } finally {
            importBatch.finish();
        }    
    }

    /* IDs used are provided in input file
    * 
    */
    private void importNodes(File file) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(file));
        final Importer.Data data = new Importer.Data(bf.readLine(), m_Delimiter, 0);
        String line;
        long id;
        Map<String,Object> map;
        m_Report.reset();
        while ((line = bf.readLine()) != null) {
            map = map(data.update(line));
            id =  Long.parseLong(map.get("ID").toString());
            m_Db.createNode(map);
            m_NodeIndex.add(id, map);
           
            m_Report.dots();
        }
        m_Report.finishImport("Nodes");
    }

    private void importRelationships(File file) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(file));
        final Importer.Data data = new Importer.Data(bf.readLine(), m_Delimiter, 0);
        Object[] rel = new Object[3];
        final Importer.Type type = new Importer.Type();
        String line;
        long relationshipId;

        m_Report.reset();
        while ((line = bf.readLine()) != null) {
            final Map<String, Object> properties = map(data.update(line, rel));
            relationshipId = m_Db.createRelationship(id(rel[0]), id(rel[1]), type.update(rel[2]), properties);
            m_RelationshipIndex.add(relationshipId, properties);
            m_Report.dots();
        }
        m_Report.finishImport("Relationships");
    }
    
    static class Data {
        private final Object[] data;
        private final int offset;
        private final String delim;

        public Data(String header, String delim, int offset) {
            this.offset = offset;
            this.delim = delim;
            String[] fields = header.split(delim);
            data = new Object[(fields.length - offset) * 2];
            for (int i = 0; i < fields.length - offset; i++) {
                data[i * 2] = fields[i + offset];
            }
        }

        public Object[] update(String line, Object... header) {
            final String[] values = line.split(delim);
            if (header.length > 0) {
                System.arraycopy(values, 0, header, 0, header.length);
            }
            for (int i = 0; i < values.length - offset; i++) {
                data[i * 2 + 1] = values[i + offset];
            }
            return data;
        }

    }

    private void finish() {
        m_Db.shutdown();
        m_Report.finish();
    }
        
    private static void deleteFileOrDirectory(final File file)
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
        
    static class Report {
        private final long batch;
        private final long dots;
        private long count;
        private long total = System.currentTimeMillis(), time, batchTime;

        public Report(long batch, int dots) {
            this.batch = batch;
            this.dots = batch / dots;
        }

        public void reset() {
            count = 0;
            batchTime = time = System.currentTimeMillis();
        }

        public void finish() {
            System.out.println((System.currentTimeMillis() - total) / 1000 + " seconds ");
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
}