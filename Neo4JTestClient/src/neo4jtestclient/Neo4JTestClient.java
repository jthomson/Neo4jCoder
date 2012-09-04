/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4jtestclient;

import java.io.IOException;


public class Neo4JTestClient {


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        
        performNewImport();
    }   
    
    public static void performNewImport() throws IOException {
    
        String db = "/refinedDB";
//        Importer componentImporter = new Importer(db, true);
//        componentImporter.importFile(FileType.Components, "C:\\Users\\jthomson\\Desktop\\CoderData\\Components.csv");
//        
//        Importer termImporter = new Importer(db, false);
//        termImporter.importFile(FileType.Terms, "C:\\Users\\jthomson\\Desktop\\CoderData\\Terms.csv");
//        
        Importer termRelImporter = new Importer(db, false);
        termRelImporter.importFile(FileType.TermRels,"C:\\Users\\jthomson\\Desktop\\CoderData\\TermRels.csv");
        
        Importer termCompRelImporter = new Importer(db, false);
        termCompRelImporter.importFile(FileType.TermComponentRels,"C:\\Users\\jthomson\\Desktop\\CoderData\\TermComponentRels.csv");
    }
    
    public static void PerformOldImport() {
     /*                    
        Importer componentImporter = new Importer("/neoDB", true);
        componentImporter.importFile(FileType.Components, "C:\\Users\\jthomson\\Desktop\\Components.csv");
        
        Importer termImporter = new Importer("/neoDB", false);
        termImporter.importFile(FileType.Terms, "C:\\Users\\jthomson\\Desktop\\FirstVersion.csv"); 
        
        Importer tcImporter = new Importer("/neoDB", false);
        tcImporter.importFile(FileType.TermComponents, "C:\\Users\\jthomson\\Desktop\\TermCompRelwLev.csv"); 
        
        //29205803 relationships took 29950 seconds w/3GB RAM, or approx ~1025 uploaded/second.
        // 7.29 GB space.
     */    
    }
    
    public static void performOldSearch() {
                Importer searchTest = new Importer("/neoDB", false);
           
        String tyWildcard = "ty*";
        String mpLevel = "MP";
        String ingLevel = "ING";
        
        // no efficient way to get all nodes from an index in cypher.  
        // Searching for index("prop=*") is really slow
        
        // pure index search on ty* only takes 379ms to count matching nodes.
        searchTest.stringSearch("start terms=node:EnglishText(\"EnglishText:" + tyWildcard + "\") return count(terms)");
        
        // term/level search takes 3.7 seconds to count 
        searchTest.countTermsByLevelNoDirection(tyWildcard, mpLevel);
        
        //neo4j doesn't effectively do searches given source AND target nodes.
        // compare the above to:
        // node 6 is the MP node
        //   String termlevelAnchored = "start level=node(6), terms=node:EnglishText(\"EnglishText:" 
        //       + stringTest + "\") match level<-[:LevelCode]-a-[:TermEnglish]->terms return count(distinct terms)";
        //searchTest.stringSearch(termlevelAnchored);    
        
        // searching without relationship direction is much faster.
        
        String adWildcard = "ad*";
        String advil="Advil";
        String pE="Pseudoephedrine";
        String componentText = "GELATIN";
        String componentType = "SEQUENCENUMBER3";
        
        
        // term/level search takes 3.7 seconds to count 
        //searchTest.countTermsByLevelNoDirection(adWildcard, mpLevel);
        
        //searchTest.getTermsByLevelNoDirection(adWildcard, mpLevel);
        searchTest.getTermsByLevelNoDirectionSorted(adWildcard, mpLevel);
        
        String gelatinComponentTypes = "start compTextNodes=node:EnglishText(\"EnglishText:GELATIN\") match compTextNodes-[r]-baseTerm return distinct type(r) as RelationshipType";
        searchTest.stringSearch(gelatinComponentTypes);   
        
        searchTest.getTermComponent(advil, mpLevel, componentText, componentType);
        
        searchTest.getTermsByLevelNoDirection(pE, ingLevel);
        // MUCH slower with all upper case input.  76s vs 1s!
    }
}