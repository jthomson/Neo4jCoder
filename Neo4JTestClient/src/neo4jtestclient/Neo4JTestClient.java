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
        
        
        Importer componentImporter = new Importer("/neoDB", true);
        componentImporter.importFile(FileType.Components, "C:\\Users\\jthomson\\Desktop\\Components.csv");
        
        Importer termImporter = new Importer("/neoDB", false);
        termImporter.importFile(FileType.Terms, "C:\\Users\\jthomson\\Desktop\\FirstVersion.csv"); 
        
        Importer tcImporter = new Importer("/neoDB", false);
        tcImporter.importFile(FileType.TermComponents, "C:\\Users\\jthomson\\Desktop\\TermComponents.csv"); 
              
        //Importer testy = new Importer("/neoDB", false);
        //testy.runLuceneTests();
    }   
}