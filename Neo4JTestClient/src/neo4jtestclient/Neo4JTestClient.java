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
        
        Importer importer = new Importer("/neoDB", true);
        importer.importComponentFile("C:\\Users\\jthomson\\Desktop\\Components.csv");
        
        Importer termImporter = new Importer("/neoDB", false);
        termImporter.importTermFile("C:\\Users\\jthomson\\Desktop\\FirstVersion.csv"); 
               
    }   
}