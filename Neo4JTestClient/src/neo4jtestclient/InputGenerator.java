/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4jtestclient;

import java.io.*;

/**
 *
 * @author JThomson
 */
public class InputGenerator {
    
    public static void main(String[] args) throws IOException{
        
        try 
        {
            String path = "/nodeOutput.csv";
            File nodeFile = new File(path);    
            Importer.deleteFileOrDirectory(nodeFile);

            FileWriter fileWriter = new FileWriter(nodeFile);
            BufferedWriter out = new BufferedWriter(fileWriter);
            
            //out.write(line);
            

            out.close();
        }
        catch (Exception e)
        {
            System.out.println(e.toString());
        }
    }
}
