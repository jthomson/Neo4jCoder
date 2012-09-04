/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4jtestclient;

import org.neo4j.graphdb.RelationshipType;

/**
 *
 * @author JThomson
 */
public final class Constants {
       
    public final class Node {
        public final static String Type = "NodeType";
        
        public final static String Dictionary = "DictionaryN";
        public final static String Level = "LevelN"; 
        public final static String BaseComponent = "BaseCN";
        public final static String Base = "BaseN";
        public final static String English = "EnglishN";
    }
    
    public final class Property {
        public final static String EnglishText = "EnglishText";
        public final static String Code = "Code";
        public final static String DictionaryName = "DictionaryName";
        public final static String LevelName = "LevelName";  
        public final static String ComponentName = "ComponentName";
             
        public final static String CodeNodeKey="CNKey";
    }
    
    public final class DictionaryLevel {
        public final static String ATC1 = "ATC1";
        public final static String ATC2 = "ATC2";
        public final static String ATC3 = "ATC3";
        public final static String ATC4 = "ATC4";
        public final static String Ingredient = "ING";
        public final static String MP = "MP";
    }
    
}

    enum DictRelType implements RelationshipType
    {   
        DictionaryLevel,
        LevelCode,
        TermEnglish,
        ChildTerm,
        TermComponent,
        ComponentEnglish,
        LevelComponent,
        
        // WhoDrug Levels
        ATC1,
        ATC2,
        ATC3,
        ATC4,
        ING,
        MP,
        
        // ComponentRelationshipTypes
        DRUGRECORDNUMBER,
        SEQUENCENUMBER1,
        SEQUENCENUMBER2,
        SEQUENCENUMBER3,
        SEQUENCENUMBER4,
        GENERIC,
        NAMESPECIFIER,
        MARKETINGAUTHORIZATIONNUMBER,
        MARKETINGAUTHORIZATIONDATE,
        MARKETINGAUTHORIZATIONWITHDRAWALDATE,
        COUNTRY,
        COMPANY,
        COMPANYCOUNTRY,
        MARKETINGAUTHORIZATIONHOLDER,
        MARKETINGAUTHORIZATIONHOLDERCOUNTRY,
        SOURCEYEAR,
        SOURCE,
        SOURCECOUNTRY,
        PRODUCTTYPE,
        PRODUCTGROUP,
        PRODUCTGROUPDATERECORDED,
        CREATEDATE,
        DATECHANGED,
        INGREDIENTCREATEDATE,
        QUANTITY,
        QUANTITY2,
        UNIT,
        PHARMACEUTICALFORM,
        ROUTEOFADMINISTRATION,
        NUMBEROFINGREDIENTS,
        PHARMACEUTICALFORMCREATEDATE,
        SUBSTANCE,
        CASNUMBER,
        LANGUAGECODE
    }

    enum FileType
    {
        Terms, 
        TermRels,
        Components,
        TermComponentRels
    }
