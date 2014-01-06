/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package jena;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


public class JenaConnector 
{
  private static Logger logger = Logger.getLogger(JenaConnector.class);
  
  public static void main(String[] args) 
  {
    try
    {
      //FileHandler lvoFileHandler = new FileHandler();
      //lvoFileHandler.printRdfFile("jena/data/books.ttl","TURTLE","RDF/XML");
      //lvoFileHandler.queryFile("jena/data/books.nt");
      DbHandler lvoDbHandler = new DbHandler();
      Map map = new HashMap();
      map.put("db","test");
      map.put("dbuser","root");
      map.put("dbkey","password");
      map.put("filepath","jena/data/books.ttl");
      map.put("filetype","TURTLE");
      lvoDbHandler.createDb(map);
      //lvoDbHandler.queryDb("food");
    }
    catch (Exception e)
    {logger.error(e);}
  }
}
