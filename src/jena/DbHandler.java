/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package jena;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.sdb.StoreDesc;
import com.hp.hpl.jena.sdb.sql.JDBC;
import com.hp.hpl.jena.sdb.sql.SDBConnection;
import com.hp.hpl.jena.sdb.store.DatabaseType;
import com.hp.hpl.jena.sdb.store.DatasetStore;
import com.hp.hpl.jena.sdb.store.LayoutType;
import com.hp.hpl.jena.util.FileManager;
import java.util.Map;
import org.apache.log4j.Logger;

public class DbHandler 
{
  private static Logger logger = Logger.getLogger(JenaConnector.class);
  
  public double createDb(Map<String,String> map) throws Exception
  {
    double rdfcount = 0;
    try
    {
      String db = map.get("db");
      String dbuser = map.get("dbuser");
      String dbkey = map.get("dbkey");
      String filepath = map.get("filepath");
      String filetype = map.get("filetype");
      
      StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesIndex, DatabaseType.MySQL);

      JDBC.loadDriverMySQL();

      String jdbcURL = "jdbc:mysql://localhost:3306/"+db; 

      // Passing null for user and password causes them to be extracted
      // from the environment variables SDB_USER and SDB_PASSWORD
      SDBConnection conn = new SDBConnection(jdbcURL, dbuser, dbkey) ; 

      // Make store from connection and store description. 
      Store store = SDBFactory.connectStore(conn, storeDesc) ;
      store.getTableFormatter().create();

      // prepare the model
      Model fileModel = ModelFactory.createDefaultModel();
      FileManager.get().readModel(fileModel,filepath,filetype);
      System.out.println(fileModel.size());
      rdfcount = fileModel.size();

      Dataset ds = DatasetStore.create(store);
      ds.getDefaultModel().add(fileModel);     
    }
    catch (Exception e)
    {
      logger.error(e);
    }
    return rdfcount;
  }
  
  
  public void queryDb(String db) throws Exception
  {
    Store store = null;
    QueryExecution qe = null;
    
    try
    {
    /*String queryString = "PREFIX xsd:     <http://www.w3.org/2001/XMLSchema#>\n" +
"PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
"PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>\n" +
"PREFIX owl:     <http://www.w3.org/2002/07/owl#>\n" +
"PREFIX fn:      <http://www.w3.org/2005/xpath-functions#>\n" +
"PREFIX apf:     <http://jena.hpl.hp.com/ARQ/property#>\n" +
"PREFIX dc:      <http://purl.org/dc/elements/1.1/>\n" +
"\n" +
"SELECT ?book ?title\n" +
"WHERE\n" +
"   { ?book dc:title ?title }" ;*/
    String queryString = "SELECT * {?s ?o ?p}";
    Query query = QueryFactory.create(queryString) ;

    StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesIndex, DatabaseType.MySQL);

    JDBC.loadDriverMySQL();
    
    String jdbcURL = "jdbc:mysql://localhost:3306/"+db; 

    // Passing null for user and password causes them to be extracted
    // from the environment variables SDB_USER and SDB_PASSWORD
    SDBConnection conn = new SDBConnection(jdbcURL, "root", "password") ; 

    // Make store from connection and store description. 
    store = SDBFactory.connectStore(conn, storeDesc);
    Dataset ds = DatasetStore.create(store);
    
    qe = QueryExecutionFactory.create(query, ds) ;
    
    ResultSet rs = qe.execSelect() ;
    ResultSetFormatter.out(rs);
    
    }
    catch (Exception e)
    {logger.error(e);}
    finally { qe.close() ; }
    store.close() ;
  }
}
