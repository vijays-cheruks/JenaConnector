/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package jena;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.sys.TDBInternal;
import com.hp.hpl.jena.util.FileManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.jena.riot.RIOT;
import org.apache.log4j.Logger;

public class FileHandler 
{
  private static Logger logger = Logger.getLogger(JenaConnector.class);
 
  
  public void loadRdfFile(String filepath)
  {
    FileManager.get().addLocatorClassLoader(FileHandler.class.getClassLoader());
    Model model = FileManager.get().loadModel(filepath, null, "TURTLE");
    
    StmtIterator iter = model.listStatements();
    try 
    {
        while ( iter.hasNext() ) 
        {
            Statement stmt = iter.next();

            Resource s = stmt.getSubject();
            Resource p = stmt.getPredicate();
            RDFNode o = stmt.getObject();

            if ( s.isURIResource() ) {
                System.out.print("URI:"+s.getURI());
            } else if ( s.isAnon() ) {
                System.out.print("[blank]");
            }

            if ( p.isURIResource() ) 
                System.out.print(" URI ");

            if ( o.isURIResource() ) {
                System.out.print("URI:"+o.asResource().getURI());
            } else if ( o.isAnon() ) {
                System.out.print("[blank]");
            } else if ( o.isLiteral() ) {
                System.out.print("literal:"+o.asLiteral().getString());
            }

            System.out.println();                
        }
    } 
    finally 
    {
        if ( iter != null ) iter.close();
    } 
  }
  
  public void printRdfFile(String filepath,String ifileformat,String ofileformat) throws IOException
  {
    InputStream in = this.getClass().getClassLoader().getResourceAsStream(filepath);
        
    //RIOT.init() ;

    Model model = ModelFactory.createDefaultModel(); // creates an in-memory Jena Model
    model.read(in, null, ifileformat); // parses an InputStream assuming RDF in Turtle format

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    // Write the Jena Model in Turtle, RDF/XML and N-Triples format
    logger.info("<< "+ofileformat.toUpperCase()+" >>");    
    model.write(out,ofileformat);
    logger.info(out);
    /*System.out.println("\n---- RDF/XML ----");
    model.write(System.out, "RDF/XML");
    System.out.println("\n---- RDF/XML Abbreviated ----");
    model.write(System.out, "RDF/XML-ABBREV");
    System.out.println("\n---- N-Triples ----");
    model.write(System.out, "N-TRIPLES");
    System.out.println("\n---- RDF/JSON ----");
    model.write(System.out, "RDF/JSON");*/
  }
  
  public void queryFile(String filepath)
  {
    try
    {
      InputStream in = this.getClass().getClassLoader().getResourceAsStream(filepath);

      Location location = new Location ("./target/TDB");
      
      // Load some initial data
      TDBLoader.load(TDBInternal.getBaseDatasetGraphTDB(TDBFactory.createDatasetGraph(location)), in, true);
      
      String queryString = "PREFIX  dc:   <http://purl.org/dc/elements/1.1/>\n" +
"PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
"PREFIX  apf:  <http://jena.hpl.hp.com/ARQ/property#>\n" +
"PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
"PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
"PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
"PREFIX  fn:   <http://www.w3.org/2005/xpath-functions#>\n" +
"\n" +
"SELECT  ?book ?title\n" +
"WHERE\n" +
"  { ?book dc:title ?title }";

      Dataset dataset = TDBFactory.createDataset(location);
      dataset.begin(ReadWrite.READ);
      try {
          Query query = QueryFactory.create(queryString);
          QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
          
          try {
              ResultSet results = qexec.execSelect();              
              while ( results.hasNext() ) {
                  QuerySolution soln = results.nextSolution();                  
                  Literal name = soln.getLiteral("title");
                  System.out.println(name);
              }
          } finally {
              qexec.close();
          }
      } 
      finally 
      {
          dataset.end();
      }
    }
    catch(Exception e)
    {logger.error(e);}
  }
}
