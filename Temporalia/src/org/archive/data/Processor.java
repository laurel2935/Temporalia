package org.archive.data;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrInputDocument;
import org.archive.TDirectory;
import org.archive.dataset.trec.query.TRECDivQuery;
import org.archive.dataset.trec.query.TRECSubtopic;
import org.archive.util.io.IOText;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;

public class Processor {
	private static final boolean debug = false;
	
	//[source-encoding]
	public static final String [] LPFields = {"url", "title", "sourcerss", "id", "host", "date", "content", "source-encoding"};
	
	//
	public static void getBatFile(){
		try {
			File LivingProjectDataDir = new File(TDirectory.LivingProjectDataDir);
			File [] allFiles = LivingProjectDataDir.listFiles();
			
			ArrayList<String> nameList = new ArrayList<String>();
			for(File file: allFiles){
				nameList.add(file.getName());
			}
			
			ArrayList<String> checkRecordList = new ArrayList<String>();
			ArrayList<String> solrRecordList = new ArrayList<String>();
			
			String checkDir = "H:/v-haiyu/TaskPreparation/Temporalia/LivingProject_Check/";
			for(String name: nameList){
				String checkName = name.substring(0, name.indexOf("."))+"_check.xml";
				checkRecordList.add("java CheckSyntax "+TDirectory.LivingProjectDataDir+name+" > "+checkDir+checkName);
				
				solrRecordList.add("perl temporalia_solrify.pl "+checkDir+checkName);				
			}
			
			//output
			String checkBat = "H:/v-haiyu/TaskPreparation/Temporalia/tool/check.bat";
			String solrBat = "H:/v-haiyu/TaskPreparation/Temporalia/tool/solr.bat";
			
			IOText.output_UTF8(checkRecordList, checkBat);
			IOText.output_UTF8(solrRecordList, solrBat);
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	//
	public static List<TreeMap<String, String>> parseSolrFile(String file){
		List<TreeMap<String, String>> solrdocList = new ArrayList<>();
		
		try {
			SAXBuilder saxBuilder = new SAXBuilder();
			//Document xmlDoc = saxBuilder.build(new File(file));	
			//new InputStreamReader(new FileInputStream(targetFile), DEFAULT_ENCODING)
			Document xmlDoc = saxBuilder.build(new InputStreamReader(new FileInputStream(file), "UTF-8"));
			Element webtrackElement = xmlDoc.getRootElement();
			//doc list
			List docList = webtrackElement.getChildren("doc");
			for(int i=0; i<docList.size(); i++){				
				Element docElement = (Element)docList.get(i);
				List fieldList = docElement.getChildren("field");
				
				TreeMap<String, String> solrdoc = new TreeMap<>();
				
				for(int j=0; j<fieldList.size(); j++){
					Element fieldElement = (Element)fieldList.get(j);
					String fieldName = fieldElement.getAttributeValue("name");
					String fieldText = fieldElement.getText();
					
					solrdoc.put(fieldName, fieldText);
				}
				
				String idStr = solrdoc.get("id");
				solrdoc.put("id", idStr.substring(1, idStr.length()-1));
				solrdocList.add(solrdoc);						
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		if(debug){
			TreeMap<String, String> solrdoc = solrdocList.get(1);
			System.out.println(LPFields[0]+"\t"+solrdoc.get(LPFields[0]));
			System.out.println(LPFields[1]+"\t"+solrdoc.get(LPFields[1]));
			System.out.println(LPFields[2]+"\t"+solrdoc.get(LPFields[2]));
			System.out.println(LPFields[3]+"\t"+solrdoc.get(LPFields[3]));
			System.out.println(LPFields[4]+"\t"+solrdoc.get(LPFields[4]));
			System.out.println(LPFields[5]+"\t"+solrdoc.get(LPFields[5]));
			System.out.println(LPFields[6]+"\t"+solrdoc.get(LPFields[6]));
			
			if(solrdoc.containsKey(LPFields[7])){
				System.out.println(LPFields[7]+"\t"+solrdoc.get(LPFields[7]));
			}
			
		}
		
		return solrdocList;					
	}
	//
	
	/**
     * parse files: ..._check.xml
     * **/
    private static List<TreeMap<String, String>> parseCheckFile(String file){
	    List<TreeMap<String, String>> checkdocList = new ArrayList<>();
	    
	    ArrayList<String> lineList = IOText.getLinesAsAList_UTF8(file);
	    
	    try {
	      //build a standard pseudo-xml file
	      StringBuffer buffer = new StringBuffer();
	      buffer.append("<add>");
	      for(String line: lineList){
	        buffer.append(line);
	      }
	      buffer.append("</add>");  
	      
	      SAXBuilder saxBuilder = new SAXBuilder();	     
	      Document xmlDoc = saxBuilder.build(new InputStreamReader(new ByteArrayInputStream(buffer.toString().getBytes("UTF-8"))));
	      
	      Element webtrackElement = xmlDoc.getRootElement();
	      
	      //doc list
	      XMLOutputter xmlOutputter = new XMLOutputter();
	      
	      List docList = webtrackElement.getChildren("doc");
	      for(int i=0; i<docList.size(); i++){        
	        TreeMap<String, String> checkdoc = new TreeMap<>();
	        
	        Element docElement = (Element)docList.get(i);
	        String id = docElement.getAttributeValue("id");
	        checkdoc.put("id", id);
	        
	        Element metaElement = docElement.getChild("meta-info");
	        List tagList = metaElement.getChildren("tag");
	        for(int j=0; j<tagList.size(); j++){
	          Element tagElement = (Element)tagList.get(j);
	          String tagName = tagElement.getAttributeValue("name");
	          String tagText = tagElement.getText();
	          
	          checkdoc.put(tagName, tagText);
	        }
	        
	        Element textElement = docElement.getChild("text");	        
	        String text = xmlOutputter.outputString(textElement);	        
	        checkdoc.put("text", text);    
	        
	        checkdocList.add(checkdoc);           
	      }
	    } catch (Exception e) {
	      // TODO: handle exception
	      e.printStackTrace();
	    }
	    
	    if(debug){
			TreeMap<String, String> checkdoc = checkdocList.get(100);
			for(Entry<String, String> entry: checkdoc.entrySet()){
   			 System.out.println(entry.getKey()+"\t"+entry.getValue());
   			 System.out.println();
			}			
		}		
	    
	    return checkdocList;
	  }

	/**
    * index files: ..._check.xml
    * **/
    public static void indexFiles_check(String dirStr){
    	String indexPath = TDirectory.LPFileIndexPath;
    	
    	try {
    		System.out.println("Indexing to directory '" + indexPath + "'...");

  	        Directory dir = FSDirectory.open(new File(indexPath));  	      
  	        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_48);
  	        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_48, analyzer);
  	        boolean create = true;
  	        if(create) {
  	          // Create a new index in the directory, removing any previously indexed documents:
  	          iwc.setOpenMode(OpenMode.CREATE);
  	        }else {
  	          // Add new documents to an existing index:
  	          iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
  	        }
  	        IndexWriter indexWriter = new IndexWriter(dir, iwc);
  	        	      
  	        Date start = new Date();
  	        
	  	    File dirFile = new File(dirStr);
	        File [] files = dirFile.listFiles();        
	        for(File f: files){
	        	List<org.apache.lucene.document.Document> docs = new ArrayList<org.apache.lucene.document.Document>();
	        	List<TreeMap<String, String>> checkdocList = parseCheckFile(f.getAbsolutePath()); 
	        	
	        	for(TreeMap<String, String> checkdoc: checkdocList){
	        		// make a new, empty document
	        		org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
	                
	                Field idField = new StringField("id", checkdoc.get("id"), Field.Store.YES);
		            doc.add(idField);
	        		for(Entry<String, String> entry: checkdoc.entrySet()){
	        			 if(!entry.getKey().equals("id")){
	        				 StoredField storeField = new StoredField(entry.getKey(), entry.getValue());
	        				 doc.add(storeField);
	        			 }
	        		}
	        		
	        		docs.add(doc);
	        	}
	        	
	        	for(org.apache.lucene.document.Document doc: docs){        			
	                indexWriter.addDocument(doc);
	        	} 
	        }
	        
	        indexWriter.close();
	        Date end = new Date();
		    System.out.println(end.getTime() - start.getTime() + " total milliseconds");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
    }
	  
	public static void main(String []args){
		//1
		//Processor.getBatFile();
		
		//2
		//String file = "H:/v-haiyu/TaskPreparation/Temporalia/tool/t1_solr.xml";
		//Processor.parseSolrFile(file);
		
		//3
		//String file = "H:/v-haiyu/TaskPreparation/Temporalia/tool/t1.xml";
		//Processor.parseCheckFile(file);
		
		//4
		/*
		String lpTestFileDir = "H:/v-haiyu/TaskPreparation/Temporalia/tool/test2/";
		Processor.indexFiles_check(lpTestFileDir);
		*/
	}

}
