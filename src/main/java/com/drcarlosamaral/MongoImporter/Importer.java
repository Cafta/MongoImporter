package com.drcarlosamaral.MongoImporter;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

import java.time.LocalDate;
import java.util.Date;

/**
 * Exporta do BD ControleNovaAmerica para ControleCronicosBD
 *
 */
public class Importer 
{
	private static String URL = "mongodb://usuario:xyz123@localhost:27017";
    public static final String cnaDB = "ControleNovaAmerica";
    public static final String ccDB = "ControleCronicosBD2";
    public static final String VERSAO = "1.2.0 (ccDB)";
    
    public static void main( String[] args )
    {
    	MongoClientURI connectionString = new MongoClientURI(URL);
    	
    	// PESSOAS
    	try (MongoClient mongoClient = new MongoClient(connectionString)){
    		MongoDatabase mongoCNA = mongoClient.getDatabase(cnaDB);
    		MongoDatabase mongoCC = mongoClient.getDatabase(ccDB);
    		MongoCollection<Document> cnaPessoas = mongoCNA.getCollection("Pessoas");
    		MongoCollection<Document> CCPessoas = mongoCC.getCollection("Pessoas");
    		MongoCollection<Document> CCPaciente = mongoCC.getCollection("Pacientes");
    		MongoCollection<Document> CCHas = mongoCC.getCollection("HAS");
//    		MongoCollection<Document> CCInfancia = mongoCC.getCollection("Infancia");
    		MongoCollection<Document> CCDm = mongoCC.getCollection("DM");
    		MongoCollection<Document> CCVd = mongoCC.getCollection("VD");
    		MongoCollection<Document> CCPneumo = mongoCC.getCollection("Pneumo");
    		MongoCollection<Document> CCSm = mongoCC.getCollection("SM");
    		MongoCollection<Document> CCReceitas = mongoCC.getCollection("Receitas");
//    		MongoCollection<Document> CCGestacao = mongoCC.getCollection("Gestacao"); 
    		MongoCursor<Document> cursor = cnaPessoas.find().iterator();
    		cursor.forEachRemaining(doc -> 
    				{
    					Document pessoa = new Document();
    					Document paciente = new Document();
    					if (doc.containsKey("ff")) paciente.append("ff", doc.getString("ff"));
    					if (doc.containsKey("posto")) paciente.append("posto", doc.getString("posto"));
    					if (doc.containsKey("CNS")) paciente.append("cns", doc.getLong("CNS"));
    					if (doc.containsKey("medico")) paciente.append("medico", doc.getString("medico"));
    					if (doc.containsKey("name")) pessoa.append("name", doc.getString("name"));
    					if (doc.containsKey("masc") && doc.getBoolean("masc")) pessoa.append("sexo", "mas");
    					if (doc.containsKey("fem") && doc.getBoolean("fem")) pessoa.append("sexo", "fem");
    					if (doc.containsKey("cell")) pessoa.append("tel", doc.getString("cell"));
    					if (doc.containsKey("vivo")) pessoa.append("vivo", doc.getBoolean("vivo"));
    					if (doc.containsKey("nota")) pessoa.append("nota", doc.getString("nota"));
    					if (doc.containsKey("hpp")) paciente.append("hpp", doc.getString("hpp"));
    					
    					if (doc.containsKey("tabagista")) paciente.append("tabagista", doc.getBoolean("tabagista"));
    					if (doc.containsKey("etilista")) paciente.append("etilista", doc.getBoolean("etilista"));
    					if (doc.containsKey("drogadicao")) paciente.append("drogadicao", doc.getBoolean("drogadicao"));
    					if (doc.containsKey("renal")) paciente.append("renal", doc.getBoolean("renal"));
    					if (doc.containsKey("riscoSocial")) paciente.append("riscoSocial", doc.getBoolean("riscoSocial"));
    					if (doc.containsKey("bolsaFamilia")) paciente.append("bolsaFamilia", doc.getBoolean("bolsaFamilia"));

    					CCPessoas.insertOne(pessoa);
    					CCPaciente.insertOne(paciente);
    					
    					pessoa.append("paciente_id", paciente.getObjectId("_id"));
    					paciente.append("pessoa_id", pessoa.getObjectId("_id"));
    					
    					CCPaciente.replaceOne(eq("_id",paciente.getObjectId("_id")), paciente);
    					CCPessoas.replaceOne(eq("_id",pessoa.getObjectId("_id")), pessoa);
    					
    					if (doc.containsKey("receita")) {
    						Document rec = new Document();
    						rec.append("receita", doc.getString("receita"));
    						rec.append("data", DateUtils.asDate(LocalDate.now()));
    						rec.append("tipo", "1via");
    						rec.append("paciente_Id", paciente.getObjectId("_id"));
    						CCReceitas.insertOne(rec);
    					}
    					if (doc.containsKey("receita2")) {
    						Document rec = new Document();
    						rec.append("receita", doc.getString("receita2"));
    						rec.append("data", DateUtils.asDate(LocalDate.now()));
    						rec.append("tipo", "1via");
    						rec.append("paciente_Id", paciente.getObjectId("_id"));
    						CCReceitas.insertOne(rec);
    					}
    					if (doc.containsKey("receita3")) {
    						Document rec = new Document();
    						rec.append("receita", doc.getString("receita3"));
    						rec.append("data", DateUtils.asDate(LocalDate.now()));
    						rec.append("tipo", "2vias");
    						rec.append("paciente_Id", paciente.getObjectId("_id"));
    						CCReceitas.insertOne(rec);
    					}
    					
    					
    					if (doc.containsKey("has") && doc.getBoolean("has")) {
    						Document hasDoc = new Document();
    						hasDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCHas.insertOne(hasDoc);
    					}
    					if (doc.containsKey("dm") && doc.getBoolean("dm")) {
    						Document dmDoc = new Document();
    						dmDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCDm.insertOne(dmDoc);
    					}
    					if (doc.containsKey("pneumo") && doc.getBoolean("pneumo")) {
    						Document pneumoDoc = new Document();
    						pneumoDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCPneumo.insertOne(pneumoDoc);
    					}
    					if (doc.containsKey("vd") && doc.getBoolean("vd")) {
    						Document vdDoc = new Document();
    						vdDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCVd.insertOne(vdDoc);
    					}
    					if (doc.containsKey("sm") && doc.getBoolean("sm")) {
    						Document smDoc = new Document();
    						smDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCSm.insertOne(smDoc);
    					}
    					
    				});
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
    	// LOGIN / FUNCIONARIOS
    	try (MongoClient mongoClient = new MongoClient(connectionString)){
    		MongoDatabase mongoCNA = mongoClient.getDatabase(cnaDB);
    		MongoDatabase mongoCC = mongoClient.getDatabase(ccDB);
    		MongoCollection<Document> cnaPessoas = mongoCNA.getCollection("Funcionarios");
    		MongoCollection<Document> CCFuncionarios = mongoCC.getCollection("Funcionarios");
    		MongoCursor<Document> cursor = cnaPessoas.find().iterator();
    		cursor.forEachRemaining(doc -> {
    			Document fila = new Document();
    			if (doc.containsKey("nome")) fila.append("nome", doc.getString("nome"));
    			if (doc.containsKey("login")) fila.append("login", doc.getString("login"));
    			if (doc.containsKey("pwd")) fila.append("pwd", doc.getLong("pwd"));
    			if (doc.containsKey("unidade")) fila.append("unidade", doc.getString("unidade"));
    			if (doc.containsKey("funcao")) fila.append("funcao", doc.getString("funcao"));
    			if (doc.containsKey("especializacao")) fila.append("especializacao", doc.getString("especializacao"));
    			if (doc.containsKey("grupo")) {
    				fila.append("grupo", doc.getString("grupo")); 
    			} else {
    				fila.append("grupo", "usuario");
    			}
    			if (doc.containsKey("versao")) fila.append("versao", doc.getString("versao"));
    			CCFuncionarios.insertOne(fila);
    		});
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    	}	
    }
}
