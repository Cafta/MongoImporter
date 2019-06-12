package com.drcarlosamaral.MongoImporter;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;

/**
 * Exporta do BD ControleNovaAmerica para ControleCronicosBD
 *
 */
public class Importer 
{
	private static String URL = "mongodb://usuario:xyz123@localhost:27017";
    public static final String cnaDB = "ControleNovaAmerica";
    public static final String ccDB = "ControleCronicosBD";
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
    					if (doc.containsKey("Equipe")) paciente.append("equipe", doc.getString("Equipe"));
    					if (doc.containsKey("CNS")) paciente.append("cns", doc.getLong("CNS"));
    					if (doc.containsKey("medico")) paciente.append("medico", doc.getString("medico"));
    					if (doc.containsKey("name")) pessoa.append("name", doc.getString("name"));
    					if (doc.containsKey("nomeMae")) pessoa.append("nomeMae", doc.getString("nomeMae"));
    					if (doc.containsKey("dn")) pessoa.append("dn", doc.getDate("dn"));
    					pessoa.append("inscricao", LocalDate.now());
    					if (doc.containsKey("masc") && doc.getBoolean("masc")) pessoa.append("sexo", "mas");
    					if (doc.containsKey("fem") && doc.getBoolean("fem")) pessoa.append("sexo", "fem");
    					if (doc.containsKey("cell")) pessoa.append("cel", doc.getString("cell"));
    					if (doc.containsKey("vivo")) pessoa.append("vivo", doc.getBoolean("vivo"));
    					if (doc.containsKey("nota")) pessoa.append("obs", doc.getString("nota"));
    					if (doc.containsKey("hpp")) paciente.append("hpp", doc.getString("hpp"));
    					
    					if (doc.containsKey("tabagista")) paciente.append("tabagista", doc.getBoolean("tabagista"));
    					if (doc.containsKey("etilista")) paciente.append("etilista", doc.getBoolean("etilista"));
    					if (doc.containsKey("drogadicao")) paciente.append("drogadicao", doc.getBoolean("drogadicao"));
    					if (doc.containsKey("renal")) paciente.append("renal", doc.getBoolean("renal"));
    					if (doc.containsKey("riscoSocial")) paciente.append("riscoSocial", doc.getBoolean("riscoSocial"));
    					if (doc.containsKey("bolsaFamilia")) paciente.append("bolsaFamilia", doc.getBoolean("bolsaFamilia"));
    					
    					CCPessoas.insertOne(pessoa);
    					CCPaciente.insertOne(paciente);
    					
    					if (doc.containsKey("has") && doc.getBoolean("has") == true) {
    						Document hasDoc = new Document();
    						hasDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCHas.insertOne(hasDoc);
    						paciente.append("controleHas", hasDoc.getObjectId("_id"));
    					} 
    					if (doc.containsKey("dm") && doc.getBoolean("dm") == true) {
    						Document dmDoc = new Document();
    						dmDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCDm.insertOne(dmDoc);
    						paciente.append("controleDm", dmDoc.getObjectId("_id"));
    					} 
    					if (doc.containsKey("pneumo") && doc.getBoolean("pneumo") == true) {
    						Document pneumoDoc = new Document();
    						pneumoDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCPneumo.insertOne(pneumoDoc);
    						paciente.append("controlePneumo", pneumoDoc.getObjectId("_id"));
    					} 
    					if (doc.containsKey("vd") && doc.getBoolean("vd") == true) {
    						Document vdDoc = new Document();
    						vdDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCVd.insertOne(vdDoc);
    						paciente.append("controleVd", vdDoc.getObjectId("_id"));
    					} 
    					if (doc.containsKey("sm") && doc.getBoolean("sm") == true) {
    						Document smDoc = new Document();
    						smDoc.append("paciente_id", paciente.getObjectId("_id"));
    						CCSm.insertOne(smDoc);
    						paciente.append("controleSm", smDoc.getObjectId("_id"));
    					} 
    					
    					pessoa.append("paciente_id", paciente.getObjectId("_id"));
    					paciente.append("pessoa_id", pessoa.getObjectId("_id"));
    					
    					CCPaciente.replaceOne(eq("_id",paciente.getObjectId("_id")), paciente);
    					CCPessoas.replaceOne(eq("_id",pessoa.getObjectId("_id")), pessoa);
    					
    					if (doc.containsKey("receita")) {
    						Document rec = new Document();
    						rec.append("receita", doc.getString("receita"));
    						rec.append("data", DateUtils.asDate(LocalDate.now()));
    						rec.append("atual", true);
    						rec.append("antiga", false);
    						rec.append("umaVia", true);
    						rec.append("duasVias", false);
    						rec.append("paciente_id", paciente.getObjectId("_id"));
    						if (!rec.getString("receita").equals(""))
    							CCReceitas.insertOne(rec);
    					}
    					if (doc.containsKey("receita2")) {
    						Document rec = new Document();
    						rec.append("receita", doc.getString("receita2"));
    						rec.append("data", DateUtils.asDate(LocalDate.now()));
    						rec.append("atual", true);
    						rec.append("antiga", false);
    						rec.append("umaVia", true);
    						rec.append("duasVias", false);
    						rec.append("paciente_id", paciente.getObjectId("_id"));
    						if (!rec.getString("receita").equals(""))
    							CCReceitas.insertOne(rec);
    					}
    					if (doc.containsKey("receita3")) {
    						Document rec = new Document();
    						rec.append("receita", doc.getString("receita3"));
    						rec.append("data", DateUtils.asDate(LocalDate.now()));
    						rec.append("atual", true);
    						rec.append("antiga", false);
    						rec.append("umaVia", false);
    						rec.append("duasVias", true);
    						rec.append("paciente_id", paciente.getObjectId("_id"));
    						if (!rec.getString("receita").equals(""))
    							CCReceitas.insertOne(rec);
    					}

    				});
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
    	// LOGIN / FUNCIONARIOS
    	try (MongoClient mongoClient = new MongoClient(connectionString)){
    		MongoDatabase mongoCNA = mongoClient.getDatabase(cnaDB);
    		MongoDatabase mongoCC = mongoClient.getDatabase(ccDB);
    		MongoCollection<Document> cnaFuncionarios = mongoCNA.getCollection("Funcionarios");
    		MongoCollection<Document> CCPessoas = mongoCC.getCollection("Pessoas");
    		MongoCollection<Document> CCFuncionarios = mongoCC.getCollection("Funcionarios");
    		MongoCursor<Document> cnaFuncionariosCursor = cnaFuncionarios.find().iterator();
    		cnaFuncionariosCursor.forEachRemaining(cnaFuncionario -> {
    			Document fila = new Document();
    			// Se já tiver esse nome em cnaPessoas eu coloco o _id no pessoas_id ao invés do nome direto no funcionarios.
    			if (cnaFuncionario != null) {	
	    			if (cnaFuncionario.containsKey("nome")) fila.append("alias", cnaFuncionario.getString("nome"));
	    			if (cnaFuncionario.containsKey("login")) fila.append("login", cnaFuncionario.getString("login"));
	    			if (cnaFuncionario.containsKey("pwd")) fila.append("pwd", cnaFuncionario.getLong("pwd"));
	    			if (cnaFuncionario.containsKey("unidade")) fila.append("unidade", cnaFuncionario.getString("unidade"));
	    			if (cnaFuncionario.containsKey("funcao")) fila.append("funcao", cnaFuncionario.getString("funcao"));
	    			if (cnaFuncionario.containsKey("especializacao")) fila.append("especializacao", cnaFuncionario.getString("especializacao"));
	    			if (cnaFuncionario.containsKey("grupo")) {
	    				fila.append("grupo", cnaFuncionario.getString("grupo")); 
	    			} else {
	    				fila.append("grupo", "usuario");
	    			}
	    			if (cnaFuncionario.containsKey("versao")) fila.append("versao", cnaFuncionario.getString("versao"));
	    			CCFuncionarios.insertOne(fila);
    			}
    		});
			MongoCursor<Document> funcionariosCursor = CCFuncionarios.find().iterator();
			funcionariosCursor.forEachRemaining(funcionario -> {
				Document pessoa = CCPessoas.find(eq("name", funcionario.getString("alias"))).first();
				if (pessoa != null && !pessoa.isEmpty()) {
					//associa à pessoa
					Document updateFuncionario = CCFuncionarios.find(eq("_id",funcionario.getObjectId("_id"))).first();
					updateFuncionario.append("pessoa_id", pessoa.getObjectId("_id"));
					CCFuncionarios.replaceOne(eq("_id",updateFuncionario.getObjectId("_id")), updateFuncionario);
					Document updatePessoa = CCPessoas.find(eq("_id",pessoa.getObjectId("_id"))).first();
					updatePessoa.append("funcionario_id", updateFuncionario.getObjectId("_id"));
					CCPessoas.replaceOne(eq("_id", updatePessoa.getObjectId("_id")), updatePessoa);
				} else {
					//cria nova pessoa?  Não! Deixa sem mesmo!
				}
			});
    	} catch (Exception e) {
    		e.printStackTrace();
    	}	
    	
    	// FAMILIAS -> Endereço
    	try (MongoClient mongoClient = new MongoClient(connectionString)){
    		MongoDatabase mongoCNA = mongoClient.getDatabase(cnaDB);
    		MongoDatabase mongoCC = mongoClient.getDatabase(ccDB);
    		MongoCollection<Document> cnaFamilias = mongoCNA.getCollection("Familias");
    		MongoCollection<Document> ccEndereco = mongoCC.getCollection("Enderecos");
    		MongoCollection<Document> ccPaciente = mongoCC.getCollection("Pacientes");
    		MongoCursor<Document> cursor = cnaFamilias.find().iterator();
    		cursor.forEachRemaining(fam -> {
    			Document novoEndereco = new Document();
    			if (fam.containsKey("ff")) novoEndereco.append("ff", fam.getString("ff"));
    			if (fam.containsKey("posto")) novoEndereco.append("posto", fam.getString("posto"));
    			if (fam.containsKey("Equipe")) novoEndereco.append("equipe", fam.getString("Equipe"));
    			if (fam.containsKey("Endereco")) novoEndereco.append("rua", fam.getString("Endereco"));
    			if (fam.containsKey("Numero")) novoEndereco.append("numero", fam.getString("Numero"));
    			if (fam.containsKey("Bairro")) novoEndereco.append("bairro", fam.getString("Bairro"));
    			if (fam.containsKey("Tel")) novoEndereco.append("tel", fam.getString("Tel"));
    			if (fam.containsKey("Nota")) novoEndereco.append("nota", fam.getString("Nota"));
    			if (novoEndereco != null && !novoEndereco.isEmpty()) {
    				ArrayList<ObjectId> moradoresId = new ArrayList<>();
    				MongoCursor<Document> ppCursor = ccPaciente.find().iterator();
    				ppCursor.forEachRemaining(paciente -> {
    					if (paciente.containsKey("ff") && novoEndereco.containsKey("ff") &&
    							paciente.getString("ff").equals(novoEndereco.getString("ff"))) {
    						if (paciente.containsKey("pessoa_id"))
    							moradoresId.add(paciente.getObjectId("pessoa_id"));
    					}
    				});
    				if (moradoresId != null && !moradoresId.isEmpty())
    					novoEndereco.append("moradores", moradoresId);
    				ccEndereco.insertOne(novoEndereco);
    			}
    		});
    	}
    }
}
