package com.drcarlosamaral.MongoImporter;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Exporta do BD ControleNovaAmerica para ControleCronicosBD
 *
 */
public class Importer 
{
	private static String URL = "mongodb://Admin:hehehe@172.22.74.41:27017";
	//private static String URL = "mongodb://usuario:xyz123@localhost:27017";
	private static MongoClientURI connectionString = new MongoClientURI(URL);
    public static final String cnaDB = "ControleNovaAmerica";
    public static final String ccDB = "ControleCronicosBD";
    public static final String VERSAO = "1.2.0 (ccDB)";

    private static Document luiz = new Document();
    private static Document carlos = new Document();
    
    private static Map<Long, Document> mapaSUS_Paciente = new HashMap<>();
    
    public static void main( String[] args )
    {
    	
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
    					paciente.append("posto", "C.S.Nova América");
    					if (doc.containsKey("Equipe")) paciente.append("equipe", doc.getString("Equipe"));
    					if (doc.containsKey("CNS")) paciente.append("cns", doc.getLong("CNS"));
    					if (doc.containsKey("medico")) paciente.append("medicoString", doc.getString("medico"));
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
    		MongoCollection<Document> CCPacientes = mongoCC.getCollection("Pacientes");
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
	    			if (fila.getString("login").equals("Carlos")) {
	    				fila.replace("grupo", "adm");
	    			}
	    			CCFuncionarios.insertOne(fila);
	    			if (fila.getString("login").equals("Luiz")) luiz = fila;
	    			if (fila.getString("login").equals("Carlos")) {
	    				fila.replace("grupo", "adm");
	    				carlos = fila;
	    			}
    			}
    		});
			MongoCursor<Document> funcionariosCursor = CCFuncionarios.find().iterator();
			funcionariosCursor.forEachRemaining(funcionario -> {
				// relacionar pessoa ao funcionario:
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
			MongoCursor<Document> pacientesCursor = CCPacientes.find().iterator();
			pacientesCursor.forEachRemaining(pac -> {
				if (pac.containsKey("medicoString")) {
					if (pac.get("medicoString") != null && pac.getString("medicoString").equals("Dr.Carlos")) {
						pac.append("medico", carlos.getObjectId("_id"));
					} else if (pac.get("medicoString") != null && pac.getString("medicoString").equals("Dr.Luiz")) {
						pac.append("medico", luiz.getObjectId("_id"));
					}
					pac.remove("medicoString");
					CCPacientes.replaceOne(eq("_id", pac.getObjectId("_id")), pac);
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
    		MongoCollection<Document> ccPessoa = mongoCC.getCollection("Pessoas");
    		MongoCursor<Document> familiasCursor = cnaFamilias.find().iterator();
    		familiasCursor.forEachRemaining(fam -> {
    			Document novoEndereco = new Document();
    			if (fam.containsKey("ff")) novoEndereco.append("ff", fam.getString("ff"));
    			novoEndereco.append("posto", "C.S.Nova América");
    			novoEndereco.append("municipio", "Campinas");
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
    		MongoCursor<Document> enderecoCursor = ccEndereco.find().iterator();
			enderecoCursor.forEachRemaining(end -> {
				if (end.containsKey("moradores")) {
					ArrayList<ObjectId> mIds = (ArrayList<ObjectId>) end.get("moradores");
					for (ObjectId mId : mIds) {
						Document p = ccPessoa.find(eq("_id", mId)).first();
						p.append("endereco_id", end.getObjectId("_id"));
						ccPessoa.findOneAndReplace(eq("_id", p.getObjectId("_id")), p);
					}
				}
			});
    	}
    	
    	// Consultas
    	
    	try (MongoClient mongoClient = new MongoClient(connectionString)){
    		MongoDatabase mongoCNA = mongoClient.getDatabase(cnaDB);
    		MongoDatabase mongoCC = mongoClient.getDatabase(ccDB);
    		MongoCollection<Document> cnaEventos = mongoCNA.getCollection("Eventos");
    		MongoCollection<Document> ccPacientes = mongoCC.getCollection("Pacientes");
    		MongoCollection<Document> ccConsultas = mongoCC.getCollection("Consultas");
    		MongoCursor<Document> cursor = cnaEventos.find(exists("quem")).iterator();
    		while (cursor.hasNext()) {
    			Document c = cursor.next();
    			Document consulta = new Document();
    			Document paciente = ccPacientes.find(eq("cns", c.getLong("CNS"))).first();
    			if (paciente != null) {
	    			consulta.append("paciente_id", paciente.getObjectId("_id"));
	    			if (c.containsKey("nota")) consulta.append("obs", c.getString("nota"));
	    			if (c.getString("quem").equals("Dr.Carlos")) consulta.append("funcionario_id", carlos.getObjectId("_id"));
	    			if (c.getString("quem").equals("Dr.Luiz")) consulta.append("funcionario_id", luiz.getObjectId("_id"));
	    			consulta.append("data", c.getDate("data"));
	    			consulta.append("local", "C.S.Nova América");
	    			ccConsultas.insertOne(consulta);
    			}
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
    	// EVENTOS - PA, Glicemia, HbA1c, Exames da Saúde da Mulher e Impressos
    	
    	try (MongoClient mongoClient = new MongoClient(connectionString)) {
    		MongoDatabase cnaBD = mongoClient.getDatabase("ControleNovaAmerica");
    		MongoDatabase ccDB = mongoClient.getDatabase("ControleCronicosBD");
    		MongoCollection<Document> eventos = cnaBD.getCollection("Eventos");
    		MongoCollection<Document> impressos = ccDB.getCollection("Impressos");
    		MongoCollection<Document> exames = ccDB.getCollection("Exames");
    		MongoCollection<Document> pacientes = ccDB.getCollection("Pacientes");
    		MongoCursor<Document> cursor = eventos.find().iterator();
    		cursor.forEachRemaining(evento -> {
    			Document exame = new Document();
    			Document paciente = pacientes.find(eq("cns", evento.getLong("CNS"))).first();
    			if (paciente == null) {
    				System.out.println("PACIENTE NÃO ENCONTRADO COM CNS: " + evento.getLong("CNS"));
    				System.out.println("Vamos vasculhar evento por uma pista");
    				paciente = vasculha(evento);
    				if (paciente != null) System.out.println("Paciente mudou CNS para: " + paciente.getLong("CNS"));
    			}
    			if (paciente != null) {
    				exame.append("paciente_id", paciente.getObjectId("_id"));
    				exame.append("data", evento.getDate("data"));
    				// PA
    				if (evento.containsKey("tipo") && evento.getString("tipo").equals("PA")) {
	    				exame.append("nome", "pa");
		    			Document pa = new Document();
		    				pa.append("max", ((Document) evento).getInteger("MAX"));
		    				pa.append("min", ((Document) evento).getInteger("MIN"));
		    			exame.append("resultado", pa.getInteger("max") + "x" + pa.getInteger("min") + "mmHg");
		    			exame.append("detalhes", pa);
		    			exames.insertOne(exame);
    				}
    				// GLICEMIA JEJUM / HbA1c
    				else if (evento.containsKey("gli") || evento.containsKey("hba1c")) {
    					if (evento.containsKey("gli")) {
    						Document gl = new Document();
    						exame.append("nome", "glicemia");
    						if (evento.containsKey("posPrandial")) {
    							gl.append("jejum", evento.getBoolean("posPrandial"));
    						} else {
    							gl.append("jejum", true);
    						}
    						exame.append("resultado", evento.getInteger("gli") + "mg/dL");
    						gl.append("glicemia", evento.getInteger("gli"));
    						exame.append("detalhes", gl);
    		    			exames.insertOne(exame);
    					}
    					if (evento.containsKey("hba1c")) {
    						if (exame.containsKey("_id")) exame.remove("_id");
    						if (exame.containsKey("nome")) exame.remove("nome");
    						if (exame.containsKey("resultado")) exame.remove("resultado");
    						if (exame.containsKey("detalhes")) exame.remove("detalhes");
    						exame.append("nome", "hba1c");
    						exame.append("resultado", evento.getDouble("hba1c") + "%");
    						Document hGlicada = new Document();
    							hGlicada.append("hba1c", evento.getDouble("hba1c"));
    						exame.append("detalhes", hGlicada);
    		    			exames.insertOne(exame);
    					}
    				}
    				// Exames da Saúde da Mulher
    				else if (evento.containsKey("tipo") && evento.getString("tipo").equals("SMULHER")) {
    					if (evento.getString("exame").equals("Mamografia")) {
    						exame.append("nome", "Mamografia");
    					} else if (evento.getString("exame").equals("C.O.")) {
    						exame.append("nome", "Papanicolau");
    					} else if (evento.getString("exame").equals("USG mamas")) {
    						exame.append("nome", "USG mamas");
    					} else if (evento.getString("exame").equals("USGtv")) {
    						exame.append("nome", "USGtv");
    					}
						String resultado = "";
						Document detalhes = new Document();
						if (evento.containsKey("nota")) {
							resultado = evento.getString("nota");
							detalhes.append("nota", evento.getString("nota"));
						} else {
							if (evento.containsKey("alterado")) {
								resultado = evento.getBoolean("alterado") ? "Alterado" : "Normal";
							}
						}
						if (evento.containsKey("alterado")) detalhes.append("alterado", evento.getBoolean("alterado"));
						if (!resultado.equals("")) exame.append("resultado", resultado);
						if (!detalhes.isEmpty()) exame.append("detalhes", detalhes);
		    			exames.insertOne(exame);
    				}
    				// ENCAMINHAMENTOS
    				else if (evento.containsKey("tipo") && evento.getString("tipo").equals("ENCAMINHAMENTO")) {
	    				Document doc = new Document();
	    				if (evento.containsKey("CNS")) doc.append("cns", evento.getLong("CNS"));
	    				if (evento.containsKey("especialidade") && !evento.getString("especialidade").equals("")) 
	    					doc.append("especialidade", evento.getString("especialidade"));
	    				if (evento.containsKey("ff") && !evento.getString("ff").equals("")) 
	    					doc.append("ff", evento.getString("ff"));
	    				if (evento.containsKey("hd") && !evento.getString("hd").equals("")) 
	    					doc.append("hd", evento.getString("hd"));
	    				if (evento.containsKey("nome") && !evento.getString("nome").equals("")) 
	    					doc.append("nome", evento.getString("nome"));
	    				if (evento.containsKey("dn") && !evento.getString("dn").equals("")) 
	    					doc.append("dn", DateUtils.asDate(evento.getString("dn")));
	    				if (evento.containsKey("nomeMae") && !evento.getString("nomeMae").equals("")) 
	    					doc.append("nomeDaMae", evento.getString("nomeMae"));
	    				if (evento.containsKey("unidade") && !evento.getString("unidade").equals("")) 
	    					doc.append("posto", evento.getString("unidade"));
	    				if (evento.containsKey("quadroClinico") && !evento.getString("quadroClinico").equals("")) 
	    					doc.append("quadroClinico", evento.getString("quadroClinico"));
	    				if (evento.containsKey("exames") && !evento.getString("exames").equals("")) 
	    					doc.append("resultadoExames", evento.getString("exames"));
	    				if (evento.containsKey("tel") && !evento.getString("tel").equals("")) 
	    					doc.append("tel", evento.getString("tel"));
	    				doc.append("vermelho", evento.getBoolean("vermelho"));
	    				doc.append("amarelo", evento.getBoolean("amarelo"));
	    				doc.append("verde", evento.getBoolean("verde"));
	    				doc.append("azul", evento.getBoolean("azul"));
	    				if (evento.containsKey("data") && evento.getDate("data") != null)
	    					doc.append("data", evento.getDate("data"));
    				
	    				Document impresso = new Document();
	    				if (paciente.getObjectId("_id") != null) impresso.append("paciente_id", paciente.getObjectId("_id"));
	    				impresso.append("oQue", "Referencia");
	    				if (evento.containsKey("data") && evento.getDate("data") != null)
	    					impresso.append("quando", evento.getDate("data"));
	    				if (doc != null) impresso.append("doc", doc);
	    				
	    				impressos.insertOne(impresso);
    				}
    			} else {
    				System.out.println("PACIENTE NÃO ENCONTRADO");
    			}
    		});
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    
    private static Document vasculha(Document evento) {
    	if (evento == null || !evento.containsKey("SUS")) return null;
    	if (mapaSUS_Paciente.containsKey(evento.getLong("SUS"))) {
    		return mapaSUS_Paciente.get(evento.getLong("SUS"));
    	}
    	if (evento.containsKey("tipo") &&
    			(evento.getString("tipo").equals("PRENATAL") ||
    			evento.getString("tipo").equals("GRUPOGESTANTES") ||
    			evento.getString("tipo").equals("VACINAS"))) {
    		// tenta pegar a gestante pelo sis e depois o paciente pelo nome e ff
    		Document gestante = getGestanteBySIS(evento.getLong("SIS"));
    		if (gestante != null) {
	    		Document pct =  getPacienteByNameAndFf(gestante.getString("nome"), gestante.getString("ff"));
	    		if (pct != null) {
	    			mapaSUS_Paciente.put(evento.getLong("SUS"), pct);
	    			return pct;
	    		}
    		}
    	}
    	else if (evento.containsKey("tipo") && evento.getString("tipo").equals("ENCAMINHAMENTO")) {
    		Document pct =  getPacienteByNameAndFf(evento.getString("nome"), evento.getString("ff"));
    		if (pct != null) {
    			mapaSUS_Paciente.put(evento.getLong("SUS"), pct);
    			return pct;
    		}
    	}
    	else if (evento.containsKey("tipo") && evento.getString("tipo").equals("SIS")) {
    		Document pct = getPacienteByNomeEDn(evento.getString("nome"), evento.getDate("dn"));
    		if (pct != null && !pct.isEmpty()) {
    			mapaSUS_Paciente.put(evento.getLong("SUS"), pct);
    			return pct;
    		}
    	}
    	mapaSUS_Paciente.put(evento.getLong("SUS"), null);
    	return null;
    }
    
    private static Document getPacienteByNomeEDn(String nome, Date dn) {
    	Document paciente = null;
    	try (MongoClient mongoClient = new MongoClient(connectionString)) {
    		MongoDatabase cc = mongoClient.getDatabase("ControleCronicosBD");
    		MongoCollection<Document> pessoas = cc.getCollection("Pessoas");
    		MongoCollection<Document> pacientes = cc.getCollection("Pacientes");
    		Document pessoa = pessoas.find(and(eq("name", nome), eq("dn", dn))).first();
			paciente = pacientes.find(eq("_id", pessoa.getObjectId("paciente_id"))).first();
    	}
    	return paciente;
    }
    
    private static Document getPacienteByNameAndFf(String name, String ff) {
    	Document pct = null;
    	try (MongoClient mongoClient = new MongoClient(connectionString)) {
    		MongoDatabase cc = mongoClient.getDatabase("ControleCronicosBD");
    		MongoCollection<Document> pacientes = cc.getCollection("Pacientes");
    		MongoCollection<Document> pessoas = cc.getCollection("Pessoas");
    		MongoCursor<Document> pessoasCursor = pessoas.find(eq("name", name)).iterator();
			while (pessoasCursor.hasNext()) {
				Document p = pessoasCursor.next();
				if (p.containsKey("paciente_id")) {
					Document paciente = pacientes.find(eq("_id", p.getObjectId("paciente_id"))).first();
					if (paciente.containsKey("ff") && paciente.getString("ff").equals(ff)) {
						pct = paciente;
					}
				}
			}
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	return pct;
    }
    
    private static Document getGestanteBySIS(Long sis) {
    	Document gestante = null;
    	try (MongoClient mongoClient = new MongoClient(connectionString)) {
    		MongoDatabase cna = mongoClient.getDatabase("ControleNovaAmerica");
    		MongoCollection<Document> gestantes = cna.getCollection("Gestantes");
    		gestante = gestantes.find(eq("SIS", sis)).first();
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	return gestante;
    }
}
