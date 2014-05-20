/*
 * Created on 30 sept. 2009
 *
 */
package reprise;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.util.Properties;

import nc.mairie.technique.reprise.DB2Connection;
import nc.mairie.technique.reprise.Log;
import nc.mairie.technique.reprise.ObjetPasserelle;
import nc.mairie.technique.reprise.OracleConnection;


/**
 * @author boulu72
 *
 */
public class Passerelle {
	
	public Properties properties = null;
	public Log log = null; 
	
	public Passerelle() {
		super();
		log= new Log(this);
	}
	
	public void passerelleBanque () throws Exception {
		
		OracleConnection oracleConnection = new OracleConnection(log, "ORACLE", properties);
		DB2Connection db2Connection = new DB2Connection(log, "DB2", properties);
		
		//Rapatriement de VBANQUE
		try {
			String champsOracle [] = {"COD_BANQUE","substr(LIB_BANQUE, 1 , 30)"};
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "OG.V_BANQUE", champsOracle);
		
			String champsDb2 [] = {"COD_BANQUE","LIB_BANQUE"};;
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "dofin.V_BANQUE", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,null, true);

		} catch (Exception e) {
			
			throw e;
		}
		
		// Copie de VBANQUE dans SIBANQ 
		try {
			String champsOrg [] = {"COD_BANQUE","LIB_BANQUE", "0"};
			ObjetPasserelle org = new ObjetPasserelle(log,db2Connection, "dofin.V_BANQUE", champsOrg);
			
			String champsDest [] = {"CDBANQ","LIBANQ", "MDREGL"};
			ObjetPasserelle dest = new ObjetPasserelle(log,db2Connection, "mairie.sibanq", champsDest);
			
			org.CopieTable(dest, 
					"where cod_banque not in (select cdbanq from mairie.sibanq)", false);
		} catch (Exception e) {
			
			throw e;
		}
		
		//Rapatriement de V_GUICHE
		try {
			String champsOracle [] = {"COD_BANQUE","COD_GUICHET","LIB_GUICHET"};
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "OG.V_GUICHE", champsOracle);
		
			String champsDb2 [] = champsOracle;
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "dofin.V_GUICHE", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,null, true);

		} catch (Exception e) {
			
			throw e;
		}
		
		// Copie de VB_GUICHE dans SIGUIC 
		try {
			String champsOrg [] = {"0","COD_BANQUE","COD_GUICHET","LIB_GUICHET"};
			ObjetPasserelle org = new ObjetPasserelle(log,db2Connection, "dofin.V_GUICHE", champsOrg);
			
			String champsDest [] = {"IDINDI","CDBANQ","CDGUIC", "LIGUIC"};
			ObjetPasserelle dest = new ObjetPasserelle(log,db2Connection, "mairie.siguic", champsDest);
			
			org.CopieTable(dest, 
					"where (cod_banque, cod_guichet) not in (select cdbanq, cdguic from mairie.siguic)", false);
		} catch (Exception e) {
			
			throw e;
		}	
		
		//on close tout
		db2Connection.getConnection().commit();
		db2Connection.getConnection().close();
		oracleConnection.getConnection().commit();
		oracleConnection.getConnection().close();
		
	}
		
	public void passerelleGFCADCPT () throws Exception {

		OracleConnection oracleConnection = new OracleConnection(log, "ORACLE", properties);
		DB2Connection db2Connection = new DB2Connection(log, "DB2", properties);
		
		//Rapatriement de GFCADCPT VDN
		try {
			//String champsOracle [] = {"NUM_ENV","NUM_CHAP","NUM_ART","case when NUM_PRG is null then ' ' else NUM_PRG END","' '"};
			//LUC	A REMETTRE EN 2010 !!!!! String champsOracle [] = {"NUM_ENV","NUM_CHAP","NUM_ART","case when cod_crit5 is null then ' ' else substr(cod_crit5, 1, length(cod_crit5) -1) END","LIB_ENV"};
			String champsOracle [] = {"max(NUM_ENV)","NUM_CHAP","NUM_ART","case when cod_crit5 is null then ' ' else substr(cod_crit5, 1, length(cod_crit5) -1) END","max(LIB_ENV)"};
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "GF.CAD_CPT", champsOracle);
		
			String champsDb2 [] = {"NUMENV","CODFON","NUMCPTE","NOACTI","LIBCPTEA"};
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "MAIRIE.GFCADCPT", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,
					" where COD_COLL='VDN' " +
					" and NUM_EXBUDG= to_char(sysdate, 'yyyy')" +
					" and COD_BUDG='01' " +
					" and NUM_SCHAP is null " +
					" and TYP_MVT='D' " +
					" and COD_GEST='DRH'" +
					//" and COD_GEST='REPRISE'" +
					" and COD_SECTION ='F'" +
					//Demande de DEBFA le 13/10/11
					//" and (entete_m14 = '012' or entete_m14 = '65')" +
					" and entete_m14 in ('012','65','011')"+
//					LUC	A REMETTRE EN 2010 !!!!!" and COD_NATURE =  'R'", true);
					" and COD_NATURE =  'R' group by NUM_CHAP,NUM_ART,case when cod_crit5 is null then ' ' else substr(cod_crit5, 1, length(cod_crit5) -1) END", true);
		} catch (Exception e) {
			
			throw e;
		}

		//Rapatriement de GFCADCPT CDE
		try {
//			LUC	A REMETTRE EN 2010 !!!!! String champsOracle [] = {"NUM_ENV","NUM_CHAP","NUM_ART","case when cod_crit5 is null then ' ' else substr(cod_crit5, 1, length(cod_crit5) -1) END","LIB_ENV"};
			String champsOracle [] = {"max(NUM_ENV)","NUM_CHAP","NUM_ART","case when cod_crit5 is null then ' ' else substr(cod_crit5, 1, length(cod_crit5) -1) END","max(LIB_ENV)"};
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "GF.CAD_CPT", champsOracle);
		
			String champsDb2 [] = {"NUMENV","CODFON","NUMCPTE","NOACTI","LIBCPTEA"};
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "MAIRCDE.GFCADCPT", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,
					" where COD_COLL='CDE' " +
					//" where COD_COLL='VDN' " +
					" and NUM_EXBUDG= to_char(sysdate, 'yyyy')" +
					" and COD_BUDG='01' " +
					" and NUM_SCHAP is null " +
					" and TYP_MVT='D' " +
					" and COD_GEST='CDE'"+
					//" and COD_GEST='DRH'" +
					" and COD_SECTION ='F'" +
					" and (entete_m14 = '012' or entete_m14 = '65')" +
//					LUC	A REMETTRE EN 2010 !!!!!" and COD_NATURE =  'R'", true);
					" and COD_NATURE =  'R' group by NUM_CHAP,NUM_ART,case when cod_crit5 is null then ' ' else substr(cod_crit5, 1, length(cod_crit5) -1) END", true);

		} catch (Exception e) {
			
			throw e;
		}

		//on close tout
		db2Connection.getConnection().commit();
		db2Connection.getConnection().close();
		oracleConnection.getConnection().commit();
		oracleConnection.getConnection().close();
		
	}

	public void passerelleGFTIERS () throws Exception {
		OracleConnection oracleConnection = new OracleConnection(log, "ORACLE", properties);
		DB2Connection db2Connection = new DB2Connection(log, "DB2", properties);

		//Rapatriement de TIERS_DONGEN left join TIERS_RIB VDN
		try {
			//String champsOracle [] = {"NUM_ENV","NUM_CHAP","NUM_ART","case when NUM_PRG is null then ' ' else NUM_PRG END","' '"};
			String champsOracle [] ={"COD_TIERS","LIC_NOM_ENREG","case when td.COD_STATUT='VALIDE' then 'O' else 'N' END","COD_BANQUE","COD_GUICHET","NUM_CPTE","CLE_RIB","to_number(substr(COD_DOM,1,2))"};
			//ObjetReprise oracle = new ObjetReprise(ObjetReprise.ORACLE, "ASTGF.TIERS_DONGEN td inner join ASTGF.TIERS_RIB tr on td.tiers_id = tr.tiers_id and td.cod_org = tr.cod_org", champsOracle);
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "ASTGF.TIERS_DONGEN td inner join ASTGF.TIERS_RIB tr on td.tiers_id = tr.tiers_id", champsOracle);
		
			String champsDb2 [] = 	{"IDETBS","ENSCOM","CDETAC","CDBANQ","CDGUIC","NOCPTE","CLERIB","NUMDOM"};
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "MAIRIE.GFTIERS", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,
					" where td.COD_ORG = 'VDN'" +
					" and IND_FINANCIER = 1" +
					" and COD_BANQUE is not null"+
					" and IND_DEFAUT = 1", true);
		} catch (Exception e) {
			
			throw e;
		}

		//	Rapatriement de TIERS_DONGEN left join TIERS_RIB CDE
		try {
			//String champsOracle [] = {"NUM_ENV","NUM_CHAP","NUM_ART","case when NUM_PRG is null then ' ' else NUM_PRG END","' '"};
			String champsOracle [] ={"COD_TIERS","LIC_NOM_ENREG","case when td.COD_STATUT='VALIDE' then 'O' else 'N' END","COD_BANQUE","COD_GUICHET","NUM_CPTE","CLE_RIB","to_number(substr(COD_DOM,1,2))"};
			//ObjetReprise oracle = new ObjetReprise(ObjetReprise.ORACLE, "ASTGF.TIERS_DONGEN td inner join ASTGF.TIERS_RIB tr on td.tiers_id = tr.tiers_id and td.cod_org = tr.cod_org", champsOracle);
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "ASTGF.TIERS_DONGEN td inner join ASTGF.TIERS_RIB tr on td.tiers_id = tr.tiers_id", champsOracle);
		
			String champsDb2 [] = 	{"IDETBS","ENSCOM","CDETAC","CDBANQ","CDGUIC","NOCPTE","CLERIB","NUMDOM"};
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "MAIRCDE.GFTIERS", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,
					" where td.COD_ORG = 'CDE'" +
					" and IND_FINANCIER = 1" +
					" and COD_BANQUE is not null"+
					" and IND_DEFAUT = 1", true);
		} catch (Exception e) {
			
			throw e;
		}

		//	Rapatriement de TIERS_DONGEN left join TIERS_RIB CCAS
		try {
			//String champsOracle [] = {"NUM_ENV","NUM_CHAP","NUM_ART","case when NUM_PRG is null then ' ' else NUM_PRG END","' '"};
			String champsOracle [] ={"COD_TIERS","LIC_NOM_ENREG","case when td.COD_STATUT='VALIDE' then 'O' else 'N' END","COD_BANQUE","COD_GUICHET","NUM_CPTE","CLE_RIB","to_number(substr(COD_DOM,1,2))"};
			//ObjetReprise oracle = new ObjetReprise(ObjetReprise.ORACLE, "ASTGF.TIERS_DONGEN td inner join ASTGF.TIERS_RIB tr on td.tiers_id = tr.tiers_id and td.cod_org = tr.cod_org", champsOracle);
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "ASTGF.TIERS_DONGEN td inner join ASTGF.TIERS_RIB tr on td.tiers_id = tr.tiers_id", champsOracle);
		
			String champsDb2 [] = 	{"IDETBS","ENSCOM","CDETAC","CDBANQ","CDGUIC","NOCPTE","CLERIB","NUMDOM"};
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "MAIRCCAS.GFTIERS", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,
					" where td.COD_ORG = 'CCAS'" +
					" and IND_FINANCIER = 1" +
					" and COD_BANQUE is not null"+
					" and IND_DEFAUT = 1", true);
		} catch (Exception e) {
			
			throw e;
		}

		//on close tout
		db2Connection.getConnection().commit();
		db2Connection.getConnection().close();
		oracleConnection.getConnection().commit();
		oracleConnection.getConnection().close();
		
	}

	public void passerelleLIQD (String codcol) throws Exception {
		OracleConnection oracleConnection = new OracleConnection(log, "ORACLE", properties);
		DB2Connection db2Connection = new DB2Connection(log, "DB2", properties);

		//Lecture de la propriété auto si LIQD_AUTO = ON alors 1 sinn 0
		String LIQD_AUTO =  properties.getProperty("LIQD_AUTO");
		if (LIQD_AUTO == null) {
			log("IMPOSSIBLE de lire LIQD_AUTO dans le fichier properties. Par défaut à 0");
			LIQD_AUTO = "0";
		} else LIQD_AUTO = LIQD_AUTO.equals("ON") ? "1" : "0";
		
		//Rapatriement de GF.INT_LIQD
		try {
			String champsDb2 [] = 	{"'"+codcol+"'",
					"EXERCI",
					"'01'",
					"NOMANDAT",
					"NOORDRE",
					"0"
					,"'RH'"
					,"'C'"
					,"substring(OBJCPT,1,40)"
					,"REFEMP"
					,"LDFMDT"
					,"MTLMANDAT",
					"julian_day(substr(char(DTMANDAT),7,2) || '.' || substr(char(DTMANDAT),5,2) || '.' || substr(char(DTMANDAT),1,4)) "
					,"1",
					"NUMENV",
					"repeat('0',6-length(varchar(idetbs))) || varchar(idetbs)",
					"NUMDOM",
					"'O'"
					,"99"
					,"9999999999"
					,"'M'"};
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, 
					codcol.equals("CDE") ? "MAIRCDE.GFMANP" : "MAIRIE.GFMANP", 
					champsDb2);

			String champsOracle [] ={"COD_COLL",
					"NUM_EXBUDG",
					"COD_BUDG"
					,"NUM_LDC",
					"NUM_LIG_LDC",
					"IND_NIV_TRAIT"
					,"Cod_module"
					,"typ_mvt_liq"
					,"obj_ldc"
					,"lib_comp1_ldc"
					,"idf_mdt"
					,"Mnt_ttc_ldc"
					,"dat_ech_ldc"
					,"typ_edit"
					,"num_env"
					,"num_tiers"
					,"num_dom_tiers"
					,"ind_maj"
					,"typ_nomenc_mar"
					,"cod_nomenc_mar"
					,"TYP_LIQ"};
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "GF.INT_LIQD", champsOracle);
		
			//On rapatrie 
			db2.CopieTable(oracle,null, false);

			//Recherche du param 
			String LIQD_CM_INTRO =  properties.getProperty("LIQD_CM_INTRO");
			if (LIQD_CM_INTRO == null) {
				log("IMPOSSIBLE de lire LIQD_CM_INTRO dans le fichier properties. Par défaut à OFF");
				LIQD_CM_INTRO = "OFF";
			}
			
			if ("ON".equals(LIQD_CM_INTRO)) {
				
				//Mise à jour de la table Cm.t_intro 
				log("Alimentation de Cm.t_intro");
				
				//String user = "'ASTRE'";
				String user = "'"+System.getProperty("user.name").trim()+"'";
				
				String req = "insert into Cm.t_intro (" +
						" num_ordre,dat_dem,  heu_dem,  idf_util,  cod_appl,  lib_nomutil,  cod_coll, " +
						" ind_trait_imme,  nbr_exempl,  cod_imp,  num_police,  dsc_param1,  dsc_param2, " +
						" demandeur_id , job_id  )" +
						" values (" +
						" (SELECT nvl(MAX(num_ordre),0)+10" +
						"		FROM cm.t_intro" +
						"		WHERE dat_dem=TO_NUMBER(TO_CHAR(sysdate,'J')))  ," +
						" (select TO_NUMBER(TO_CHAR(sysdate,'J')) from dual), " +
						" (select TO_NUMBER(TO_CHAR(sysdate,'SSSSS'))from dual)," +
						" ? ," + //************user
						" 'GFINTDEP'," +
						" ? ," + //************user
						" ?," + //**************** 'VDN'
						" ?," + //*********LIQD_AUTO
						" 1," +
						" (SELECT SUBSTR(val_char,1,instr(val_char,'!',1,1)-1)FROM gf.param     " +
						" 		WHERE nvl(cod_coll,'-1')=?    " + //**************** 'VDN'
						" 		AND identifiant='NOUMEA_RH'), " +
						" (select to_number(SUBSTR(val_char,INSTR(val_char,'!',1,1)+1,    INSTR(val_char,'!',1,2)-INSTR(val_char,'!',1,1)-1)) " +
						"    FROM gf.param " +
						"    WHERE nvl(cod_coll,'-1')=?" + //**************** 'VDN'
						"    AND identifiant='NOUMEA_RH'), " +
						" (select 'GFINTDEP!!'|| TO_NUMBER(TO_CHAR(sysdate,'J'))||'!'||  " +
						"		TO_NUMBER(TO_CHAR(sysdate,'SSSSS'))||'!'|| 'RH!N!N!'|| " +
						" 		? ||" + //**************** 'CHARG'
						" 		'!'" + 
						"		from sys.dual)," +
						" ? ||" + //**************** 'VDN'
						"		'!'," + 
						" ? , " + //**************** user 
						" 0)";
				
						PreparedStatement ps=null;
						try {
							ps = oracle.getConnection().prepareStatement(req);
							
							String toto = "TRAIT";
							
							toto = "TRAIT";
							ps.setString(1,user);
							ps.setString(2,user);
							ps.setString(3,codcol);
							ps.setString(4,LIQD_AUTO);
							ps.setString(5,codcol);
							ps.setString(6,codcol);
							ps.setString(7,toto);
							ps.setString(8,codcol);
							ps.setString(9,user);
							ps.execute();
							
							toto = "CHARG";
							ps.setString(1,user);
							ps.setString(2,user);
							ps.setString(3,codcol);
							ps.setString(4,LIQD_AUTO);
							ps.setString(5,codcol);
							ps.setString(6,codcol);
							ps.setString(7,toto);
							ps.setString(8,codcol);
							ps.setString(9,user);
							ps.execute();
							
							toto = "RETEN";
							ps.setString(1,user);
							ps.setString(2,user);
							ps.setString(3,codcol);
							ps.setString(4,LIQD_AUTO);
							ps.setString(5,codcol);
							ps.setString(6,codcol);
							ps.setString(7,toto);
							ps.setString(8,codcol);
							ps.setString(9,user);
							ps.execute();
						} catch (Exception e) {
							throw e;
						} finally {
							ps.close();
						}
						//ps.executeBatch();
			} else {
				//Mise à jour de la table Cm.t_intro 
				log("PAS D'ALIMENTATION de Cm.t_intro");
			}
					
		} catch (Exception e) {
			
			throw e;
		} finally {

			//on close tout
			db2Connection.getConnection().commit();
			db2Connection.getConnection().close();
			oracleConnection.getConnection().commit();
			oracleConnection.getConnection().close();
		}
		
	}

	public void passerelleSEAT () throws Exception {
		OracleConnection oracleConnection = new OracleConnection(log, "ORACLE", properties);
		DB2Connection db2Connection = new DB2Connection(log, "DB2", properties);

		//Rapatriement de VBANQUE
		try {
			String champsOracle [] = {"COD_EXEBUD", "TIERS_ID", "IDE_COM_COM", "NUM_COM", "COM_LIG_ID", "REF_COM_LIG", "MNT_TTC_COM_LIG"};
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, 
					"astgf.COMMANDE com inner join astgf.COM_LIG lig on com.COMMANDE_ID = lig.COMMANDE_ID", champsOracle);
		
			String champsDb2 [] = {"EXERCI","IDETBS","ENSCOM","NOENGJ","NLENGJU","CDDEP","MTLENJU"};   
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, "SEAT.F_ENJU_ASTRE", champsDb2);
			
			//On rapatrie V_BANQUE
			oracle.CopieTable(db2,
					"where com.COD_ORG = 'VDN' and com.cod_bud = '01' and com.SER_DES = 1078", true);

		} catch (Exception e) {
			
			throw e;
		}
		
		
		//on close tout
		db2Connection.getConnection().commit();
		db2Connection.getConnection().close();
		oracleConnection.getConnection().commit();
		oracleConnection.getConnection().close();
		
	}

	public void passerelleTEST () throws Exception {
		OracleConnection oracleConnection = new OracleConnection(log, "ORACLE", properties);
		DB2Connection db2Connection = new DB2Connection(log, "DB2", properties);

		String codcol="VDN";
		
		//Rapatriement de GF.INT_LIQD
		try {
			String champsDb2 [] = 	{"'"+codcol+"'",
					"EXERCI",
					"'01'",
					"NOMANDAT",
					"NOORDRE",
					"0"
					,"'RH'"
					,"'C'"
					,"substring(OBJCPT,1,40)"
					,"REFEMP"
					,"LDFMDT"
					,"MTLMANDAT",
					"julian_day(substr(char(DTMANDAT),7,2) || '.' || substr(char(DTMANDAT),5,2) || '.' || substr(char(DTMANDAT),1,4)) "
					,"1",
					"NUMENV",
					"repeat('0',6-length(varchar(idetbs))) || varchar(idetbs)",
					"NUMDOM",
					"'O'"
					,"99"
					,"9999999999"
					,"'M'"};
			ObjetPasserelle db2 = new ObjetPasserelle(log,db2Connection, 
					codcol.equals("CDE") ? "MAIRCDE.GFMANP" : "MAIRIE.GFMANP", 
					champsDb2);

			String champsOracle [] ={"COD_COLL",
					"NUM_EXBUDG",
					"COD_BUDG"
					,"NUM_LDC",
					"NUM_LIG_LDC",
					"IND_NIV_TRAIT"
					,"Cod_module"
					,"typ_mvt_liq"
					,"obj_ldc"
					,"lib_comp1_ldc"
					,"idf_mdt"
					,"Mnt_ttc_ldc"
					,"dat_ech_ldc"
					,"typ_edit"
					,"num_env"
					,"num_tiers"
					,"num_dom_tiers"
					,"ind_maj"
					,"typ_nomenc_mar"
					,"cod_nomenc_mar"
					,"TYP_LIQ"};
			ObjetPasserelle oracle = new ObjetPasserelle(log,oracleConnection, "GF.INT_LIQD", champsOracle);
		
			//On rapatrie 
			db2.CopieTable(oracle,null, false);
			
			//Mise à jour de la table Cm.t_intro 

			log("Alimentation de Cm.t_intro");
			
			//String user = "'ASTRE'";
			String user = "'DEBFA66'";
			
			String req = "insert into Cm.t_intro (" +
					" num_ordre,dat_dem,  heu_dem,  idf_util,  cod_appl,  lib_nomutil,  cod_coll, " +
					" ind_trait_imme,  nbr_exempl,  cod_imp,  num_police,  dsc_param1,  dsc_param2, " +
					" demandeur_id , job_id  )" +
					" values (" +
					" (SELECT nvl(MAX(num_ordre),0)+10" +
					"		FROM cm.t_intro" +
					"		WHERE dat_dem=TO_NUMBER(TO_CHAR(sysdate,'J')))  ," +
					" (select TO_NUMBER(TO_CHAR(sysdate,'J')) from dual), " +
					" (select TO_NUMBER(TO_CHAR(sysdate,'SSSSS'))from dual)," +
					" ? ," +  //************** user
					" 'GFINTDEP'," +
					" ?  ," + //************** user
					" ?," + //**************** 'VDN'
					" '0'," +
					" 1," +
					" (SELECT SUBSTR(val_char,1,instr(val_char,'!',1,1)-1)FROM gf.param     " +
					" 		WHERE nvl(cod_coll,'-1')=?    " + //**************** 'VDN'
					" 		AND identifiant='NOUMEA_RH'), " +
					" (select to_number(SUBSTR(val_char,INSTR(val_char,'!',1,1)+1,    INSTR(val_char,'!',1,2)-INSTR(val_char,'!',1,1)-1)) " +
					"    FROM gf.param " +
					"    WHERE nvl(cod_coll,'-1')=?" + //**************** 'VDN'
					"    AND identifiant='NOUMEA_RH'), " +
					" (select 'GFINTDEP!!'|| TO_NUMBER(TO_CHAR(sysdate,'J'))||'!'||  " +
					"		TO_NUMBER(TO_CHAR(sysdate,'SSSSS'))||'!'|| 'RH!N!N!'|| " +
					" 		? ||" + //**************** 'CHARG'
					" 		'!'" + 
					"		from sys.dual)," +
					" ? ||" + //**************** 'VDN'
					"		'!'," + 
					" ? , " +  //**************user
					" 0)";
			
					PreparedStatement ps=null;
					try {
						ps = oracle.getConnection().prepareStatement(req);
						
						String toto = "TRAIT";
						
						toto = "TRAIT";
						ps.setString(1,user);
						ps.setString(2,user);
						ps.setString(3,codcol);
						ps.setString(4,codcol);
						ps.setString(5,codcol);
						ps.setString(6,toto);
						ps.setString(7,codcol);
						ps.setString(8,user);
						ps.execute();
						
						toto = "CHARG";
						ps.setString(1,user);
						ps.setString(2,user);
						ps.setString(3,codcol);
						ps.setString(4,codcol);
						ps.setString(5,codcol);
						ps.setString(6,toto);
						ps.setString(7,codcol);
						ps.setString(8,user);
						ps.execute();
					} catch (Exception e) {
						throw e;
					} finally {
						ps.close();
					}
					
					//ps.executeBatch();
					
		} catch (Exception e) {
			
			throw e;
		} finally {
			//on close tout
			db2Connection.getConnection().commit();
			db2Connection.getConnection().close();
			oracleConnection.getConnection().commit();
			oracleConnection.getConnection().close();
		
		}
	}

	public void afficherSyntaxe() {
		log("Passer en paramètre 1 2 3 4");
		log("1 : passerelleBanque");
		log("2 : passerelleGFCADCPT");
		log("3 : passerelleGFTIERS");
		log("4 : passerelleLIQD");
		log("5 : passerelleSEAT");
		}
	
	
	public void run(String args[]) throws Exception {
		//init
		init();
		
		//Si 4 
		if (args[0].equals("4")) {
			//Si pas de 2ieme param
			if (args.length == 1) {
				log("Il manque le paramètre VDN ou CDE");
				System.exit(1);
			}
			// Si pas VDN ou pas CDE
			if ( ! (args[1].equals("VDN") || args[1].equals("CDE"))) {
				log("Il manque le paramètre VDN ou CDE");
				System.exit(1);
			}
		}
		
		
		switch (args[0].charAt(0)) {
		case '1':
			log("Lancement de passerelleBanque");
			passerelleBanque();			
			break;
		case '2':
			log("Lancement de passerelleGFCADCPT");
			passerelleGFCADCPT();
			break;
		case '3':
			log("Lancement de passerelleGFTIERS");
			passerelleGFTIERS();
			break;
		case '4':
			log("Lancement de passerelleLIQD avec "+args[1]);
			passerelleLIQD(args[1]);
			break;
		case '5':
			log("Lancement de passerelleSEAT");
			passerelleSEAT();
			break;
		case '6':
			log("Test");
			passerelleTEST();
			break;
		default:
			log(args[0].charAt(0)+" est un paramètre incorrect") ;
			afficherSyntaxe();
			System.exit(1);
		}
	
	}
	
	public static void main (String args[])  {
		Passerelle passerelle = new Passerelle();
		try {
			if (args == null || args.length == 0 || args[0].length() != 1) {
				passerelle.afficherSyntaxe();
				System.exit(1);
			}

			//log démarrage
			String txt = "Démarrage de la passerelle par "+System.getProperty("user.name")+" avec "+args[0];
			
			StringBuffer sb = new StringBuffer(txt);
			
			for (int i = 1; i < args.length; i++) {
				sb.append(" et ");
				sb.append(args[i]);
			}
			txt=sb.toString();
			
			passerelle.log("--------------------------------------------------------");
			passerelle.log(txt);
			
			passerelle.run(args);
			passerelle.log("Fin normale");
		} catch (Exception e) {
			passerelle.log(e);
		}
	}
	
	public void log(String message) {
		log.log(message);
	}
	public void log(Exception e) {
		log.log(e);
	}

	public void init() {
	//	lecture des propriétés
		try {
			properties = new Properties();
			
			String root = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
			if (root.toUpperCase().endsWith("JAR") || root.toUpperCase().endsWith("CLASS") ) {
				root=root.substring(0, root.lastIndexOf('/') +1);
			}
			String className = getClass().getName().substring(getClass().getName().lastIndexOf(".")+1);
			
			InputStream is= null;
			try {
				is = new FileInputStream(root+className+".properties");
				properties.load(is);
			} catch (Exception e) {
				throw e;
			} finally {
				is.close();
			}
			log("Lecture des propriétés : "+properties);
	      
		} catch (Exception e) {
			log("Impossible de lire le fichier properties : "+e.getMessage());
			System.exit(1);
		}
	}
	
}
