package nl.tsmeele.irelocate;

import java.io.IOException;
import nl.tsmeele.myrods.api.DataObjInp;
import nl.tsmeele.myrods.api.ExecMyRuleInp;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.api.Kw;
import nl.tsmeele.myrods.api.MsParam;
import nl.tsmeele.myrods.api.MsParamArray;
import nl.tsmeele.myrods.api.RHostAddr;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.irodsStructures.DataPtr;
import nl.tsmeele.myrods.irodsStructures.DataStruct;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class Replica {
	private final static String GOOD = "1";
	private final static String STALE = "0";
	public String dataId, collName, dataName, dataReplNum, dataReplStatus, dataSize, dataChecksum, dataRescName, dataPath;
	public String path;
	
	public Replica(String dataId, String collName, String dataName, String dataReplNum, String dataReplStatus, 
			String dataSize, String dataChecksum, String dataRescName, String dataPath) {
		this.dataId = dataId;
		this.collName = collName;
		this.dataName = dataName;
		this.path = collName + "/" + dataName;
		this.dataReplNum = dataReplNum;
		this.dataReplStatus = dataReplStatus;
		this.dataSize = dataSize;
		this.dataChecksum = dataChecksum;
		this.dataRescName = dataRescName;
		this.dataPath = dataPath;
	}
	
	public boolean replicate(Hirods hirods, String destResource, boolean doChecksum) throws MyRodsException, IOException {
		KeyValPair condInput = new KeyValPair();
		condInput.put(Kw.ADMIN_KW, "");
		condInput.put(Kw.REPL_NUM_KW, dataReplNum);	// source replica
		condInput.put(Kw.DEST_RESC_NAME_KW, destResource);
		if (doChecksum) {
			if (!dataChecksum.equals("")) {
				// also ensure that checksum of destination replica matches the ICAT stored source replica checksum 
				condInput.put(Kw.VERIFY_CHKSUM_KW, "");
			} else {
				// just calculate and store the destination replica checksum in ICAT
				condInput.put(Kw.REG_CHKSUM_KW, "");
			}
		} else {
			condInput.put(Kw.NO_COMPUTE_KW, "");
		}
		DataObjInp dataObjInp = new DataObjInp(path, condInput);
		hirods.rcDataObjRepl(dataObjInp);
		return !hirods.error;
	}

	public boolean trim(Hirods hirods) throws MyRodsException, IOException {
		KeyValPair condInput = new KeyValPair();
		condInput.put(Kw.ADMIN_KW, "");
		condInput.put(Kw.COPIES_KW, "1");
		condInput.put(Kw.REPL_NUM_KW, dataReplNum);	// source replica to trim
		DataObjInp dataObjInp = new DataObjInp(path, condInput);
		hirods.rcDataObjTrim(dataObjInp);
		return !hirods.error;
	}
	
	public int retrieveDatafileStatus(Hirods hirods) throws MyRodsException, IOException {
		// construct rule and its input args
		RHostAddr rHostAddr = new RHostAddr("", "", 0, 0);
		MsParam inputVar1 = new MsParam("*rescName", dataRescName);
		MsParam inputVar2 = new MsParam("*dataPath", dataPath);
		MsParam inputVar3 = new MsParam("*replicaSize", dataSize);
		MsParamArray msParamArray = new MsParamArray(0);
		msParamArray.add(inputVar1);
		msParamArray.add(inputVar2);
		msParamArray.add(inputVar3);
		String outParamDesc = "ruleExecOut";
		outParamDesc = outParamDesc.concat("%*result");
		String myRule = "@external rule " + 
			"{" +
				"*fileType = \"\";" +
				"*fileSize = \"\";" +
				"*result = msi_stat_vault(*rescName, *dataPath, *fileType, *fileSize);" +
				"if (*result < 0 || *fileType != \"FILE\") {" +
					"*result = -1;" +
				"} else {" +
					"if (*fileSize != *replicaSize) {" +
						"*result = 0;" +
					"} else {" +
						"*result = 1;" +
					"}" +
				"}" +
			"}";
		KeyValPair condInput = new KeyValPair();
		condInput.put(Kw.INSTANCE_NAME_KW, "irods_rule_engine_plugin-irods_rule_language-instance");
		ExecMyRuleInp ruleInp = new ExecMyRuleInp(myRule, rHostAddr, condInput, outParamDesc, msParamArray);
		
		// execute rule and interpret result
		MsParamArray out = hirods.rcExecMyRule(ruleInp);
		if (hirods.error || out == null) {
			return hirods.intInfo;
		}
		MsParam param = (MsParam) ((DataPtr)out.get(2)).get();
		DataStruct d = param.getParamContent();
		Integer result = d.lookupInt("myInt");
		return result;
	}
	
	public boolean isGood() {
		return dataReplStatus.equals(GOOD);
	}
	
	public boolean isStale() {
		return dataReplStatus.equals(STALE);
	}
	
	public String toString() {
		return dataReplNum + "-" + dataRescName + ":" + path; 
	}
}
