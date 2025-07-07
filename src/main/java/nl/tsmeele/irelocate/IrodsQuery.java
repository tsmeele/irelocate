package nl.tsmeele.irelocate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import nl.tsmeele.myrods.api.Columns;
import nl.tsmeele.myrods.api.Flag;
import nl.tsmeele.myrods.api.GenQueryInp;
import nl.tsmeele.myrods.api.GenQueryOut;
import nl.tsmeele.myrods.api.InxIvalPair;
import nl.tsmeele.myrods.api.InxValPair;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class IrodsQuery {

	public static List<String> dataObjectsOnResources(Hirods hirods, List<String> sourceResources, long startDataId)
			throws MyRodsException, IOException {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String resc : sourceResources) {
			if (first) {
				first = false;
			} else {
				sb.append(",");
			}
			sb.append("'" + resc + "'");
		}
		String quotedResourceList = sb.toString();
		
		// SELECT clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.DATA_ID.getId(), Flag.SELECT_NORMAL | Flag.ORDER_BY);

		// WHERE clause
		InxValPair inxValPair = new InxValPair();
		inxValPair.put(Columns.DATA_RESC_NAME.getId(), "in (" + quotedResourceList + ")");
		if (startDataId != 0L) {
			inxValPair.put(Columns.DATA_ID.getId(), ">= '" + startDataId + "'");
		}

		int maxRows = 256;
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0, new KeyValPair(), inxIvalPair, inxValPair);
		Iterator<GenQueryOut> it = hirods.genQueryIterator(genQueryInp);
		List<String> out = new ArrayList<String>();
		while (it.hasNext()) {
			GenQueryOut genOut = it.next();
			for (int i = 0; i < genOut.rowCount; i++) {
				String dataId = genOut.data[i][0]; // data_id
				out.add(dataId);
			}
		}
		return out;
	}

	public static List<Replica> getReplicas(Hirods hirods, String dataId) throws MyRodsException, IOException {
		// SELECT clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.DATA_ID.getId(), Flag.SELECT_NORMAL | Flag.ORDER_BY);
		inxIvalPair.put(Columns.COLL_NAME.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_NAME.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_REPL_NUM.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_REPL_STATUS.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_SIZE.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_CHECKSUM.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_RESC_NAME.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_PATH.getId(), Flag.SELECT_NORMAL);
		// WHERE clause
		InxValPair inxValPair = new InxValPair();
		inxValPair.put(Columns.DATA_ID.getId(), "= '" + dataId + "'");

		int maxRows = 256;
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0, new KeyValPair(), inxIvalPair, inxValPair);
		Iterator<GenQueryOut> it = hirods.genQueryIterator(genQueryInp);
		List<Replica> out = new ArrayList<Replica>();
		while (it.hasNext()) {
			GenQueryOut genOut = it.next();
			for (int i = 0; i < genOut.rowCount; i++) {
				Replica r = new Replica(genOut.data[i][0], // data_id
						genOut.data[i][1], // coll_name
						genOut.data[i][2], // data_name
						genOut.data[i][3], // data_repl_num
						genOut.data[i][4], // data_repl_status
						genOut.data[i][5], // data_size
						genOut.data[i][6], // data_checksum
						genOut.data[i][7], // data_resc_name
						genOut.data[i][8]); // data_path
				out.add(r);
			}
		}
		return out;
	}

}
