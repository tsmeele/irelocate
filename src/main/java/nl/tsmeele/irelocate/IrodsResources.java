package nl.tsmeele.irelocate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class IrodsResources {
	public HashMap<String,Resource> resources = new HashMap<String,Resource>();
	public List<String> parentResources = new ArrayList<String>();
	
	public IrodsResources(Hirods hirods) throws MyRodsException, IOException {
			if (!hirods.isAuthenticated()) throw new RuntimeException("iRODS connection not authenticated");
			String host = hirods.getHost().toLowerCase();
			// collect properties of all resources in the data grid
			// select clause
			InxIvalPair inxIvalPair = new InxIvalPair();
			inxIvalPair.put(Columns.RESC_ID.getId(), Flag.SELECT_NORMAL);	
			inxIvalPair.put(Columns.RESC_NAME.getId(), Flag.SELECT_NORMAL);	
			inxIvalPair.put(Columns.RESC_LOC.getId(), Flag.SELECT_NORMAL);
			// known types:  "random", "passthru", "unixfilesystem"
			inxIvalPair.put(Columns.RESC_TYPE_NAME.getId(), Flag.SELECT_NORMAL);	
			inxIvalPair.put(Columns.RESC_PARENT.getId(), Flag.SELECT_NORMAL);	
			// where clause - empty
			InxValPair inxValPairColl = new InxValPair();

			int maxRows = 256;

			// query the resources
			GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
					new KeyValPair(), inxIvalPair , inxValPairColl);
			Iterator<GenQueryOut> it = hirods.genQueryIterator(genQueryInp);
			while (it.hasNext()) {
				GenQueryOut genOut = it.next();
				for (int i = 0; i < genOut.rowCount; i++) {
					Resource resc = new Resource(
						genOut.data[i][0], // resc_id
						genOut.data[i][1], // resc_name
						genOut.data[i][2], // resc_loc
						genOut.data[i][3], // resc_type_name
						genOut.data[i][4], // resc_parent
						// infer if resource is located on iRODS host itself
						// TODO: should do dns lookup and compare ip addresses
						genOut.data[i][2].toLowerCase().equals(host) ||
						genOut.data[i][2].toLowerCase().equals("localhost"));
					resources.put(resc.name, resc);
					if (!resc.parent.equals("")) {
						parentResources.add(resc.parent);
					}
				}
			}
	}
	
	public Resource get(String rescName) {
		return resources.get(rescName);
	}
	
	public boolean exists(String rescName) {
		return rescName != null && resources.get(rescName) != null;
	}

	public boolean isValid(String rescName) {
		if (rescName == null) return false;
		Resource resc = resources.get(rescName);
		return resc != null && !resc.name.equals("bundleResc");
	}
	
	public boolean isUnixFileSystem(String rescName) {
		if (rescName == null) return false;
		Resource resc = resources.get(rescName);
		return resc != null && resc.type.toLowerCase().equals("unixfilesystem") && !resc.name.equals("bundleResc");
	}

	public boolean isStandalone(String rescName) {
		if (rescName == null) return false;
		Resource resc = resources.get(rescName);
		return resc != null && resc.parent.equals("") && !parentResources.contains(resc.name);
	}
	
	public boolean isLeaf(String rescName) {
		return isUnixFileSystem(rescName) && !parentResources.contains(rescName);
	}
	
	public boolean isInHierarchy(String rescTree, String rescName) {
		if (rescName == null) return false;
		Resource resc = resources.get(rescName);
		if (resc == null || !exists(rescTree)) {
			return false;
		}
		if (rescName.equals(rescTree)) {
			return true;
		}
		if (resc.parent.equals("")) {
			return false;
		}
		return isInHierarchy(rescTree, resc.parent);
	}
	
	

	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String key : resources.keySet()) {
			if (first) {
				first = false;
			} else {
				sb.append("\n");
			}
			sb.append(resources.get(key).toString());
		}
		return sb.toString();
	}
	
}
