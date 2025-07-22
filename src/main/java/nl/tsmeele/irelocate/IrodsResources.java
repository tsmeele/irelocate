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
						genOut.data[i][4], // resc_parent (= resc_id of parent)
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
	
	/**
	 * @param rescName name of resource to lookup
	 * @return resource the selected resource or null if the resource does not exist
	 */
	public Resource get(String rescName) {
		if (rescName == null) return null;
		return resources.get(rescName);
	}	
	
	/**
	 * @param rescid id of resource to lookup
	 * @return resource the selected resource or null if the resource does not exist
	 */
	private Resource getById(String rescId) {
		for (Resource r : resources.values()) {
			if (r.id.equals(rescId)) {
				return r;
			}
		}
		return null;
	}

	/**
	 * @param resc selected resource
	 * @return true if resource is a storage resource or a coordinating resource 
	 * with at least one storage resource in its hierarchy, otherwise false.
	 */
	public boolean hasStorageResource(Resource resc) {
		for (Resource r : expandToLeafs(resc)) {
			if (r.isStorageResource()) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * @param rescTree root resource of a tree
	 * @param resc selected resource
	 * @return true if selected resource is part of the tree (or its root), otherwise false.
	 */
	public boolean isInTree(Resource rescTree, Resource resc) {
		if (rescTree == null || resc == null) return false;
		if (resc == rescTree) return true;
		Resource parent = getById(resc.parent);
		return isInTree(rescTree, parent);
	}
	

	
	/**
	 * @param resc selected (root) resource
	 * @return list of resources that are the leafs in the tree referenced by the root resource. 
	 */
	public List<Resource> expandToLeafs(Resource resc) {
		List<Resource> out = new ArrayList<Resource>();
		// if resc has no children then just return this resc
		if (!parentResources.contains(resc.id)) {
			out.add(resc);
			return out;
		}
		// resc is parent, return its children
		for (Resource r : resources.values()) {
			// skip self to avoid loops
			if (r.name.equals(resc.name)) {
				continue;
			}
			// is this a direct child of resc? then add its leafs
			if (r.parent.equals(resc.id)) {
				out.addAll(expandToLeafs(r));
			}
		}
		return out;
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
