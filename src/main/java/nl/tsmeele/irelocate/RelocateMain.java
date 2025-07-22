package nl.tsmeele.irelocate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import nl.tsmeele.log.Log;
import nl.tsmeele.log.LogLevel;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.high.IrodsUser;
import nl.tsmeele.myrods.plumbing.MyRodsException;

/**
 * iRelocate replicates data objects to a new resource and trims it at other (obsolete) resources 
 *
 */
public class RelocateMain {
	static Hirods hirods = null;
	static int processedObjectCount = 0;
	static RelocateContext ctx = new RelocateContext();
	static Queue<String> queue = new ConcurrentLinkedQueue<String>();
	
    public static void main( String[] args ) throws IOException
	{
		// analyze command line arguments
		try {
			ctx.processArgs(args);
		} catch (MyRodsException e) {
			System.err.println(e.getMessage() + "\n");
			ctx.usage = true;
		}
		// process command line options
		if (ctx.sourceList.isEmpty()) {
			Log.error("At least one source resource must be specified.\n");
			ctx.usage = true;
		}
		if (ctx.usage) {
			System.out.println(ctx.usage());
			System.exit(1);
		}
		if (ctx.verbose)
			Log.setLoglevel(LogLevel.INFO);
		if (ctx.debug) {
			Log.setLoglevel(LogLevel.DEBUG);
			String[] classFilter = { "nl.tsmeele.irelocate" };
			Log.setDebugOutputFilter(classFilter);
		}
		if (ctx.trim) {
			Log.info("TRIM option specified: will trim data from source resources (only if also exists on destination)");
		}
		
		// log in and assert that we are a rodsadmin type user
		IrodsUser user = new IrodsUser(ctx.userName, ctx.zone);
		hirods = rodsAdminLogin(ctx.host, ctx.port, user, ctx.password, ctx.authPam);
		if (hirods == null) {
			System.exit(2);
		}
		Log.debug("Logged in as " + user.nameAndZone() + " (rodsadmin)");
		
		// collect information on all resources in the data grid
		ctx.rescList = new IrodsResources(hirods);
		Log.debug(ctx.rescList.toString());
		
		/* assert that the destination resource exists
		 * and directly/indirectly refers to a resource that contains a storage resource 
		 */
		Resource destResc = ctx.rescList.get(ctx.destinationResource);
		if (destResc == null || !ctx.rescList.hasStorageResource(destResc)) {
			errorExit(ctx.destinationResource, "does not exist or is invalid destination resource");
		}
		
		// expand source list to include all (if any) leafs of coordinating source resources
		HashSet<Resource> sources = new HashSet<Resource>();
		for (String rescName : ctx.sourceList) {
			Resource resc = ctx.rescList.get(rescName);
			if (resc == null) {
				errorExit(rescName, "source resource does not exist");
			}
			// note that leafs may be empty coordinating resources
			List<Resource> expanded = ctx.rescList.expandToLeafs(resc);
			if (expanded.size() != 1 || expanded.get(0) != resc) {
				List<String> expandedStr = expanded.stream().map(r->r.name).collect(Collectors.toList());
				Log.debug(rescName + " expanded to " + expandedStr.toString());
			} 
			sources.addAll(expanded);
		}

		// assert source resources meet our needs
		for (Resource resc : sources) {
			// indicate to user if we have derived the resource
			String expanded = "";
			if (!ctx.sourceList.contains(resc.name)) {
				expanded = " (using expanded source list)";
			}
			// source must be a storage type resource
			if (!resc.isStorageResource()) {
				errorExit(resc.name, "is not a valid (source) storage type resource" + expanded );
			}
			// source may not overlap with destination
			if (ctx.rescList.isInTree(destResc, resc) ||
				ctx.rescList.isInTree(resc,  destResc)) {
				errorExit(resc.name, "source resource may not overlap with destination resource" + expanded);
			}
		}
		
		// save expanded list as source resources
		ctx.sourceList = sources.stream().map(r -> r.name).collect(Collectors.toList());

		// find all data objects with one or more replicas on source resources
		// if specified, filter out data objects with a data id less than startDataID
		List<String> objs = IrodsQuery.dataObjectsOnResources(hirods, ctx.sourceList, ctx.startDataId);
		Log.debug("Found " + objs.size() + " matching data objects");

		// done with preparation
		hirods.rcDisconnect();	
		
		// are there any objects to process?
		if (objs.isEmpty()) {
			System.out.println("No processing needed (object list empty) for selected source resources and data object range.");
			System.exit(0);
		}
		
		
		// start a new log
		ctx.log = new LogFile(ctx.logFile);
		
		// initiate processing of selected data objects
		queue.addAll(objs);

		// create threads for parallel processing
		System.out.println("Start processing data objects using " + ctx.threads + " threads");
		ArrayList<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < ctx.threads; i++) {
			Thread t = new Thread(new DataObjectProcessor(i, ctx, queue));
			threads.add(t);
			t.start();
		}
		
		
		
	}
    
    public static void errorExit(String rescName, String errorMessage) throws MyRodsException, IOException {
    	Log.error("'" + rescName + "' " + errorMessage);
		hirods.rcDisconnect();
		System.exit(3);
    }
    
    
    public static Hirods rodsAdminLogin(String host, int port, IrodsUser user, String password, boolean authPam)  {
		Hirods hirods = new Hirods(host, port);
		boolean success = false;
		try {
			if (authPam) {
				success = hirods.pamLogin(user.name, user.zone, password, user.name, user.zone);
			} else {
				success = hirods.nativeLogin(user.name, user.zone, password, user.name, user.zone);
			}
			if (success) {
				// also make sure that user is a rodsadmin
				String userType = hirods.getUserType(user.name, user.zone);
				if (userType != null && userType.equals("rodsadmin")) {
					return hirods;
				} else {
					Log.error("User '" + user.nameAndZone() + "' lacks rodsadmin privileges on host " + host);
					success = false;
				}
			} else { 
				Log.error("Unable to connect and/or login to " + host + " as " + user.nameAndZone() + " iRODS error: " + hirods.intInfo);
			}
			// rodsAdminLogin failed, attempt to clean up the connection
			hirods.rcDisconnect();
		} catch (IOException e) { 
			Log.error(e.getMessage());
		}
		return null;
	}
   
    
}
