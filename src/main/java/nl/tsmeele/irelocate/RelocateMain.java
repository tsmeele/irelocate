package nl.tsmeele.irelocate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
			Log.info("TRIM option specified: will trim data from source resources");
		}
		
		// log in and assert that we are a rodsadmin type user
		IrodsUser user = new IrodsUser(ctx.userName, ctx.zone);
		Hirods hirods = rodsAdminLogin(ctx.host, ctx.port, user, ctx.password, ctx.authPam);
		if (hirods == null) {
			System.exit(1);
		}
		Log.debug("Logged in as " + user.nameAndZone() + " (rodsadmin)");
		
		// collect information on all resources in the data grid
		ctx.rescList = new IrodsResources(hirods);
		Log.debug(ctx.rescList.toString());
		
		/* assert that the destination resource exists
		 * and refers to a valid resource
		 */
		String errorMsg = null;
		if (ctx.rescList.isValid(ctx.destinationResource)) 
			errorMsg = "is an invalid destination resource";
		if (!ctx.rescList.exists(ctx.destinationResource)) 
			errorMsg = "does not exist";
		if (errorMsg != null) {
			Log.error("'" + ctx.destinationResource + "' " + errorMsg);
			hirods.rcDisconnect();
			System.exit(2);
		}
		
		/* assert that source resources refer to existing leaf resources
		 * and assert source resources do not overlap with destination resource
		 */
		for (String resc : ctx.sourceList) {
			if (!ctx.rescList.isLeaf(resc)) 
				errorMsg = "is a non-existing or invalid leaf resource";
			if (ctx.rescList.isInHierarchy(ctx.destinationResource, resc)) 
				errorMsg = "source resource must be different from destination resource";
			if (errorMsg != null) {
				Log.error("'" + resc + "' " + errorMsg);
				hirods.rcDisconnect();
				System.exit(3);
			}
		}

		// find all data objects with one or more replicas on source resources
		// if specified, filter out data objects with a data id less than startDataID
		List<String> objs = IrodsQuery.dataObjectsOnResources(hirods, ctx.sourceList, ctx.startDataId);
		Log.debug("Found " + objs.size() + " matching data objects");

		// done with preparation
		hirods.rcDisconnect();	
		
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
