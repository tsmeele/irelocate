package nl.tsmeele.irelocate;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.high.IrodsUser;

public class DataObjectProcessor implements Runnable {
	static final int MAX_DATA_OBJECTS_PER_SESSION = 1000;
	static final int DATA_OBJECTS_PER_PROGRESS_REPORT = 5000;
	private int threadId;
	private RelocateContext ctx = null;
	private Queue<String> queue = null;
	private boolean stop = false;
	private long count;
	private Hirods hirods = null;
	
	DataObjectProcessor(int threadId, RelocateContext ctx, Queue<String> queue) {
		this.threadId = threadId;
		this.ctx = ctx;
		this.queue = queue;
	}

	public void stop() {
		stop = true;
	}
	
	@Override
	public void run() {
		Log.debug("START DataObjectProcessor #" + threadId);
		while (process(queue.poll()) && !stop) {
			count++;
		}
		System.out.println("DataObjectProcessor #" + threadId + " is done, has processed " + count + " objects");
		// clean up any open server session
		try {
			if (hirods != null) {
				hirods.rcDisconnect();
			}
		} catch (IOException e) {
		}
	}

	private boolean process(String dataObj) {
		/* 
		 * iRODS agents may suffer from memory leaks due to custom rules and/or micro services.
		 * we reconnect now and then to avoid impact of such potential leaks.
		 */
		
		if (dataObj == null) return false;
		if (hirods != null && count % MAX_DATA_OBJECTS_PER_SESSION == 0) {
			try {
				hirods.rcDisconnect();
			} catch (IOException e) {
			} finally {
				hirods = null;
			}
		}
		
		// make sure we are logged in
		if (hirods == null) {
			IrodsUser user = new IrodsUser(ctx.userName, ctx.zone);
			hirods = RelocateMain.rodsAdminLogin(ctx.host, ctx.port, user, ctx.password, ctx.authPam);
			if (hirods == null) {
				Log.error("Unable to reconnect while processing data objects");
				return false;
			} else {
				Log.debug("reconnected");
			}
		}
		
		// report progress (independent of log level)
		if ( count != 0 && count % DATA_OBJECTS_PER_PROGRESS_REPORT == 0) {
			System.out.println("Data object processor #" + threadId + " progress: at data object " + dataObj);
		}

		/* assert all replicas that belong to data_object
		 *   - classify the replicas into perfect, good, stale, bad
		 *   - qreplicate if 
		 *     1) a perfect replica exist  AND
		 *     2) the destination resource lacks a perfect replica
		 *     
		 *  FUTURE: trim if:
		 *     1) a perfect replica exists on the destination resource AND
		 *     2) replicas exist on obsolete resources (select these)
		*/
		try {
			// analyze replicas of data object
			List<Replica> replicas = IrodsQuery.getReplicas(hirods, dataObj);
			Replica localPerfect = null;
			Replica perfect = null;
			Replica goodOrStale = null;
			Replica destPerfect = null;
			boolean intermediate = false;
			for (Replica r : replicas) {
				if (r.isGood() && r.retrieveDatafileStatus(hirods) == 1) {
					// replica in perfect condition
					perfect = r;
					Resource resc = ctx.rescList.get(r.dataRescName);
					if (resc != null && resc.isLocal) {
						localPerfect = r;
					}
					if (r.dataRescName.equals(ctx.destinationResource)) {
						destPerfect = r;
					}
				}
				if (r.isGood() || r.isStale()) {
					goodOrStale = r;
				} else {
					intermediate = true;
				}
			}
			
			// ignore data object if all replicas not at rest 
			if (replicas.isEmpty() || (goodOrStale == null && intermediate)) {
				Log.debug("Skipping intermediate object " + dataObj);
				return true;
			}
			
			String path = replicas.get(0).path;
			// report error in case data object lacks a perfect replica
			if (!intermediate && perfect == null) {
				ctx.log.logError(path, "Object lacks a perfect replica");
				if (ctx.verbose) {
					Log.info("ERROR, lacks perfect replica: " + path);
				}
				return true;
			}
			
			// (silently) ok in case data object has a perfect replica on destination
			if (destPerfect != null) {
				if (ctx.verbose) {
					Log.info("OK: " + path);
				}
				return true;
			}
			
			// replicate perfect copy to destination, prefer a local copy
			if (localPerfect != null) {
				perfect = localPerfect;
			}
			Log.debug("...replicating: " + path);
			boolean replicated = perfect.replicate(hirods, ctx.destinationResource, false);
			if (!replicated) {
				ctx.log.logError(path, "Replication failed. iRODS error = " + hirods.intInfo);
				if (ctx.verbose) {
					Log.info("ERROR, replication failed (" + hirods.intInfo + "): " + path);
				}
			} else {
				if (ctx.verbose) {
					Log.info("REPLICATED OK: " + path);
				}
			}
			
		} catch (IOException e) {
			Log.info("IOException while fetching replicas: " + e.getMessage());
			return false;
		}
		
		return true;
	}
	
	
	
}
