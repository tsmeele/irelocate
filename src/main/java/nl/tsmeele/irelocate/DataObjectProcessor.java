package nl.tsmeele.irelocate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.high.IrodsUser;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class DataObjectProcessor implements Runnable {
	static final int MAX_DATA_OBJECTS_PER_SESSION = 5000;
	static final int DATA_OBJECTS_PER_PROGRESS_REPORT = 5000;
	private int threadId;
	private RelocateContext ctx = null;
	private Queue<String> queue = null;
	private boolean stop = false;
	private long count;
	private long doneOk;
	private long doneReplicated;
	private long doneTrimmed;
	private long doneError;
	private long doneSkipped;
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
		if (ctx.trim) {
			System.out.println("DataObjectProcessor #" + threadId + " is done. Data objects subtotal: " + count + "  okay: " + doneOk +
				"  trimmed-okay: " + doneTrimmed + "  error: " + doneError + "  skipped: " + doneSkipped);
		} else {
			System.out.println("DataObjectProcessor #" + threadId + " is done. Data objects subtotal: " + count + "  okay: " + doneOk +
				"  replicated-okay: " + doneReplicated + "  error: " + doneError + "  skipped: " + doneSkipped);
		}
		// clean up any open server session
		try {
			if (hirods != null) {
				hirods.rcDisconnect();
			}
		} catch (IOException e) {
		}
	}

	/**
	 * @param dataObjId  reference to the data object to process
	 * @return true if no exception raised during processing
	 */
	private boolean process(String dataObjId) {
		/* 
		 * iRODS agents may suffer from memory leaks due to custom rules and/or micro services.
		 * we reconnect now and then to avoid impact of such potential leaks.
		 */
		
		if (dataObjId == null) return false;
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
			System.out.println("Data object processor #" + threadId + " progress: at data object " + dataObjId);
		}

		/* assert all replicas that belong to data_object
		 *   - classify the replicas into perfect, good, stale, bad
		 *  
		 * execute either replicate or trim task, based on command line arguments
		 *   - replicate ONLY IF 
		 *     1) a perfect replica exist  AND
		 *     2) the destination resource lacks a perfect replica
		 *     
		 *   - trim ONLY IF:
		 *     1) a perfect replica exists on (or within of hierarchy of) the destination resource AND
		 *     2) one or more replicas exist on source resources (select these to trim)
		*/
		try {
			// analyze replicas of data object
			List<Replica> replicas = IrodsQuery.getReplicas(hirods, dataObjId);
			Replica localPerfect = null;
			Replica perfect = null;
			Replica goodOrStale = null;
			Replica destPerfect = null;
			boolean intermediate = false;
			Resource destResc = ctx.rescList.get(ctx.destinationResource);
			List<Replica> onSourceResource = new ArrayList<Replica>();
			for (Replica r : replicas) {
				// does replica classify as perfect?
				if (r.isGood() && r.retrieveDatafileStatus(hirods) == 1) {
					// make a note we have at least one perfect replica
					perfect = r;
					// lookup the resource and make a note of other attributes
					Resource resc = ctx.rescList.get(r.dataRescName);
					if (resc != null && resc.isLocal) {
						localPerfect = r;
					}
					if (resc != null && ctx.rescList.isInTree(destResc, resc)) {
						destPerfect = r;
					}
					// optionally consider leaf resources on same host as destination sufficient
					if (ctx.nearby && destPerfect == null && resc != null 
							&& ctx.otherDestinationResources.contains(resc)) {
						destPerfect = r;
					}
				}
				// is replica at rest?
				if (r.isGood() || r.isStale()) {
					// at rest: see if it is located on a source resource
					goodOrStale = r;
					if (ctx.sourceList.contains(r.dataRescName)) {
						onSourceResource.add(r);
					}
				} else {
					intermediate = true;
				}
			}
			
			// decide on an action based on the analysis of all replicas of this object
			
			// ignore data object if none of the replicas are currently at rest 
			if (replicas.isEmpty() || (goodOrStale == null && intermediate)) {
				Log.debug("Skipping intermediate object " + dataObjId);
				doneSkipped++;
				return true;
			}
			
			String path = replicas.get(0).path;
			// report error in case data object lacks a perfect replica
			if (perfect == null) {
				ctx.log.logError(path, "Object lacks a perfect replica");
				if (ctx.verbose) {
					Log.info("ERROR, lacks perfect replica: " + path);
				}
				doneError++;
				return true;
			}
			
			// general preconditions have been met, now trim or replicate
			
			if (ctx.trim) {
				// TRIM action requested
				if (destPerfect == null) {
					// unable to trim because destination does not yet have a perfect replica
					Log.debug("Object lacks perfect replica at destination: " + path);
					doneError++;
					return true;
				}
				if (onSourceResource.isEmpty()) {
					// no replicas to trim
					Log.info("OK: " + path);
					doneOk++;
					return true;
				}
				trimAction(onSourceResource, path);
				return true;
			} else {
				// REPLICATE action requested
				if (destPerfect != null) {
					// perfect replica already exists on destination, notify no action needed
					Log.info("OK: " + path);
					doneOk++;
					return true;
				}
				// we need to replicate, prefer to source from a local copy (performance!) 
				if (localPerfect != null) {
					perfect = localPerfect;
				}
				replicateAction(perfect, path);
				return true;
			}
		
		} catch (IOException e) {
			Log.error("IOException while fetching replicas: " + e.getMessage());
			return false;
		}
	}
	
	private void replicateAction(Replica perfect, String path) throws MyRodsException, IOException {
		Log.debug("...replicating: " + path);
		boolean replicated = perfect.replicate(hirods, ctx.destinationResource, false);
		if (replicated) {
			Log.info("REPLICATED OK: " + path);
			ctx.log.logDone(path);
			doneReplicated++;
		} else {
			ctx.log.logError(path, "Replication failed. iRODS error = " + hirods.intInfo);
			Log.info("ERROR, replication failed (" + hirods.intInfo + "): " + path);
			doneError++;
		}
	}
	
	
	private void trimAction(List<Replica> onSourceResource, String path) throws MyRodsException, IOException {
		Log.debug("...trimming: " + path);
		ArrayList<String> trimErrors = new ArrayList<String>();
		for (Replica r : onSourceResource) {
			if (r.trim(hirods)) {
				Log.info("TRIMMED ON " + r.dataRescName + ": " + path);
				ctx.log.logTrimmed(path, r.dataRescName);
			} else {
				Log.info("ERROR, trim failed (" + hirods.intInfo + ") for resource: " + r.dataRescName  + "  path: " + path);
				trimErrors.add(r.dataRescName);
			}
		}
		if (!trimErrors.isEmpty()) {
			ctx.log.logError(path, "Unable to trim replica on resource(s): " + trimErrors.toString());
			doneError++;
		} else {
			doneTrimmed++;
		}
	}
	
	
	
	
	
}
