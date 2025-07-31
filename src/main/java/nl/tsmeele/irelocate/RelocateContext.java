package nl.tsmeele.irelocate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tsmeele.myrods.high.ConfigReader;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class RelocateContext {
	public static final String PROGRAM_NAME = "irelocate";
	private static final String[] REQUIRED_KEYWORDS = {
			"host","port","username","zone","password", "auth_scheme", "destinationResource"};
	private static final String CONFIG_FILE = PROGRAM_NAME + ".ini";
	private static final String LOG_FILE = PROGRAM_NAME + ".log";
	
	// commandline info that can be queried after processing:
	public HashMap<String,String> options = new HashMap<String,String>();
	public String sourceResources = null;
	public List<String> sourceList = new ArrayList<String>();
	public String destinationResource = null;
	public long startDataId = 0L;
	public String host, userName, zone, password;
	public int port;
	public boolean authPam;
	
	public boolean verbose = false;
	public boolean debug = false;
	public boolean resume = false;
	public String resumeFile = LOG_FILE;
	public String logFile = LOG_FILE;
	public int threads = 1;
	public boolean trim = false;
	public boolean nearby = false;
	public boolean dryrun = false;
	public boolean usage = false;
	
	// information added during session by RelocateMain, after connections have been established
	public IrodsResources rescList = null;
	public LogFile log = null;
	
	
	public void processArgs(String[] args) throws MyRodsException {
		String configFile = CONFIG_FILE;
		// collect and process arguments
		int argIndex = 0;
		while (argIndex < args.length) {
			String optionArg = args[argIndex].toLowerCase();
			if (!optionArg.startsWith("-")) {
				break;
			}
			switch (optionArg) {
				// debug is a hidden option 
				case "-d":
				case "-debug":	{
					debug = true;
					break;
				}
				case "-dryrun": {
					dryrun = true;
					break;
				}
				case "-v":
				case "-verbose": {
					verbose = true;
					break;
				}
				case "-n":
				case "-nearby": {
					nearby = true;
					break;
				}
				case "-c":
				case "-config": {
					if (argIndex < args.length + 1) {
						argIndex++;
						configFile = args[argIndex];
					}
					break;
				}
				case "-l":
				case "-log": {
					if (argIndex < args.length + 1) {
						argIndex++;
						logFile = args[argIndex];
					}
					break;
				}
				case "-s":
				case "-start": {
					if (argIndex < args.length + 1) {
						argIndex++;
						try {
							startDataId = Long.valueOf(args[argIndex]);
						} catch (NumberFormatException e) { 
							/* keep threads 1 in case of parse error */ 
						}
					}
					break;
				}
				case "-t":
				case "-threads": {
					if (argIndex < args.length + 1) {
						argIndex++;
						try {
							threads = Integer.valueOf(args[argIndex]);
							if (threads < 1) threads = 1;
						} catch (NumberFormatException e) { 
							/* keep threads 1 in case of parse error */ 
						}
					}
					break;
				}
				case "-trim":
					trim = true;
					break;
					
				// add new options above this line
				case "-h":
				case "-help":
				case "-?":	
					// an unknown option will trigger the help option
				default: 
					usage =true;
			}
			argIndex++;
		}
		
		// process the remaining, non-option, arguments
		for (; argIndex < args.length; argIndex++) {
			sourceList.add(args[argIndex].trim());
		}
		
		// read and process configuration information
		ConfigReader configReader = new ConfigReader();
		Map<String,String> config = configReader.readConfig(configFile, REQUIRED_KEYWORDS);
		if (config == null) {
			throw new MyRodsException("Missing configuration file: " + configFile);
		}
		host 		= config.get("host");
		port 		= Integer.parseInt(config.get("port"));
		userName 	= config.get("username");
		zone 		= config.get("zone");
		password 	= config.get("password");
		authPam 	= config.get("auth_scheme").toLowerCase().startsWith("pam");
		destinationResource = config.get("destinationResource");
		String startDataIdStr = config.get("startDataId");
		if (startDataId == 0L && startDataIdStr != null) {
			try {
				startDataId = Long.parseLong(startDataIdStr);
			} catch (NumberFormatException e) { };
		}
		sourceResources = config.get("sourceResources");
		if (sourceList.size() == 0 && sourceResources != null) {
			for (String source : sourceResources.split(",| ")) {
				sourceList.add(source.trim());
			}
		}
	}
	
	public String usage() {
		return
				"Usage: " + PROGRAM_NAME + " [options] <source_resources> \n" +
				"<source_resources>      : whitespace separated list of resources\n" +
				"                          selects data objects that have one or more replicas on any of these resources\n\n" +
		        "Options:\n" +
				"-help, -h, -?           : exit after showing this usage text.\n" +
				"-verbose, -v            : print names of processed objects.\n" +
				"-log, -l                : specify name of logfile (default is '" + LOG_FILE + "')\n" +
				"-threads <#threads>, -t : specify number of parallel threads to use. Default is 1 thread.\n" +
				"-trim                   : trim replicas from source resources, provided that a perfect replica exists on destination\n" +
				"                          NB: When trim option is specified, only trim actions take place, no replication actions\n" +
				"-start, -s              : filters objects, only select objects with data id higher or equal to start\n" +
				"-nearby, -n             : a replica on a resource located on the same host as the destination resource suffices" +
				"-dryrun                 : perform all preparations (and select data objects) but do not take any further actions" +
		        "-config <configfile>    :\n" +
		        "   The configfile is a local path to a textfile with configuration key=value lines.\n" +
		        "\nConfiguration file keywords:\n" +
				printKeywords(REQUIRED_KEYWORDS) + "\n";
	}
	
	private String printKeywords(String[] keywords) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String s: keywords) {
			sb.append("  " + s);
			if (i >= 4) sb.append("\n");
			i = (i + 1) % 5; 
		}
		return sb.toString();
	}
	
	public String toString() {
		return 
			"verbose / debug / usage      = " + verbose + " / " + debug + " / " + usage + "\n" +
			"logfile                      = " + logFile + "\n" +
			"threads                      = " + threads + "\n" +
			"host : port                  = " + host + " : " + port + "\n" +
			"username # zone (authPam)    = " + userName + " # " + zone + " (" + authPam + ")\n" +
			"password                     = " + (password == null || password.equals("")? "null" : "*redacted*") + "\n" +
			"destinationResource          = " + destinationResource + "\n" +
			"sourceResources              = " + sourceResources + "\n";
	}
	
}
