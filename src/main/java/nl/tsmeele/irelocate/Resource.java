package nl.tsmeele.irelocate;

public class Resource {
	public String id, name, loc, type, parent;
	public boolean isLocal;

	public Resource(String id, String name, String loc, String type, String parent, boolean isLocal) {
		this.id = id;
		this.name = name;
		this.loc = loc;
		this.type = type;
		this.parent = parent;
		this.isLocal = isLocal;
	}
	
	public boolean isStorageResource() {
		return type.toLowerCase().equals("unixfilesystem") && !name.equals("bundleResc");
	}
	
	public String toString() {
		return "RESC{" + id + ", " + name + ", " + loc + (isLocal ? "(IS_LOCAL)" : "") + ", " + type + ", " + parent + "}";
	}
}
