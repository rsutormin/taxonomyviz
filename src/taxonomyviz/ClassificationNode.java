package taxonomyviz;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ClassificationNode implements Comparable<ClassificationNode> {
	public String id;
	public String name;
	public String descr;
	public Set<ClassificationNode> subnodes = null;
	public Set<ClassificationNode> parents = null;
	public Map<String, ClassificationNode> nameToSubTree = null;
	
	public ClassificationNode() {}

	/**
	 * Root node.
	 * @param subnodes
	 */
	public ClassificationNode(ClassificationNode... subnodes) {
		this("", subnodes);
	}

	public ClassificationNode(String id, ClassificationNode... subnodes) {
		this(id, id, subnodes);
	}

	public ClassificationNode(String id, String name, ClassificationNode... subnodes) {
		this(id, name, "", subnodes);
	}

	public ClassificationNode(String id, String name, String descr, ClassificationNode... subnodes) {
		this.id = id;
		this.name = name;
		this.descr = descr;
		if (subnodes != null && subnodes.length > 0) {
			this.subnodes = new LinkedHashSet<ClassificationNode>(Arrays.asList(subnodes));
			for (ClassificationNode child : subnodes)
				child.addParent(this);
		}
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();
	}
	
	@Override
	public String toString() {
		return name == null ? "" : name;
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj != null && obj instanceof ClassificationNode && 
				toString().equals(obj.toString());
	}
	
	@Override
	public int compareTo(ClassificationNode o) {
		return toString().compareTo(o.toString());
	}
	
	public void addParent(ClassificationNode parent) {
		if (parents == null)
			parents = new LinkedHashSet<ClassificationNode>();
		if (parents.contains(parent))
			throw new IllegalStateException("Parent " + parent + " was already added");
		parents.add(parent);
	}
	
	public void addChild(ClassificationNode child) {
		if (subnodes == null)
			subnodes = new LinkedHashSet<ClassificationNode>();
		subnodes.add(child);
		child.addParent(this);
	}
}
