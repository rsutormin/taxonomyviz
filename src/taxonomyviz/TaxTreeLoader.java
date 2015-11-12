package taxonomyviz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;

import util.db.MysqlConn;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxTreeLoader {
	private static final String ncbiFtpZipUrl = "ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdmp.zip";
	private static final Pattern div = Pattern.compile(Pattern.quote("\t|\t"));
	private static final String dataDirName = "data";
	private static final String taxonomyFileName = "taxonomy.json";
	private static final String tempDirName = "temp";
	private static final boolean removeHidden = false;
	
	public static void main(String[] args) throws Exception {
		loadTaxTree();
	}

	public static TaxNode getTaxTree() throws Exception {
		loadTaxTree();
		File taxFile = new File(dataDirName, taxonomyFileName);
		return new ObjectMapper().readValue(taxFile, TaxNode.class);
	}
	
	public static void loadTaxTree() throws Exception {
		File dataDir = new File(dataDirName);
		if (!dataDir.exists())
			dataDir.mkdirs();
		File taxFile = new File(dataDir, taxonomyFileName);
		if (taxFile.exists())
			return;
		File tempDir = new File(tempDirName);
		Map<Integer, TaxNode> nodeMap = new HashMap<Integer, TaxNode>();
		BufferedReader br = new BufferedReader(new InputStreamReader(findZipEntry(
				ncbiFtpZipUrl, tempDir, "names.dmp")));
		while (true) {
			String l = br.readLine();
			if (l == null)
				break;
			if (l.trim().length() == 0)
				continue;
			if (l.endsWith("\t|"))
				l = l.substring(0, l.length() - 2);
			String[] parts = div.split(l);
			if (parts.length != 4)
				throw new IllegalStateException("Wrong line format: [" + l + "]");
			if (!parts[3].equals("scientific name"))
				continue;
			int nodeId = Integer.parseInt(parts[0]);
			nodeMap.put(nodeId, new TaxNode(nodeId, parts[1]));
		}
		br.close();
		System.out.println("Nodes: " + nodeMap.size());
		int rootId = -1;
		Set<Integer> hidden = new HashSet<Integer>();
		br = new BufferedReader(new InputStreamReader(findZipEntry(ncbiFtpZipUrl, 
				tempDir, "nodes.dmp")));
		while (true) {
			String l = br.readLine();
			if (l == null)
				break;
			if (l.trim().length() == 0)
				continue;
			if (l.endsWith("\t|"))
				l = l.substring(0, l.length() - 2);
			String[] parts = div.split(l);
			if (parts.length < 12)
				throw new IllegalStateException("Wrong line format (" + 
						parts.length + " fields): [" + l + "]");
			int nodeId = Integer.parseInt(parts[0]);
			int parentId = Integer.parseInt(parts[1]);
			if (nodeId == parentId) {
				rootId = nodeId;
				continue;
			}
			TaxNode parent = nodeMap.get(parentId);
			if (parent == null)
				throw new IllegalStateException("No node for parent id=" + parentId);
			TaxNode node = nodeMap.get(nodeId);
			if (node == null)
				throw new IllegalStateException("No node for id=" + nodeId);
			if (parts[10].equals("1")) {
				hidden.add(nodeId);
				node.hidden = 1;
			}
			if (parent.children == null)
				parent.children = new ArrayList<TaxNode>();
			parent.children.add(node);
		}
		br.close();
		TaxNode root = nodeMap.get(rootId);
		if (removeHidden) {
			System.out.println("Hidden in genbank: " + hidden.size());
			removeHidden(null, root, hidden);
			System.out.println("Remaining nodes: " + countNodes(root));
		}
		prepareNodeIndeces(root, 0, new TreeMap<Integer, Integer>(), -1);
		new ObjectMapper().writeValue(taxFile, root);
		MysqlConn.get().dropTableIfExists(MysqlDbManager.TBL_TAX_INDEX);
		long time = System.currentTimeMillis();
		MysqlConn.Batch batch = MysqlDbManager.createTaxIndexBatch();
		int inserted = insertTaxIndex(-1, "", root, batch);
		batch.close();
		System.out.println("Db upload time: " + (System.currentTimeMillis() - time) + " ms");
		System.out.println("Rows inserted: " + inserted);
	}
	
	private static InputStream findZipEntry(String zipUrl, File tempDir, 
			String entryName) throws Exception {
		if (!tempDir.exists())
			tempDir.mkdirs();
		File tempFile = new File(tempDir, zipUrl.substring(zipUrl.lastIndexOf('/') + 1));
		if (!tempFile.exists()) {
			InputStream is = new URL(zipUrl).openStream();
			FileOutputStream fos = new FileOutputStream(tempFile);
			IOUtils.copy(is, fos);
			fos.close();
			is.close();
		}
		ZipInputStream zis = new ZipInputStream(new FileInputStream(tempFile));
		while (true) {
			ZipEntry ze = zis.getNextEntry();
			if (ze == null)
				break;
			if (ze.getName().equals(entryName))
				return zis;
		}
		zis.close();
		throw new IllegalStateException("Can't find entry " + entryName + " in zip file");
	}
	
	private static void removeHidden(TaxNode parent, TaxNode node, Set<Integer> hiddenNodes) {
		TaxNode futPar = node;
		if (hiddenNodes.contains(node.taxid)) {
			parent.children.remove(node);
			if (node.children != null)
				for (TaxNode ch : node.children)
					parent.children.add(ch);
			futPar = parent;
		}
		if (node.children != null) {
			List<TaxNode> children = new ArrayList<TaxNode>(node.children);
			for (TaxNode ch : children)
				removeHidden(futPar, ch, hiddenNodes);
		}
		if (futPar.children != null && futPar.children.isEmpty())
			futPar.children = null;
	}
	
	private static int countNodes(TaxNode node) {
		int ret = 1;
		if (node.children != null) {
			for (TaxNode ch : node.children)
				ret += countNodes(ch);
		}
		return ret;
	}
	
	private static int prepareNodeIndeces(TaxNode node, int layer, 
			Map<Integer, Integer> layerMaxId, int prevIndex) {
		prevIndex++;
		node.ind = prevIndex;
		node.layer = layer;
		Integer lpos = layerMaxId.get(layer);
		if (lpos == null) {
			lpos = 0;
		} else {
			lpos += 1;
		}
		node.lpos = lpos;
		layerMaxId.put(layer, lpos);
		if (node.children != null) {
			for (TaxNode ch : node.children)
				prevIndex = prepareNodeIndeces(ch, layer + 1, layerMaxId, prevIndex);
		}
		node.maxind = prevIndex;
		return prevIndex;
	}

	private static int insertTaxIndex(int parId, String path, TaxNode node, MysqlConn.Batch target) throws Exception {
		path += "/" + node.taxid;
		int size = node.children == null ? 0 : node.children.size();
		target.addNextRow(new Object[] {node.taxid, parId, node.title, node.hidden, 
				node.layer, node.lpos, node.ind, node.maxind, path, size});
		int ret = 1;
		if (node.children != null)
			for (TaxNode ch : node.children)
				ret += insertTaxIndex(node.taxid, path, ch, target);
		return ret;
	}
}