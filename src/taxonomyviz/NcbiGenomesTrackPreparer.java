package taxonomyviz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class NcbiGenomesTrackPreparer {
	private static final File inputZip = new File("input/ncbi_genomes/ncbi_genomes.zip");
	private static final File classificationFile = new File("input/ncbi_genomes/classification.txt");
	private static final Pattern tabDiv = Pattern.compile(Pattern.quote("\t"));
	private static final ColumnDescription[] columns = {
		new ColumnDescription("name", "Organism/Name", ColumnType.StringVal, "Organism name at the species level", "#Organism/Name"),
		new ColumnDescription("tax_id", "TaxID", ColumnType.IntegerVal, "NCBI Taxonomy ID"),
		new ColumnDescription("bio_proj_acc", "BioProject Accession", ColumnType.StringVal, "BioProject Accession number from BioProject database"),
		new ColumnDescription("bio_proj_id", "BioProject ID", ColumnType.IntegerVal, "BioProject ID from BioProject database"),
		new ColumnDescription("tax_group", "Group", ColumnType.StringVal, "Commonly used organism groups (Phylum)"),
		new ColumnDescription("tax_subgroup", "SubGroup", ColumnType.StringVal, "NCBI Taxonomy level below group (Class level)"),
		new ColumnDescription("size_mb", "Size (Mb)", ColumnType.DoubleVal, "Total length of DNA submitted for the project"),
		new ColumnDescription("size_kb", "Size (Kb)", ColumnType.DoubleVal, "Total length of DNA (in kilobases)"),
		new ColumnDescription("gc", "GC%", ColumnType.DoubleVal, "Percent of nitrogenous bases (guanine or cytosine) in DNA submitted for the project"),
		new ColumnDescription("assem_acc", "Assembly Accession", ColumnType.StringVal, "Name of the genome assembly (from NCBI Assembly database)"),
		new ColumnDescription("chroms", "Chromosomes", ColumnType.IntegerVal, "Number of chromosomes submitted for the project"),
		new ColumnDescription("chr_refseq", "Chromosomes/RefSeq", ColumnType.StringVal, "Refseq chromosome sequence accessions"),
		new ColumnDescription("chr_genbank", "Chromosomes/INSDC", ColumnType.StringVal, "GenBank chromosome sequence accessions"),
		new ColumnDescription("orgs", "Organelles", ColumnType.IntegerVal, "Number of organelles submitted for the project"),
		new ColumnDescription("plasms", "Plasmids", ColumnType.IntegerVal, "Number of plasmids submitted for the project"), 
		new ColumnDescription("pl_refseq", "Plasmids/RefSeq", ColumnType.StringVal, "Refseq plasmid sequence accessions"),
		new ColumnDescription("pl_genbank", "Plasmids/INSDC", ColumnType.StringVal, "GenBank plasmid sequence accessions"),
		new ColumnDescription("wgs", "WGS", ColumnType.StringVal, "Four-letter Accession prefix followed by version as defined in WGS division of GenBank/INSDC"),
		new ColumnDescription("scaffs", "Scaffolds", ColumnType.IntegerVal, "Number of scaffolds in the assembly"),
		new ColumnDescription("genes", "Genes", ColumnType.IntegerVal, "Number of Genes annotated in the assembly"),
		new ColumnDescription("prots", "Proteins", ColumnType.IntegerVal, "Number of Proteins annotated in the assembly"),
		new ColumnDescription("rel_date", "Release Date", new ColumnType("YYYY/MM/dd"), "First public sequence release for the project"),
		new ColumnDescription("mod_date", "Modify Date", new ColumnType("YYYY/MM/dd"), "Sequence modification date for the project"),
		new ColumnDescription("status", "Status", new ColumnType(new ClassificationNode(
				new ClassificationNode("Complete", 
						new ClassificationNode("Complete Genome")), 
				new ClassificationNode("Incomplete", 
						new ClassificationNode("Chromosome", 
								new ClassificationNode("Chromosome(s)")),
						new ClassificationNode("Contig"), 
						new ClassificationNode("Scaffold"))
				)), "Highest level of assembly: Chromosomes - one or more chromosomes are assembled, Scaffolds or contigs - sequence assembled but no chromosomes, SRA or Traces - raw sequence data available, No data - no data is connected to the BioProject ID"),
		new ColumnDescription("center", "Center", ColumnType.StringVal, "Sequencing/assembling organisation"),
		new ColumnDescription("bio_samp_acc", "BioSample Accession", ColumnType.StringVal, "BioSample Accession number from BioProject database"),
		new ColumnDescription("ref", "Reference", ColumnType.StringVal, "Reference"),
		new ColumnDescription("ftp", "FTP Path", ColumnType.StringVal, "FTP Path"),
		new ColumnDescription("pmid", "Pubmed ID", ColumnType.StringVal, "Pubmed ID"),
		new ColumnDescription("host", "Host", ColumnType.StringVal, "Natural host of a virus"),
		new ColumnDescription("segms", "Segments", ColumnType.IntegerVal, "Number of segments in viral genome", "Segmemts")
	};

	public static void main(String[] args) throws Exception {
		Map<String, ColumnDescription> columnHash = new HashMap<String, ColumnDescription>();
		for (ColumnDescription cd : columns) {
			columnHash.put(cd.name, cd);
			if (cd.aliases != null)
				for (String alias : cd.aliases)
					columnHash.put(alias, cd);
		}
		List<Object> ret = new ArrayList<Object>();
		Set<String> classificationStat = new TreeSet<String>();
		ZipInputStream zis = new ZipInputStream(new FileInputStream(inputZip));
		try {
			while (true) {
				ZipEntry ze = zis.getNextEntry();
				if (ze == null)
					break;
				System.out.println("Parsing " + ze.getName() + "...");
				BufferedReader br = new BufferedReader(new InputStreamReader(zis));
				parseFile(ze.getName(), br, columnHash, ret, classificationStat);
			}
		} finally {
			zis.close();
		}
		System.out.println(ret.size());
		PrintWriter pw = new PrintWriter(classificationFile);
		try {
			for (String value : classificationStat)
				pw.println(value);
		} finally {
			pw.close();
		}
	}
	
	private static void parseFile(String fileName, BufferedReader br, 
			Map<String, ColumnDescription> columnHash, List<Object> ret, 
			Set<String> classificationStat) throws Exception {
		String[] header = tabDiv.split(br.readLine().trim());
		ColumnDescription[] colDescrs = new ColumnDescription[header.length];
		for (int i = 0; i < header.length; i++) {
			String colName = header[i];
			ColumnDescription cd = columnHash.get(colName);
			if (cd == null)
				throw new IllegalStateException("Unexpected column name: [" + colName + "]");
			colDescrs[i] = cd;
		}
		for (int linePos = 1; ; linePos++) {
			String l = br.readLine();
			if (l == null)
				break;
			String[] values = tabDiv.split(l);
			if (values.length != colDescrs.length)
				throw new IllegalStateException("Unexpected number of cells at line (" + 
						(linePos + 1) + ": [" + l + "]");
			Map<String, Object> row = new LinkedHashMap<String, Object>();
			for (int i = 0; i < values.length; i++) {
				String rawValue = values[i];
				if (rawValue.length() == 0 || rawValue.equals("-"))
					continue;
				Object propValue = null;
				String propKey = colDescrs[i].id;
				ColumnType type = colDescrs[i].type;
				try {
					if (type.equals(ColumnType.BooleanVal)) {
						propValue = Boolean.parseBoolean(rawValue);
					} else if (type.equals(ColumnType.IntegerVal)) {
						propValue = Integer.parseInt(rawValue);
					} else if (type.equals(ColumnType.DoubleVal)) {
						propValue = Double.parseDouble(rawValue);
					} else {
						if (type.isClassification()) {
							rawValue = rawValue.trim();
							classificationStat.add(rawValue);
						}
						propValue = rawValue;
					}
				} catch (Exception ex) {
					throw new IllegalStateException("Error parsing value at line " + 
							(linePos + 1) + ", column " + (i + 1) + " (" + 
							colDescrs[i].name + ") with type " + type + " (" + 
							rawValue + "): [" + l + "]", ex);
				}
				row.put(propKey, propValue);
			}
			ret.add(row);
		}
	}
}
