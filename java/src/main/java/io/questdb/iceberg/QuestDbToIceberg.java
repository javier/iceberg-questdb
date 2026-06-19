package io.questdb.iceberg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Zero-copy register QuestDB cold-storage Parquet as an Iceberg table (Java edition).
 *
 * <p>Mirrors the Python {@code questdb_to_iceberg.py}: it registers existing S3 Parquet in place
 * (no rewrite) via Iceberg's core append API, partitioned by hour(timestamp). Bucket, prefix,
 * region and warehouse are required; nothing site-specific is hardcoded. The table name is taken
 * from the prefix. Runs are incremental by default; {@code --rebuild} drops and re-registers.
 *
 * <p>Unlike PyIceberg, the Java reference implementation can keep nanosecond timestamps and UUIDs
 * as native Iceberg types:
 * <ul>
 *   <li>{@code --timestamp-mode v3} (default v2): create a format-version-3 table and keep
 *       {@code timestamp_ns}; {@code v2} downcasts ns to microsecond {@code timestamptz} for broad
 *       engine compatibility.</li>
 *   <li>UUID columns (QuestDB writes fixed_len_byte_array(16) + UUID logical type) map straight to
 *       Iceberg {@code uuid} on both modes - no fixed[16] fallback.</li>
 * </ul>
 *
 * <p>Register the same data twice under different namespaces/modes (e.g. {@code questdb_v2} and
 * {@code questdb_v3}) to serve mixed-capability readers from one untouched copy of the data.
 */
public final class QuestDbToIceberg {

  private QuestDbToIceberg() {}

  enum TsMode { V2, V3 }

  // ---- the only AWS-specific boundary: credentials come from the default SDK chain ----
  // Auth is resolved by the AWS SDK default provider chain (env vars, shared config, SSO,
  // instance/role). With --profile we set the AWS profile system property so the chain (incl.
  // SSO) uses it; run `aws sso login --profile <p>` first. Swap this if you authenticate
  // differently (e.g. build an explicit AwsCredentialsProvider and pass it to the S3 client /
  // S3FileIO via the `client.credentials-provider` property).
  static void applyProfile(String profile) {
    if (profile != null && !profile.isEmpty()) {
      System.setProperty("aws.profile", profile);
    }
  }

  public static void main(String[] argv) throws Exception {
    Args a = Args.parse(argv);
    applyProfile(a.profile);

    TableIdentifier id = TableIdentifier.of(Namespace.of(a.namespace), tableNameFromPrefix(a.prefix));

    // 1) list files
    List<String> files;
    try (S3Client s3 = S3Client.builder().region(Region.of(a.region)).build()) {
      files = listParquet(s3, a.bucket, a.prefix);
    }
    System.out.println("Found " + files.size() + " parquet files under s3://" + a.bucket + "/" + a.prefix);
    if (files.isEmpty()) {
      System.err.println("No files matched. Check --bucket/--prefix.");
      System.exit(1);
    }

    // FileIO for footer reads / metrics (S3FileIO honours the same AWS chain)
    Map<String, String> ioProps = new HashMap<>();
    ioProps.put("s3.region", a.region);
    org.apache.iceberg.aws.s3.S3FileIO io = new org.apache.iceberg.aws.s3.S3FileIO();
    io.initialize(ioProps);

    // 2) build the Iceberg schema from one file (uuid logical -> uuid, ns -> timestamp_ns or us)
    MessageType parquetSchema = readParquetSchema(io, files.get(0));
    boolean sourceHasNs = hasNanoTimestamp(parquetSchema);
    Schema schema = buildSchema(parquetSchema, a.mode);
    String formatVersion = (a.mode == TsMode.V2) ? "2" : "3";

    System.out.println("\n--- inferred iceberg schema (mode " + a.mode + ", format-version " + formatVersion + ") ---");
    System.out.println(schema);
    // In v2 mode the ns timestamp column is labelled microsecond while the Parquet bytes stay
    // nanosecond, so footer min/max would be written ~1000x too large. Suppress those bounds.
    Set<Integer> suppressBounds = new HashSet<>();
    if (sourceHasNs) {
      if (a.mode == TsMode.V2) {
        System.out.println("WARNING: source has nanosecond timestamps but v2 has no ns type. The column is "
            + "labelled microsecond over nanosecond bytes: row reads are engine-dependent and min/max bounds "
            + "are unreliable, so this tool drops the ns timestamp bounds. Prefer --timestamp-mode v3 for ns "
            + "sources; use v2 only for already-microsecond tables or v3-incapable readers.");
        for (org.apache.parquet.schema.Type pf : parquetSchema.getFields()) {
          LogicalTypeAnnotation lta = pf.getLogicalTypeAnnotation();
          if (lta instanceof TimestampLogicalTypeAnnotation ts
              && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS) {
            Types.NestedField fld = schema.findField(pf.getName());
            if (fld != null) {
              suppressBounds.add(fld.fieldId());
            }
          }
        }
      } else {
        System.out.println("note: source has nanosecond timestamps; v3 mode keeps them native as timestamp_ns "
            + "(lossless; readers must support Iceberg v3 nanosecond timestamps).");
      }
    }

    // 3) catalog: local JDBC (SQLite) backend, metadata in the S3 warehouse
    Map<String, String> catProps = new HashMap<>();
    catProps.put(CatalogProperties.URI, jdbcUri(a.catalogDb));
    catProps.put(CatalogProperties.WAREHOUSE_LOCATION, a.warehouse);
    catProps.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    catProps.put("s3.region", a.region);
    JdbcCatalog catalog = new JdbcCatalog();
    catalog.initialize("iceberg", catProps);

    if (catalog instanceof SupportsNamespaces sn) {
      Namespace ns = Namespace.of(a.namespace);
      if (!sn.namespaceExists(ns)) {
        sn.createNamespace(ns);
      }
    }

    if (a.rebuild && catalog.tableExists(id)) {
      System.out.println("--rebuild: dropping existing table (S3 data left intact)");
      catalog.dropTable(id, false); // drop, not purge
    }

    Table table;
    if (catalog.tableExists(id)) {
      table = catalog.loadTable(id);
    } else {
      PartitionSpec spec = PartitionSpec.builderFor(schema).hour(a.tsCol).build();
      Map<String, String> tableProps = new HashMap<>();
      tableProps.put(TableProperties.FORMAT_VERSION, formatVersion);
      // QuestDB files carry no Iceberg field IDs, so reads must fall back to name mapping.
      tableProps.put(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(MappingUtil.create(schema)));
      table = catalog.createTable(id, schema, spec, tableProps);
      System.out.println("created " + id);
    }

    // 4) incremental zero-copy registration
    int added = registerNewFiles(table, io, files, suppressBounds);
    System.out.println(added == 0 ? "nothing to do; table is up to date" : "registered " + added + " new files");

    // 5) validation
    table.refresh();
    System.out.println("\n--- iceberg table ---");
    System.out.println("schema:\n" + table.schema());
    System.out.println("partition spec:\n" + table.spec());

    long totalRows = 0;
    int fileCount = 0;
    List<String> samplePaths = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        totalRows += t.file().recordCount();
        fileCount++;
        if (samplePaths.size() < 3) {
          samplePaths.add(t.file().path().toString());
        }
      }
    }
    System.out.println("\nregistered files: " + fileCount);
    System.out.println("total rows (from footer stats, no scan): " + totalRows);
    System.out.println("sample registered file paths:");
    for (String p : samplePaths) {
      System.out.println("  " + p);
    }

    if (a.sampleRows > 0) {
      System.out.println("\n--- " + a.sampleRows + " sample rows ---");
      int n = 0;
      try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
        for (Record r : rows) {
          System.out.println(r);
          if (++n >= a.sampleRows) {
            break;
          }
        }
      }
    }
  }

  // ----------------------------------------------------------------------------------------

  static List<String> listParquet(S3Client s3, String bucket, String prefix) {
    List<String> out = new ArrayList<>();
    String token = null;
    do {
      ListObjectsV2Request req = ListObjectsV2Request.builder()
          .bucket(bucket).prefix(prefix).continuationToken(token).build();
      ListObjectsV2Response resp = s3.listObjectsV2(req);
      for (S3Object o : resp.contents()) {
        if (o.key().endsWith("data.parquet")) {
          out.add("s3://" + bucket + "/" + o.key());
        }
      }
      token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
    } while (token != null);
    Collections.sort(out);
    return out;
  }

  static String tableNameFromPrefix(String prefix) {
    String last = prefix.replaceAll("/+$", "");
    last = last.substring(last.lastIndexOf('/') + 1);
    int tilde = last.indexOf('~');
    return tilde >= 0 ? last.substring(0, tilde) : last;
  }

  static String jdbcUri(String catalogDb) {
    return catalogDb.startsWith("jdbc:") ? catalogDb : "jdbc:sqlite:" + catalogDb;
  }

  static MessageType readParquetSchema(FileIO io, String path) throws IOException {
    InputFile in = io.newInputFile(path);
    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile(in))) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
  }

  /**
   * Build the Iceberg schema from the Parquet schema, honouring QuestDB's logical types that
   * {@code ParquetSchemaUtil.convert} flattens: UUID logical type -> Iceberg uuid, and nanosecond
   * timestamps -> timestamp_ns (v3) or microsecond timestamp (v2 downcast). Other columns keep the
   * standard conversion (incl. nested lists).
   */
  static Schema buildSchema(MessageType parquetSchema, TsMode mode) {
    Schema base = ParquetSchemaUtil.convert(parquetSchema);
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField f : base.columns()) {
      LogicalTypeAnnotation lta = parquetSchema.getType(f.name()).getLogicalTypeAnnotation();
      fields.add(Types.NestedField.of(f.fieldId(), f.isOptional(), f.name(), refine(f.type(), lta, mode), f.doc()));
    }
    return new Schema(fields);
  }

  private static Type refine(Type fallback, LogicalTypeAnnotation lta, TsMode mode) {
    if (lta instanceof UUIDLogicalTypeAnnotation) {
      return Types.UUIDType.get();
    }
    if (lta instanceof TimestampLogicalTypeAnnotation ts
        && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS) {
      boolean utc = ts.isAdjustedToUTC();
      if (mode == TsMode.V3) {
        return utc ? Types.TimestampNanoType.withZone() : Types.TimestampNanoType.withoutZone();
      }
      return utc ? Types.TimestampType.withZone() : Types.TimestampType.withoutZone();
    }
    return fallback;
  }

  /** Adapt an Iceberg InputFile to a parquet-hadoop InputFile (Iceberg's ParquetIO is internal). */
  static org.apache.parquet.io.InputFile parquetInputFile(InputFile in) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() {
        return in.getLength();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() {
        org.apache.iceberg.io.SeekableInputStream s = in.newStream();
        return new org.apache.parquet.io.DelegatingSeekableInputStream(s) {
          @Override
          public long getPos() throws IOException {
            return s.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            s.seek(newPos);
          }
        };
      }
    };
  }

  static boolean hasNanoTimestamp(MessageType parquetSchema) {
    for (org.apache.parquet.schema.Type f : parquetSchema.getFields()) {
      LogicalTypeAnnotation lta = f.getLogicalTypeAnnotation();
      if (lta instanceof TimestampLogicalTypeAnnotation ts
          && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS) {
        return true;
      }
    }
    return false;
  }

  private static final Pattern HIVE_HOUR =
      Pattern.compile("year=(\\d+)/month=(\\d+)/day=(\\d+)/hour=(\\d+)");

  /** Iceberg hour(timestamp) value (hours since epoch) read from a Hive path. */
  static int hourOrdinal(String path) {
    Matcher m = HIVE_HOUR.matcher(path);
    if (!m.find()) {
      throw new IllegalArgumentException("no year=/month=/day=/hour= partition in " + path);
    }
    long epochSec = OffsetDateTime.of(
        Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)),
        Integer.parseInt(m.group(4)), 0, 0, 0, ZoneOffset.UTC).toEpochSecond();
    return (int) (epochSec / 3600L);
  }

  static int registerNewFiles(Table table, FileIO io, List<String> files, Set<Integer> suppressBounds)
      throws IOException {
    Set<String> existing = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        existing.add(t.file().path().toString());
      }
    }
    List<String> toAdd = new ArrayList<>();
    for (String f : files) {
      if (!existing.contains(f)) {
        toAdd.add(f);
      }
    }
    System.out.println(existing.size() + " files already registered, " + toAdd.size() + " new");
    if (toAdd.isEmpty()) {
      return 0;
    }

    PartitionSpec spec = table.spec();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    org.apache.iceberg.mapping.NameMapping mapping = MappingUtil.create(table.schema());
    AppendFiles append = table.newAppend();
    for (String path : toAdd) {
      InputFile in = io.newInputFile(path);
      Metrics metrics = ParquetUtil.fileMetrics(in, metricsConfig, mapping);
      if (!suppressBounds.isEmpty()) {
        metrics = withoutBounds(metrics, suppressBounds);
      }
      PartitionData partition = new PartitionData(spec.partitionType());
      partition.set(0, hourOrdinal(path));
      DataFile dataFile = DataFiles.builder(spec)
          .withPath(path)
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(in.getLength())
          .withMetrics(metrics)
          .withPartition(partition)
          .build();
      append.appendFile(dataFile);
    }
    append.commit();
    return toAdd.size();
  }

  /** Copy of the metrics without lower/upper bounds for the given field ids (counts kept). */
  static Metrics withoutBounds(Metrics m, Set<Integer> ids) {
    Map<Integer, ByteBuffer> lower = m.lowerBounds() == null ? null : new HashMap<>(m.lowerBounds());
    Map<Integer, ByteBuffer> upper = m.upperBounds() == null ? null : new HashMap<>(m.upperBounds());
    if (lower != null) {
      ids.forEach(lower::remove);
    }
    if (upper != null) {
      ids.forEach(upper::remove);
    }
    return new Metrics(m.recordCount(), m.columnSizes(), m.valueCounts(),
        m.nullValueCounts(), m.nanValueCounts(), lower, upper);
  }

  // ----------------------------------------------------------------------------------------

  static final class Args {
    String bucket;
    String prefix;
    String region;
    String warehouse;
    String namespace = "questdb";
    String catalogDb = "iceberg_catalog.db";
    String tsCol = "timestamp";
    String profile = null;
    TsMode mode = TsMode.V2;
    int sampleRows = 5;
    boolean rebuild = false;

    static Args parse(String[] argv) {
      Args a = new Args();
      for (int i = 0; i < argv.length; i++) {
        String k = argv[i];
        switch (k) {
          case "--bucket" -> a.bucket = argv[++i];
          case "--prefix" -> a.prefix = argv[++i];
          case "--region" -> a.region = argv[++i];
          case "--warehouse" -> a.warehouse = argv[++i];
          case "--namespace" -> a.namespace = argv[++i];
          case "--catalog-db" -> a.catalogDb = argv[++i];
          case "--ts-col" -> a.tsCol = argv[++i];
          case "--profile" -> a.profile = argv[++i];
          case "--timestamp-mode" -> a.mode = TsMode.valueOf(argv[++i].toUpperCase());
          case "--sample-rows" -> a.sampleRows = Integer.parseInt(argv[++i]);
          case "--rebuild" -> a.rebuild = true;
          default -> {
            System.err.println("unknown argument: " + k);
            System.exit(2);
          }
        }
      }
      require(a.bucket, "--bucket");
      require(a.prefix, "--prefix");
      require(a.region, "--region");
      require(a.warehouse, "--warehouse");
      return a;
    }

    private static void require(String v, String name) {
      if (v == null) {
        System.err.println("missing required argument: " + name);
        System.exit(2);
      }
    }
  }
}
