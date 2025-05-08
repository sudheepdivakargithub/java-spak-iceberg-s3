package org.example;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

public class Main {

  static String warehousePath = "s3a://cdp-wind-data-test/fenceline/iceberg/";
  static String iceberg_table_name = "anemometer_reading_java";
  static Schema schema = new Schema(
      Types.NestedField.required(1, "system_id", Types.StringType.get()),
      Types.NestedField.optional(2, "timestamp", Types.TimestampType.withZone()),
      Types.NestedField.optional(3, "wind_speed", Types.DoubleType.get()),
      Types.NestedField.optional(4, "wind_direction", Types.IntegerType.get())
  );
  static PartitionSpec spec = PartitionSpec.builderFor(schema)
      .identity("system_id")
      .day("timestamp")
      .build();

  public static void main(String[] args) throws IOException {
//    createIcebergTables();
    writeWindData();
//    readFenceLineData();
//    writeToIceberg();
//    readTestData();
  }


  static void createIcebergTables() throws IOException {

    // Set up Hadoop Configuration
    Configuration conf = getConfiguration();

    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    Namespace namespace = Namespace.of("db");
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, iceberg_table_name);

    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.FORMAT_VERSION, "2");
    properties.put("write.format.default", "parquet");

    Table table = catalog.createTable(tableIdentifier, schema, spec, properties);

    System.out.println("Iceberg table created at: " + table.location());

    catalog.close();
  }

  static Configuration getConfiguration() {
    Configuration conf = new Configuration();

    return conf;
  }

  static Catalog getCatalog() {
    return new HadoopCatalog(getConfiguration(), warehousePath);
  }

  static void writeWindData() throws IOException {
    long startTs = System.nanoTime();
    TableIdentifier tableIdentifier = TableIdentifier.of("db", iceberg_table_name);
    Catalog catalog = new HadoopCatalog(getConfiguration(),
        warehousePath);
    Table table = catalog.loadTable(tableIdentifier);
    InternalRecordWrapper internalRecordWrapper = new InternalRecordWrapper(
        table.schema().asStruct());
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.spec().schema());
    int partitionId = 0;
    int taskId = 0;

    GenericRecord record = GenericRecord.create(schema);

    record.setField("system_id", "system_123");
    // Use java.time.OffsetDateTime for timestamp with timezone
    record.setField("timestamp", OffsetDateTime.now(ZoneOffset.UTC));
    record.setField("wind_speed", 15.5);
    record.setField("wind_direction", 290);

    internalRecordWrapper.wrap(record);

    OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(FileFormat.PARQUET)
        .build();

    PartitionedFanoutWriter<Record> writer = new PartitionedFanoutWriter<Record>(
        table.spec(),
        FileFormat.PARQUET,
        appenderFactory,
        outputFileFactory,
        table.io(),
        128 * 1024 * 1024
    ) {
      @Override
      protected PartitionKey partition(Record rec) {
        partitionKey.partition(internalRecordWrapper);
        return partitionKey;
      }
    };

    writer.write(record);

    AppendFiles append = table.newAppend();
    for (DataFile dataFile : writer.dataFiles()) {
      append.appendFile(dataFile);
    }
    append.commit();

    writer.close();
    long endTs = System.nanoTime();

    System.out.printf("Record written successfully in %s sec",
        TimeUnit.SECONDS.convert((endTs - startTs), TimeUnit.NANOSECONDS));

  }


  static void readFenceLineData() {
    long startTime = System.nanoTime();
    Catalog catalog = new HadoopCatalog(getConfiguration(), warehousePath);
    TableIdentifier identifier = TableIdentifier.of("db", iceberg_table_name);
    Table table = catalog.loadTable(identifier);
    long tableLoadedTs = System.nanoTime();
    System.out.println(
        "Table loading duration: " + TimeUnit.SECONDS.convert((tableLoadedTs - startTime),
            TimeUnit.NANOSECONDS) + " sec");

    try (CloseableIterable<Record> records = IcebergGenerics.read(table)
//        .where(Expressions.equal("system_id", "fc484275-1e62-4091-aff2-69a3b3ed03e4"))
        .build()) {

      Iterator<Record> iterator = records.iterator();
      int count = 0;

      while (iterator.hasNext()) {
        Record record = iterator.next();
        System.out.println(record);
        count++;
      }

      System.out.println("Total records found: " + count);

      long endTime = System.nanoTime();
      System.out.println(
          "Execution time: " + TimeUnit.SECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS)
              + " sec");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  //
  static void readTestData() {
    long startTimeNano = System.nanoTime();
    Catalog catalog = getCatalog();
    TableIdentifier identifier = TableIdentifier.of("demo", "sample_table");
    Table table = catalog.loadTable(identifier);
    System.out.println(table.schema().asStruct());
    // Define timestamp range
//        long start = OffsetDateTime.now().minusDays(10).toEpochSecond();
//        long end = OffsetDateTime.now().minusDays(1).toEpochSecond();

    // Build filter
    Expression filter = Expressions.equal("id", "21e87b6e-f4be-4c83-bf72-f8ce7c285d32");

//  / /        Expression filter = Expressions.and( / Expressions.greaterThanOrEqual("timestamp",
//  / ZonedDateTime.parse("2025-03-24T18:55:15.825745Z").toEpochSecond() /                ),
//  / Expressions.lessThanOrEqual("timestamp",
//  / ZonedDateTime.parse("2025-04-02T18:55:15.825760Z").toEpochSecond()));

    try (CloseableIterable<Record> records = IcebergGenerics.read(table)
        .select("system_id", "timestamp", "value")
        .where(filter)
        .build()) {
      int count = 0;
      for (Record record : records) {
        System.out.println(record);
        count++;
      }
      long endTime = System.nanoTime();
      System.out.println("Total records found: " + count);
      System.out.printf("Result found in: %s Sec %n",
          TimeUnit.SECONDS.convert(endTime - startTimeNano, TimeUnit.NANOSECONDS));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  static void writeToIceberg() throws IOException {

    Catalog catalog = getCatalog();

    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.optional(2, "system_id", Types.StringType.get()),
        Types.NestedField.optional(3, "timestamp", Types.TimestampType.withZone()),
        Types.NestedField.optional(4, "value", Types.IntegerType.get())
    );

    // Define partition spec
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("system_id")
        .day("timestamp")
        .build();

    // Create table identifier
    TableIdentifier identifier = TableIdentifier.of("demo", "sample_table");

    // Drop if exists
    if (catalog.tableExists(identifier)) {
      catalog.dropTable(identifier, true);
    }

    // Create table
    Table table = catalog.createTable(identifier, schema, spec);

//    ---------------------------
    FileFormat format = FileFormat.PARQUET;
    OutputFile outputFile = table.io().newOutputFile(
        table.locationProvider()
            .newDataLocation(UUID.randomUUID() + "." + format.name().toLowerCase())
    );

    try (FileAppender<Record> appender = Parquet.write(
            outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {

      GenericRecord record = GenericRecord.create(schema);
      Random random = new Random();

      for (int i = 0; i < 1_000_000; i++) {
        record.setField("id", UUID.randomUUID().toString());
        record.setField("system_id", "system-" + (random.nextInt(10) + 1)); // 10 different systems
        record.setField("timestamp",
            OffsetDateTime.now().minusDays(random.nextInt(30))); // past 30 days
        record.setField("value", random.nextInt(100) + 1); // 1 to 100
        appender.add(record);
      }

      System.out.println("Finished writing 1M records to Parquet");
    }

    DataFile dataFile = DataFiles.builder(spec)
        .withInputFile(outputFile.toInputFile())
        .withRecordCount(1_000_000)
        .withFormat(format)
        .build();

    table.newAppend()
        .appendFile(dataFile)
        .commit();

    System.out.println("Table created and data inserted successfully.");


  }

  //  static void dropTable() {
//    Catalog catalog = getCatalog();
//    TableIdentifier identifier = TableIdentifier.of("demo", "sample_table");
//
//    // Drop if exists
//    if (catalog.tableExists(identifier)) {
//      catalog.dropTable(identifier, true);
//    }
//
//    System.out.println("Drooped table");
//  }
//

//    static void migrate() throws IOException {
//        // 1. Setup Hadoop catalog
//
//        Catalog catalog = getCatalog();
//
//        // 2. Load source and target table
//        TableIdentifier sourceId = TableIdentifier.of("db", "anemometer_reading");
//        TableIdentifier targetId = TableIdentifier.of("db", "anemometer_reading_partitioned");
//
//        Table sourceTable = catalog.loadTable(sourceId);
//
//        // 3. Define the partition spec: system_id, days(timestamp)
//        PartitionSpec spec = PartitionSpec.builderFor(sourceTable.schema())
//                .identity("system_id")
//                .day("timestamp")
//                .build();
//
//        // 4. Create target table if not exists
//        if (catalog.tableExists(targetId)) {
//            catalog.dropTable(targetId); // Drop if exists
//        }
//        Table targetTable = catalog.createTable(targetId, sourceTable.schema(), spec);
//
//        // 5. Setup the file for writing
//        String fileName = UUID.randomUUID() + ".parquet";
//        OutputFile outputFile = targetTable.io().newOutputFile(targetTable.location() + "/data/" + fileName);
//
//        // 6. Write to the Parquet file using FileAppender
//        FileAppender<Record> appender = Parquet.write(outputFile)
//                .schema(targetTable.schema())
//                .createWriterFunc(GenericParquetWriter::buildWriter)
//                .overwrite()
//                .build();
//
//        // 7. Read records and write to Parquet
//        long count = 0;
//        String partitionPath = null;
//        try (CloseableIterable<Record> records = IcebergGenerics.read(sourceTable).build()) {
//            for (Record record : records) {
//                // Compute partition path for the first record (for demonstration)
//                if (partitionPath == null) {
//                    partitionPath = computePartitionPath(record, spec); // Compute partition for first record
//                }
//                appender.add(record);
//                count++;
//                if (count >= 1_000_000) break;
//            }
//        }
//
//        // 8. Finalize the appender to flush the file
//        appender.close();
//
//        // 9. Create DataFile with partition info and commit
//        DataFile dataFile = DataFiles.builder(spec)
//                .withPath(outputFile.location())
//                .withFormat(FileFormat.PARQUET)
//                .withFileSizeInBytes(appender.length())
//                .withPartitionPath(partitionPath)  // Use the computed partition path
//                .withRecordCount(count)
//                .build();
//
//        AppendFiles append = targetTable.newAppend();
//        append.appendFile(dataFile);
//        append.commit();
//
//        System.out.println("✅ Migrated " + count + " records to partitioned table.");
//    }
//
//    private static String computePartitionPath(Record record, PartitionSpec spec) {
//        String systemId = record.getField("system_id").toString();
//        long timestampMillis = ((OffsetDateTime) record.getField("timestamp")).toInstant().toEpochMilli();
//        String timestampDay = new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestampMillis));
//
//        // Manually build partition path: system_id=a123/timestamp_day=2024-01-01
//        return String.format("system_id=%s/timestamp_day=%s", systemId, timestampDay);
//    }
//  static void readThroughSpark() {
//
//    SparkSession spark = SparkSession.builder()
//        .appName("IcebergReader")
//        .master("spark://sc-fenceline-int-dev3.corp.picarro.com:7077")
////        .master("local[*]")
//        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
//        .config("spark.sql.catalog.my_catalog.type", "hadoop")
//        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://picarro/fenceline/iceberg/")
//        .config("spark.hadoop.fs.s3a.access.key", "test")
//        .config("spark.hadoop.fs.s3a.secret.key", "test")
//        .config("spark.hadoop.fs.s3a.endpoint",
//            "http://sc-fenceline-int-dev3.corp.picarro.com:4566")
//        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
//            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.path.style.access",
//            "true").config("spark.executor.memory", "4g")
//        .config("spark.dynamicAllocation.enabled", "true")
//        .config("spark.dynamicAllocation.minExecutors", "1")
//        .config("spark.dynamicAllocation.maxExecutors", "2")
//        .config("spark.scheduler.mode", "FAIR")
//        .config("spark.executor.cores", "2").config("spark.sql.catalog.my_catalog.catalog-impl",
//            "org.apache.iceberg.hadoop.HadoopCatalog")
////        .config("spark.sql.extensions",
////            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
////        .config("spark.sql.catalog.my_catalog.catalog-impl",
////            "org.apache.iceberg.hadoop.HadoopCatalog")
//        .getOrCreate();
//
//    spark.sparkContext().setLogLevel("DEBUG");
//
//    String table = "my_catalog.db.anemometer_reading";
//
//    Dataset<Row> df = spark.read()
//        .format("iceberg")
//        .load(table)
//        .filter("system_id = '110791e3-47b3-460f-84e5-0caae7ae9ff8'")
//        .filter(
//            "timestamp == '2025-04-28T07:49:07.000Z'");
//

  /// /    List<Row> rowsList = df.collectAsList(); /    df.show(); /    df.collectAsList() /
  /// .forEach(row -> { /          Double speed = row.getAs("wind_speed"); /          Double
  /// direction = row.getAs("wind_direction"); /          System.out.println("Speed: " + speed + ",
  /// Direction: " + direction); /        });
//
//  }

}





