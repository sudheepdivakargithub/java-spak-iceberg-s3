package org.example;

import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

  public static void main(String[] args) throws IOException {

    readThroughSpark();
  }

  static void readThroughSpark() {

    SparkSession spark = SparkSession.builder()
        .appName("IcebergReader")
        .master("spark://sc-fenceline-int-dev3.corp.picarro.com:7077")
//        .master("local[*]")
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.my_catalog.type", "hadoop")
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://picarro/fenceline/iceberg/")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.endpoint",
            "http://sc-fenceline-int-dev3.corp.picarro.com:4566")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access",
            "true").config("spark.executor.memory", "4g")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "2")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.executor.cores", "2")
        .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate();

    spark.sparkContext().setLogLevel("DEBUG");

    String table = "my_catalog.db.op_status";

    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load(table)
        .filter("system_id = '110791e3-47b3-460f-84e5-0caae7ae9ff8'");
//        .filter(
//            "timestamp == '2025-04-28T07:49:07.000Z'");

    List<Row> rowsList = df.collectAsList();

    df.collectAsList()
        .forEach(row -> {
          Double speed = row.getAs("wind_speed");
          Double direction = row.getAs("wind_direction");
          System.out.println("Speed: " + speed + ", Direction: " + direction);
        });

  }

//  static void readFenceLineData() {
//    long startTime = System.currentTimeMillis();
//    Catalog catalog = getCatalog();
//    TableIdentifier identifier = TableIdentifier.of("db", "anemometer_reading");
//    Table table = catalog.loadTable(identifier);
//    long tableLoadedTs = System.currentTimeMillis();
//    System.out.println("Table loading duration: " + (tableLoadedTs - startTime) + " ms");
//
//    Expression filter = Expressions.and(
//        Expressions.equal("system_id", "110791e3-47b3-460f-84e5-0caae7ae9ff8"),
//        Expressions.greaterThanOrEqual("timestamp", "2025-04-22T07:41:30.200Z"),
//        Expressions.lessThan("timestamp", "2025-04-22T07:41:30.300Z")
//    );
//    try (CloseableIterable<Record> records = IcebergGenerics.read(table)
//        .where(filter)
//        .build()) {
//
//      Iterator<Record> iterator = records.iterator();
//      int count = 0;
//
//      while (iterator.hasNext()) {
//        Record record = iterator.next();
//        System.out.println(record);
//        count++;
//      }
//
//      System.out.println("Total records found: " + count);
//
//      long endTime = System.currentTimeMillis();
//      System.out.println("Execution time: " + (endTime - startTime) + " ms");
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  static void readTestData() {
//    Catalog catalog = getCatalog();
//    TableIdentifier identifier = TableIdentifier.of("demo", "sample_table");
//    Table table = catalog.loadTable(identifier);
//
//    // Define timestamp range
////        long start = OffsetDateTime.now().minusDays(10).toEpochSecond();
////        long end = OffsetDateTime.now().minusDays(1).toEpochSecond();
//
//    // Build filter
//    Expression filter = Expressions.equal("system_id", "system-5");
//
////        Expression filter = Expressions.and(
////                Expressions.greaterThanOrEqual("timestamp", ZonedDateTime.parse("2025-03-24T18:55:15.825745Z").toEpochSecond()
////                ), Expressions.lessThanOrEqual("timestamp", ZonedDateTime.parse("2025-04-02T18:55:15.825760Z").toEpochSecond()));
//
//    try (CloseableIterable<Record> records = IcebergGenerics.read(table)
//        .where(filter)
//        .build()) {
//
//      Iterator<Record> iterator = records.iterator();
//      int count = 0;
//
//      while (iterator.hasNext()) {
//        Record record = iterator.next();
//        System.out.println(record);
//        count++;
//      }
//
//      System.out.println("Total records found: " + count);
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  static long toEpochMicros(Instant instant) {
//    return instant.getEpochSecond() * 1_000_000 + (instant.getNano() / 1_000);
//  }

//    static void writeToIceberg() throws IOException {
//
//
//        Catalog catalog = getCatalog();
//
//        Schema schema = new Schema(
//                Types.NestedField.required(1, "id", Types.StringType.get()),
//                Types.NestedField.optional(2, "system_id", Types.StringType.get()),
//                Types.NestedField.optional(3, "timestamp", Types.TimestampType.withZone()),
//                Types.NestedField.optional(4, "value", Types.IntegerType.get())
//        );
//
//        // Define partition spec
//        PartitionSpec spec = PartitionSpec.builderFor(schema)
//                .day("timestamp")
//                .build();
//
//        // Create table identifier
//        TableIdentifier identifier = TableIdentifier.of("demo", "sample_table");
//
//        // Drop if exists
//        if (catalog.tableExists(identifier)) {
//            catalog.dropTable(identifier, true);
//        }
//
//        // Create table
//        Table table = catalog.createTable(identifier, schema, spec);
//        // Write 1M records
//        File dataFile = new File("/tmp/");
//
//        try (FileAppender<Record> appender = Parquet.write(Files.localOutput(dataFile.toPath().toFile()))
//                .schema(schema)
//                .createWriterFunc(GenericParquetWriter::buildWriter)
//                .build()) {
//
//            GenericRecord record = GenericRecord.create(schema);
//            Random random = new Random();
//
//            for (int i = 0; i < 1_000_000; i++) {
//                record.setField("id", UUID.randomUUID().toString());
//                record.setField("system_id", "system-" + (random.nextInt(10) + 1)); // 10 different systems
//                record.setField("timestamp", OffsetDateTime.now().minusDays(random.nextInt(30))); // past 30 days
//                record.setField("value", random.nextInt(100) + 1); // 1 to 100
//                appender.add(record);
//            }
//
//            System.out.println("Finished writing 1M records to Parquet");
//        }
//
//        // Add the data file to the table
//        DataFile data = DataFiles.builder(spec)
//                .withPath(dataFile.getAbsolutePath())
//                .withFileSizeInBytes(dataFile.length())
//                .withRecordCount(1)
//                .withPartitionPath("timestamp_day=" + java.time.LocalDate.now())  // simplistic example
//                .build();
//
//        table.newAppend()
//                .appendFile(data)
//                .commit();
//
//        System.out.println("Table created and data inserted successfully.");
//
//
//    }

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
//  static Catalog getCatalog() {
//    Configuration conf = new org.apache.hadoop.conf.Configuration();
//    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//    conf.set("fs.s3a.endpoint", "http://sc-fenceline-int-dev3.corp.picarro.com:4566");
//    conf.set("fs.s3a.access.key", "test");
//    conf.set("fs.s3a.secret.key", "test");
//    conf.set("fs.s3a.path.style.access", "true");
//
//    return new HadoopCatalog(conf, "s3a://picarro/fenceline/iceberg");
//  }

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


}





