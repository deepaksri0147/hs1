package org.cloud.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SaveMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;


public class SparkHudiGenerateData {


    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String storageAccountName = "test1datalakestoragegen2";
        String storageAccountKey = "3K/fWSAN7/PJNPr7qBcH5idJfu8W96ISGXpbg8+i2vAYWR+D1LKZhVeEyuIIhEaXiOjXt9Osc9py+AStV4xdQw==";


        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("className", "org.apache.hudi")
                .set("hoodie.datasource.write.commit_time_extractor.class", "com.example.EpochTimeCommitExtractor")
                .set("spark.sql.hive.convertMetastoreParquet", "false")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
                .set("spark.sql.warehouse.dir", "/home/gaian/spark-warehouse")
                .set("fs.defaultFS", "abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/")
                .set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", storageAccountKey);

        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();


        String mappingId = "660c21352039b8009bae8fcd";
        String tenantIdtoken = "Bearer_eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImZmOGYxNjhmLTNmZjYtNDZlMi1iMTJlLWE2YTdlN2Y2YTY5MCJ9.eyJwcm9maWxlVXJsIjoid3d3Lmdvb2dsZS5jb20vcHJvZmlsZS9waWMiLCJyZWNlbnRfc2Vzc2lvbiI6Ik5BIiwic3ViIjoiZ2FpYW4uY29tIiwicGFyZW50VGVuYW50SWQiOiJOQSIsImNvbG9yIjoiNjFkYWRjNGE3ZGI1NGRmMThlZDYzMzBhMWJhODJkZjYiLCJ1c2VyX25hbWUiOiJ0ZW5hbnRfdXNlcm5hbWUiLCJpc3MiOiJnYWlhbi5jb20iLCJpc0FkbWluIjp0cnVlLCJwbGF0Zm9ybUlkIjoiNjA0Nzg5ZWI0MmI3ZGMwMDAxN2E4MzQxIiwidXNlck5hbWUiOiJ0ZW5hbnRfdXNlcm5hbWUiLCJhdXRob3JpdGllcyI6WyJST0xFX01BUktFVFBMQUNFX1VTRVIiXSwiY2xpZW50X2lkIjoiZ2FpYW4iLCJzY29wZSI6WyJ0cnVzdCIsInJlYWQiLCJ3cml0ZSJdLCJ0ZW5hbnRJZCI6IjY0ZTFmZDNkMTQ0M2ViMDAwMThjYzIzMSIsImxvZ28iOiJ3d3cuZ29vZ2xlLmNvbS90ZW5hbnQvbG9nby9waWMucG5nIiwiZXhwIjoxNzA4MDI1NDIyLCJqdGkiOiI4ZDVmMjRkZS0wYWE4LTQ5ZjgtYWJjNS1jZjlhM2ViNmM5YmMiLCJlbWFpbCI6ImFwcHNAZ2FpYW5zb2x1dGlvbnMuY29tIn0.GykRloS1hNKe95XL2S56CrQDSxAG8uJ8GDEcKr892__CyhWGMFU5x2jaOuX71XkvHamV5cURNZQid5183G6163JJsIHY4EU6Ce7_KRtSFCBQl6XbUbfi1qzl3EWbvuvo0c6kJtHFoWm56jTX_6NGT9xmovufxtXhiW80DvxlV6zM6LAizYXwkhdOwGulNcKUe3mjR4v4StiIlUwv0fgYPY5DYNbU8fT9Z_U4RqOOeB8HBwqOQNFKSlZujQNlEcrvVlbxmDlCIoT9hV8kaY4o4_3RMbVYUqeNn8I-jKTj0GfhTWaFCr3VTaGvjzTABFJHTO_zLTETus299wAW22zZsQ";
       // MappingDto mappingDto;
//        try {
//            mappingDto = MappingConversion(mappingId,tenantIdtoken);
//        } catch (Exception e) {
//            throw new RuntimeException("exception while having the restCall to get the mappingId");
//        }


        StructType schema = new StructType()
                .add("precinct", DataTypes.StringType, false)
                .add("office", DataTypes.StringType, false)
                .add("partydetailed", DataTypes.StringType, false)
                .add("partysimplified", DataTypes.StringType, false)
                .add("mode", DataTypes.StringType, false)
                .add("votes", DataTypes.IntegerType, false)
                .add("countyname", DataTypes.StringType, false)
                .add("countyfips", DataTypes.StringType, false)
                .add("jurisdictionname", DataTypes.StringType, false)
                .add("jurisdictionfips", DataTypes.StringType, false)
                .add("candidate", DataTypes.StringType, false)
                .add("district", DataTypes.StringType, false)
                .add("magnitude", DataTypes.StringType, false)
                .add("dataverse", DataTypes.StringType, false)
                .add("yearofvote", DataTypes.IntegerType, false)
                .add("Id", DataTypes.DoubleType, false)
                .add("stage", DataTypes.StringType, false)
                .add("state", DataTypes.StringType, false)
                .add("special", DataTypes.StringType, false)
                .add("writein", DataTypes.StringType, false)
                .add("statepo", DataTypes.StringType, false)
                .add("statefips", DataTypes.StringType, false)
                .add("statecen", DataTypes.StringType, false)
                .add("stateic", DataTypes.StringType, false)
                .add("dateofelection", DataTypes.StringType, false)
                .add("readmecheck", DataTypes.StringType, false);


        // Path to CSV file
//        String csvPath = "file:////Users/deepak/Downloads/hs-1/src/main/resources/b.csv";
        String csvPath = "abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/politicalData.csv";
//        String csvPath = "https://testmobiusfileshare.blob.core.windows.net/test/_615e8b5397b94d000155448c/GAIAN/Downloads/2ce84101-6c63-476d-9219-1fd4afa7e9f2_$$_V1_TEST.csv";

        // Load CSV file into DataFrame
        Dataset<Row> data = spark.read()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load(csvPath);

        // Rename column "Name" to "empName" in the dataset
//        Dataset<Row> renamedData = data.withColumnRenamed("Name", "empName");


        data.write().format("org.apache.hudi").option(PARTITIONPATH_FIELD_NAME.key(), "state")
                .option("hoodie.table.name", "Spark_109")
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), HoodieTableType.MERGE_ON_READ.name())
                .mode(SaveMode.Append)
                .save("abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/Spark_109");
        System.out.println("Number of lines in file = " + data.count());
        long endtime = System.currentTimeMillis();
        long elapsedTime = endtime - startTime;
        System.out.println("time take to insert is " + elapsedTime);
        for (String fieldName : data.schema().fieldNames()) {
            System.out.println("Field Name: " + fieldName);


        }
    }
}











//
//public static void main(String[] args) {
////        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
////        System.setProperty("java.library.path","D:\\sparksetup\\hadoop\\bin");
//
//    String storageAccountName = "test1datalakestoragegen2";
//    String storageAccountKey = "3K/fWSAN7/PJNPr7qBcH5idJfu8W96ISGXpbg8+i2vAYWR+D1LKZhVeEyuIIhEaXiOjXt9Osc9py+AStV4xdQw==";
//
//
//    SparkConf sparkConf = new SparkConf()
//            .setAppName("Example Spark App")
////                .setMaster("local[*]")
//            .setMaster("spark://172.212.74.174:7077")
//            .set("className", "org.apache.hudi")
//            .set("hoodie.datasource.write.commit_time_extractor.class", "com.example.EpochTimeCommitExtractor")
//            .set("spark.sql.hive.convertMetastoreParquet", "false")
//            .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//            .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.hudi.catalog.HoodieCatalog")
//            .set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
//            .set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar")
//            .set("spark.sql.warehouse.dir", "/home/gaian/spark-warehouse")
//            .set("fs.defaultFS", "abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/")
//            .set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", storageAccountKey);
//
//    SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();
//
//
//
//
//
//    StructType structType = new StructType();
//    structType = structType.add("ts", DataTypes.LongType, false);
//    structType = structType.add("uuid", DataTypes.StringType, false);
//    structType = structType.add("rider", DataTypes.StringType, false);
//    structType = structType.add("driver", DataTypes.StringType, false);
//    structType = structType.add("fare", DataTypes.DoubleType, false);
//    structType = structType.add("city", DataTypes.StringType, false);
//
//    List<Row> nums = new ArrayList<Row>();
//    nums.add(RowFactory.create(1695259648187L,"321e26a9-8355-45cc-97c6-c31daf0da310","praveen-A","praveen-K",7.10,"Delhi"));
//    nums.add(RowFactory.create(1695216138116L,"e21f431c-889d-4015-bc98-59bdce1e331c","kiran-F","kiran-P",10.15,"UP" ));
//    nums.add(RowFactory.create(1695215998111L,"c31bbe69-8d89-47ea-b4ce-4d224bae4b1a","naveen-J","naveen-T",1050.85,"Shimla"));
//
//
//    Dataset<Row> dataset = spark.createDataFrame(nums, structType);
//    dataset.write().format("org.apache.hudi").option(PARTITIONPATH_FIELD_NAME.key(),"uuid")
//            //.option("hoodie.metadata.record.index.enable","true")
//            //.option("hoodie.index.type","RECORD_INDEX")
//            //.option("hoodie.metadata.enable","true")
//            //.option("hoodie.metadata.index.bloom.filter.enable","true")
//            //.option("hoodie.metadata.index.column.stats.enable","true")
//            .option("hoodie.table.name","Spark_06")
//            .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), HoodieTableType.MERGE_ON_READ.name())
//            .mode(SaveMode.Append)
//            .save("abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/Spark_06");
//    System.out.println("Number of lines in file = " + dataset.count());
//}

