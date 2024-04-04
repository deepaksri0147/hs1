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
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;


public class SparkHudiGenerateData {



    public static void main(String[] args) throws IOException, URISyntaxException {
        long startTime = System.currentTimeMillis();
        String storageAccountName = "test1datalakestoragegen2";
        String storageAccountKey = "3K/fWSAN7/PJNPr7qBcH5idJfu8W96ISGXpbg8+i2vAYWR+D1LKZhVeEyuIIhEaXiOjXt9Osc9py+AStV4xdQw==";
        CsvMapping csvMapping = new CsvMapping();

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


//        String mappingId = "660c21352039b8009bae8fcd";
        String mappingId =  "660e89353b4a9e4855a3ba63";
        String tenantIdtoken = "Bearer_eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImZmOGYxNjhmLTNmZjYtNDZlMi1iMTJlLWE2YTdlN2Y2YTY5MCJ9.eyJwcm9maWxlVXJsIjoid3d3Lmdvb2dsZS5jb20vcHJvZmlsZS9waWMiLCJyZWNlbnRfc2Vzc2lvbiI6Ik5BIiwic3ViIjoiZ2FpYW4uY29tIiwicGFyZW50VGVuYW50SWQiOiJOQSIsImNvbG9yIjoiNjFkYWRjNGE3ZGI1NGRmMThlZDYzMzBhMWJhODJkZjYiLCJ1c2VyX25hbWUiOiJ0ZW5hbnRfdXNlcm5hbWUiLCJpc3MiOiJnYWlhbi5jb20iLCJpc0FkbWluIjp0cnVlLCJwbGF0Zm9ybUlkIjoiNjA0Nzg5ZWI0MmI3ZGMwMDAxN2E4MzQxIiwidXNlck5hbWUiOiJ0ZW5hbnRfdXNlcm5hbWUiLCJhdXRob3JpdGllcyI6WyJST0xFX01BUktFVFBMQUNFX1VTRVIiXSwiY2xpZW50X2lkIjoiZ2FpYW4iLCJzY29wZSI6WyJ0cnVzdCIsInJlYWQiLCJ3cml0ZSJdLCJ0ZW5hbnRJZCI6IjY0ZTFmZDNkMTQ0M2ViMDAwMThjYzIzMSIsImxvZ28iOiJ3d3cuZ29vZ2xlLmNvbS90ZW5hbnQvbG9nby9waWMucG5nIiwiZXhwIjoxNzA4MDI1NDIyLCJqdGkiOiI4ZDVmMjRkZS0wYWE4LTQ5ZjgtYWJjNS1jZjlhM2ViNmM5YmMiLCJlbWFpbCI6ImFwcHNAZ2FpYW5zb2x1dGlvbnMuY29tIn0.GykRloS1hNKe95XL2S56CrQDSxAG8uJ8GDEcKr892__CyhWGMFU5x2jaOuX71XkvHamV5cURNZQid5183G6163JJsIHY4EU6Ce7_KRtSFCBQl6XbUbfi1qzl3EWbvuvo0c6kJtHFoWm56jTX_6NGT9xmovufxtXhiW80DvxlV6zM6LAizYXwkhdOwGulNcKUe3mjR4v4StiIlUwv0fgYPY5DYNbU8fT9Z_U4RqOOeB8HBwqOQNFKSlZujQNlEcrvVlbxmDlCIoT9hV8kaY4o4_3RMbVYUqeNn8I-jKTj0GfhTWaFCr3VTaGvjzTABFJHTO_zLTETus299wAW22zZsQ";
        String transactionId = "c20d08ef-1f5c-4f33-9e34-041207f98c47";
        MappingDto mappingDto;

//              long start = System.currentTimeMillis();

//        try {
//            mappingDto = csvMapping.MappingConversion(mappingId,tenantIdtoken,transactionId);
//        } catch (Exception e) {
//            throw new RuntimeException("exception while having the restCall to get the mappingId");
//        }
//
//
//        System.out.println("size"+mappingDto.getColumnMapping().size());
//
//        for (String name: mappingDto.getColumnMapping().keySet()) {
//            String key = name;
//            String value = mappingDto.getColumnMapping().get(name);
//            System.out.println(key + " " + value);
//        }

        long start = System.currentTimeMillis();
//        String csvPath = "abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/politicalData.csv";

        String csvPath = "https://ig.aidtaas.com/mobius-content-service/v1.0/content/download/901fd42f-5dbf-4567-a08e-8069d5c18245";
        BufferedReader br = new BufferedReader(new InputStreamReader(new URI(csvPath).toURL().openStream()));

        // Read the first line from the CSV file
        String firstLine = br.readLine();
        // Split the first line into an array of strings based on the comma delimiter
        String[] columns = firstLine.split(",");
        // Print the array of column names
        System.out.println("Column names: " + Arrays.toString(columns));

        // Close the BufferedReader
        br.close();


        long end = System.currentTimeMillis();
        long between = end-start;
        System.out.println("time taken "+ between);





        System.out.println("sucessfully ran");




        }
    }


