package org.cloud.data;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.json.simple.parser.ParseException;

import java.io.IOException;



public class SparkHudiReadData {

    public static void main(String[] args) throws IOException, ParseException {
        System.out.println("Hello World");
        long time = System.currentTimeMillis();


//        String query =args[0];
//        String tables = args[1];
//        String tiDBTable = args[2];


        String query ="SELECT * FROM Spark_101";
        String tables = "Spark_101";

//        List<String> oldresultList = Arrays.asList(tables.split(", "));
//        List<String> resultList = new ArrayList<>();
//        for(String contanicatetable : oldresultList){
//            resultList.add("t_"+contanicatetable+"_t");
//        }


        String url = "jdbc:mysql://20.237.110.90:4000/targettingFramework?useSSL=false";
        String username = "root";
        String password = "";

        String storageAccountName = "test1datalakestoragegen2";
        String storageAccountKey = "3K/fWSAN7/PJNPr7qBcH5idJfu8W96ISGXpbg8+i2vAYWR+D1LKZhVeEyuIIhEaXiOjXt9Osc9py+AStV4xdQw==";

        System.out.println("@@@@@@@@@@@@@");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.sql.warehouse.dir", "/home/gaian/spark-warehouse")
                .set("fs.defaultFS", "abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/")
                .set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", storageAccountKey);

        SparkSession spark = SparkSession.builder().appName("Spark Hudi Read").config(sparkConf).getOrCreate();


//        for (String tableName : resultList) {
            String loadPath = "abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/" + tables;
            Dataset<?> trips_df = spark.read().format("org.apache.hudi").load(loadPath);
            trips_df.createOrReplaceTempView(tables);
//        }
        spark.sql(query).show(false);

//        Dataset<Row> df = spark.sql(query);
//        UUID uuid = UUID.randomUUID();
//
//        long mostSignificantBits = uuid.getMostSignificantBits();
//
//        df.withColumn("ID", functions.lit(mostSignificantBits))
//                .write()
//                .format("jdbc")
//                .option("url", url)
//                .option("dbtable", tiDBTable)
//                .option("user", username)
//                .option("password", password)
//                .option("driver","com.mysql.cj.jdbc.Driver")
//                .mode("append") // You can change this to "overwrite" or "ignore" as needed
//                .save();
//
//
//        long current = System.currentTimeMillis() - time;
//        System.out.println("Execution time: " + current + " milliseconds");

    }
}










