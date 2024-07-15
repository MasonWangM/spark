package com.myspark.com;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class FailureCountByDate {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Failure Count By Date")
                .master("local[*]")  // 这里使用local模式，实际运行时应根据集群环境设置
                .getOrCreate();

        // 读取CSV文件并创建DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true") // 第一行作为表头
                .option("inferSchema", "true") // 自动推断数据类型
                .csv("/Users/masonwang/Downloads/data_Q3_2019/*.csv");

        // 选择需要的列并过滤不合法的行
        Dataset<Row> filteredDF = df.select("date", "failure")
                .filter("date is not null and failure is not null");

        // 按日期分组并计算failure的总和
        Dataset<Row> resultDF = filteredDF.groupBy("date")
                .agg(functions.sum("failure").alias("total_failure"));

        // 将结果写入单个CSV文件
        resultDF.coalesce(1)  // 合并为一个分区，写入单个文件
                .write()
                .option("header", "true") // 输出文件包含表头
                .csv("output/failure_count_by_date");

        System.out.println("Data saved successfully to output/failure_count_by_date");

        spark.stop();
    }
}




