package com.myspark.com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class RowCountByDate {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Row Count By Date")
                .master("local[*]")  // 这里使用local模式，实际运行时应根据集群环境设置
                .getOrCreate();

        // 读取CSV文件并创建DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true") // 第一行作为表头
                .option("inferSchema", "true") // 自动推断数据类型
                .csv("/Users/masonwang/Downloads/data_Q3_2019/*.csv");

        // 选择需要的列并过滤不合法的行
        Dataset<Row> filteredDF = df.select("date")
                .filter("date is not null");

        // 按日期分组并计算行数
        Dataset<Row> resultDF = filteredDF.groupBy("date")
                .agg(functions.count("date").alias("row_count"));

        // 将结果写入单个CSV文件
        resultDF.coalesce(1)  // 合并为一个分区，写入单个文件
                .write()
                .option("header", "true") // 输出文件包含表头
                .csv("output/row_count_by_date");

        System.out.println("Data saved successfully to output/row_count_by_date");

        spark.stop();
    }
}

