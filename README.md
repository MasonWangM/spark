# spark
a spark parctice

## 作业要求
- 统计 Backblaze 每天 Drive count/Drive failures
- 统计 Backblaze 每年 Drive failures count by brand

## local模式
* 先完成最基本的local模式
  * 手动下载数据至本地磁盘
  * 在本地打jar包并运行spark
  * 输出结果保存为csv
  * 执行命令： 
    * mvn clean package
    * spark-submit --class <className> <xxx.jar> 