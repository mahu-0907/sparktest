package Core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/*
    wordcount 案例测试
 */
object wordcount {
  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")   //设置日志显示的级别，减少干扰信息
    // 读取文件数据
    val fileRDD: RDD[String] = sc.textFile("data/word.txt")
    // 将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap( _.split(" ") )
    // 转换数据结构 word => (word, 1)
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    //设置隐式转换参数的降序排列
    implicit val ord = new Ordering[Int](){
      override def compare(x: Int, y: Int): Int = y.compareTo(x)
    }
    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_).sortBy(_._2)
    // 将数据聚合结果采集到内存中

    val word2Count: Array[(String, Int)] = word2CountRDD.collect()
    // 打印结果
    word2Count.foreach(println)
    println("我是版本2的修改！")
    println("这是在 hot-fix 分支上修改的代码！")
    //关闭Spark连接
    sc.stop()
  }
}
