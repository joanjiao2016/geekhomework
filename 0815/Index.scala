import org.apache.spark.{SparkConf, SparkContext}

object ReadDirectort {
  def main(args: Array[String]): Unit = {

val sparkconf = new SparkConf()
  .setAppName("words")
  .setMaster("local[1]")
    val sc = new SparkContext(sparkconf)

    val data = sc.wholeTextFiles("hdfs://cdh2:8020/user/geekjoan")
    //使用分割"/''获取文件名
    val r1 = data.flatMap { x =>
      val doc = x._1.split("/").last
      //先按行切分,在按列空格进行切分
      x._2.split("\r\n").flatMap(_.split(" ").map { y => (y, doc)})}
    .groupByKey.map{case(x,y)=>(x,y.toSet.mkString(","))}
    r1.foreach(println)
  }


