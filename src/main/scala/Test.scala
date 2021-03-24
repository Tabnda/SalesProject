import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Test extends App  {
  case class TrainData(item_Identifier : String, Item_Weight : Double,
                   Item_Fat_Content: String,Item_Visibility: Double,Item_Type:String,Item_MRP :Double,
                   Outlet_Identifier : String,Outlet_Establishment_Year: Int,Outlet_Size : String,
                   Outlet_Location_Type : String,Outlet_Type : String,
                   Item_Outlet_Sales:Double,Gender:String,Age : Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Dataset")

  val spark = SparkSession
    .builder
    .master("Local[*]")
    .appName("Test")
    .getOrCreate()


val TrainstructureSchema = new StructType()
  .add("item_Identifier",StringType)
  .add("Item_Weight",DoubleType)
  .add("Item_Fat_Content",StringType)
  .add("Item_Visibility",DoubleType)
  .add("Item_Type",StringType)
  .add("Item_MRP",DoubleType)
  .add("Outlet_Identifier",StringType)
  .add("Outlet_Establishment_Year",IntegerType)
  .add("Outlet_Size",StringType)
  .add("Outlet_Location_Type",StringType)
  .add("Outlet_Type",StringType)
  .add("Item_Outlet_Sales",DoubleType)
  .add("Gender",StringType)
  .add("Age",IntegerType)



import spark.implicits._
  val Train = spark.read
    .option("header", "false")
    .schema(TrainstructureSchema)
    .option("inferSchema", "true")
    .csv("data/Train-1.csv")
   // .as[TrainData]




  //Train.show(2)
  //val checksizeTrain = Train.count()
  //println(checksizeTrain)

  val TestData = spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("data/Test-1.csv")



  //val checksizeTest = Test.count()
  //println(checksizeTest)


  //Test.show(2)



  //val union = Train.union(Test)
  //val Totalsize = union.count()
  //println(Totalsize)


}