import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, DateType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.functions._
import java.util.Properties



object Main {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

		val dataExtractor = new DataExtractor(spark)

		// val query = "(SELECT * FROM yelp.review) as q1"
		// val postgresData = dataExtractor.PostgresData(query)
		// postgresData.limit(1000).show()

		val schema = new StructType()
		.add("business_id", StringType, true)
		.add("compliment_count", IntegerType, true)
		.add("date", DateType, true)
		.add("text", StringType, true)
		.add("user_id", StringType, true)

		
		val csvOptions = Map("sep" -> ",", "nullValue" -> "NULL", "dateFormat" -> "yyyy-MM-dd")

		var csvLoad = dataExtractor.CSVData("../dataset/yelp_academic_dataset_tip.csv", schema, csvOptions)
		csvLoad.limit(50).show();



		//val jsonLoad = dataExtractor.JSONData("../dataset/yelp_academic_dataset_business.json")
		

		/*Class.forName("oracle.jdbc.driver.OracleDriver")
		val url = "jdbc:oracle:thin:@stendhal.iem:1521:enss2023" // Remplacez [hostname], [port] et [DBName] par vos informations

		import java.util.Properties
		val connectionProperties = new Properties()
		connectionProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
		connectionProperties.setProperty("user", "ld754555") // Remplacez par votre nom d'utilisateur
		connectionProperties.setProperty("password", "ld754555") // Remplacez par votre mot de passe

		// Enregistrement du DataFrame users dans la table "user"
		csvLoad.write
			.mode(SaveMode.Overwrite).jdbc(url, "ld754555.review", connectionProperties) // Remplacez nom_schema par le nom de votre schéma
*/

		val dl = new DataLoader();

		dl.loadToOracle(csvLoad, "ld754555.review")


		spark.stop()

	}
}
