import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

		val dataLoader = new DataLoader(spark)

		// Propriétés de connexion à PostgreSQL
		

		// Requête SQL pour charger les données
		val query = "(SELECT * FROM yelp.review) as q1"
		val postgresData = dataLoader.loadPostgresData(query)

		// Afficher les premières lignes pour vérifier
		postgresData.limit(1000).show()

		spark.stop()

	}
}
