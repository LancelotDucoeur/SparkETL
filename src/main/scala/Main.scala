import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, DateType}
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
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

		val schemaTip = new StructType()
		.add("business_id", StringType, true)
		.add("compliment_count", IntegerType, true)
		.add("date", DateType, true)
		.add("text", StringType, true)
		.add("user_id", StringType, true)
		val csvOptions = Map("sep" -> ",", "nullValue" -> "NULL", "dateFormat" -> "yyyy-MM-dd")
		var tipLoad = dataExtractor.CSVData("../dataset/yelp_academic_dataset_tip.csv", schemaTip, csvOptions)
		
		
		var businessLoad = dataExtractor.JSONData("../dataset/yelp_academic_dataset_business.json")
		
		// Liste des mots autorisés
		val allowedWords = Set("Food", "Fast Food")

		def containsOnlyAllowedWords(s: String, allowedWords: Set[String]): Boolean = {
			if (s == null) return false
			s.split(", ").exists(allowedWords.contains)
		}


		// Enregistrer la fonction UDF
		val containsOnlyAllowedWordsUDF = udf((s: String) => containsOnlyAllowedWords(s, allowedWords))

		businessLoad = businessLoad.filter(containsOnlyAllowedWordsUDF(col("categories")))


		// Créer le DataFrame 'location' avec une colonne 'location_id' unique
		val selectedColumns: Array[String] = Array("latitude", "longitude", "state", "city", "postal_code", "address")
		val locationDF = businessLoad
		.select(selectedColumns.head, selectedColumns.tail: _*)
		.distinct() // Assurez-vous que les emplacements sont uniques
		.withColumn("location_id", monotonically_increasing_id())

		// Aliasser les DataFrames avant de les joindre pour éviter les ambiguïtés
		val businessDF = businessLoad.as("business")
		.join(locationDF.as("location"), 
				col("business.latitude") === col("location.latitude") &&
				col("business.longitude") === col("location.longitude") &&
				col("business.state") === col("location.state") &&
				col("business.city") === col("location.city") &&
				col("business.postal_code") === col("location.postal_code") &&
				col("business.address") === col("location.address")
		)
		.select(
			col("business.business_id"),
			col("location.location_id"), // Sélection qualifiée pour éviter l'ambiguïté
			col("business.stars"),
			col("business.review_count")
		)



		val categoriesDF: DataFrame = businessLoad
		.select(explode(split(col("categories"), ", ")).alias("name"))
		.distinct()
		.withColumn("category_id", monotonically_increasing_id())

		var businessCategories: DataFrame = businessLoad
		.withColumn("category", explode(split(col("categories"), ", ")))
		.select("business_id", "category")

		businessCategories = businessCategories
		.join(categoriesDF, businessCategories("category") === categoriesDF("name"))
		.select(businessCategories("business_id"), categoriesDF("category_id"))

		/*println(s"Nombre de lignes : ${categoriesDF.count()}")

		// DataFrame contenant les `business_id` et les catégories séparées
		val businessCategoriesDF = businessLoad
		.withColumn("category", explode(split(col("categories"), ", ")))
		.select("business_id", "category")

		// Joindre avec le DataFrame des catégories pour obtenir le `category_id`
		val businessCategoryIdsDF = businessCategoriesDF
		.join(categoriesDF, businessCategoriesDF("category") === categoriesDF("name"))
		.select(businessCategoriesDF("business_id"), categoriesDF("category_id"))

		businessCategoryIdsDF.show()
		*/

		//val dl = new DataLoader();

		//dl.loadToOracle(location, "ld754555.location")


		spark.stop()

	}
}
