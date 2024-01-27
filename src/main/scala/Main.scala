import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, DateType}
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.expressions.Window
import scala.util.Try
import play.api.libs.json.Json



object Main {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

		val dataExtractor = new DataExtractor(spark)

		
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
		var businessDF = businessLoad.as("business")
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

		var businessCategoriesDF: DataFrame = businessLoad
		.withColumn("category", explode(split(col("categories"), ", ")))
		.select("business_id", "category")

		businessCategoriesDF = businessCategoriesDF
		.join(categoriesDF, businessCategoriesDF("category") === categoriesDF("name"))
		.select(businessCategoriesDF("business_id"), categoriesDF("category_id"))

		

		val queryUser = "(SELECT * FROM yelp.user) as q1"
		var userDF = dataExtractor.PostgresData(queryUser).select("user_id", "average_stars", "cool", "fans", "funny", "useful", "review_count")
		
		val querryReview =  "(SELECT * FROM yelp.review) as q1"
		var reviewDF = dataExtractor.PostgresData(querryReview).select("review_id", "user_id", "business_id", "cool", "funny", "useful", "stars", "text", "date")

		reviewDF.show()

		val schemaTip = new StructType()
		.add("business_id", StringType, true)
		.add("compliment_count", IntegerType, true)
		.add("date", DateType, true)
		.add("text", StringType, true)
		.add("user_id", StringType, true)
		val csvOptions = Map("sep" -> ",", "nullValue" -> "NULL", "dateFormat" -> "yyyy-MM-dd")
		var tipDF = dataExtractor.CSVData("../dataset/yelp_academic_dataset_tip.csv", schemaTip, csvOptions)

		tipDF.show()

		// Ajoutez la colonne 'isTip' à reviewDF
		val reviewDFWithTip = reviewDF.withColumn("isTip", lit(false))

		// Générer de nouveaux ID pour tipDF, ajouter la colonne 'isTip'
		val tipDFWithIdAndTip = tipDF.withColumn("review_id", monotonically_increasing_id())
									.withColumn("isTip", lit(true))

		// Ajoutez des colonnes fictives pour 'cool', 'funny', 'useful' et 'stars' à tipDF pour correspondre à reviewDF
		val tipDFAdjusted = tipDFWithIdAndTip
		.withColumn("cool", lit(null).cast("int"))
		.withColumn("funny", lit(null).cast("int"))
		.withColumn("useful", lit(null).cast("int"))
		.withColumn("stars", lit(null).cast("int"))

		// Sélectionnez les mêmes colonnes pour reviewDF et tipDF, en omettant 'compliment_count'
		val reviewDFSelected = reviewDFWithTip.select("review_id", "user_id", "business_id", "cool", "funny", "useful", "stars", "text", "date", "isTip")
		val tipDFSelected = tipDFAdjusted.select("review_id", "user_id", "business_id", "cool", "funny", "useful", "stars", "text", "date", "isTip")

		// Fusionnez les DataFrames
		reviewDF = reviewDFSelected.unionByName(tipDFSelected)

		// Affichez le résultat pour vérifier
		reviewDF.show()

		//val whereDf: DataFrame = combinedDF.where(col("isTip") === "true")

		//whereDf.show()

		

		var checkinDF = dataExtractor.JSONData("../dataset/yelp_academic_dataset_checkin.json")
		
		checkinDF.show()

		// Convertissez la colonne 'date' en DateType et extrayez l'année et le mois
		val checkinWithYearMonth = checkinDF
		.withColumn("date", explode(split(col("date"), ", ")))
		.withColumn("date", to_date(col("date"), "yyyy-MM-dd HH:mm:ss"))
		.withColumn("year", year(col("date")))
		.withColumn("month", month(col("date")))

		// Assigne un identifiant unique pour chaque paire business_id-year
		val yearDF = checkinWithYearMonth
		.select("business_id", "year")
		.distinct()
		.withColumn("year_id", row_number().over(Window.orderBy("business_id", "year")))

		// Comptez le nombre de check-ins par mois
		val monthCounts = checkinWithYearMonth
		.groupBy("business_id", "year", "month")
		.count()

		// Pivot pour transformer les mois en colonnes et les comptes en valeurs
		val monthPivotDF = monthCounts
		.groupBy("business_id", "year")
		.pivot("month", 1 to 12)
		.agg(sum("count"))
		.na.fill(0) // Remplissez les mois sans check-ins par 0

		// Joindre avec yearDF pour ajouter year_id
		val monthDF = monthPivotDF
		.join(yearDF, Seq("business_id", "year"))

		// Sélectionner les colonnes de manière uniforme, soit toutes en tant que String, soit toutes en tant que Column
		val monthDFSelected = monthDF.select(
		col("year_id"),
		col("1").alias("January"),
		col("2").alias("February"),
		col("3").alias("March"),
		col("4").alias("April"),
		col("5").alias("May"),
		col("6").alias("June"),
		col("7").alias("July"),
		col("8").alias("August"),
		col("9").alias("September"),
		col("10").alias("October"),
		col("11").alias("November"),
		col("12").alias("December")
		)

		// Afficher les DataFrames pour vérifier
		yearDF.show()
		monthDFSelected.show()

		//val whereDf: DataFrame = monthDFSelected.where(col("year_id") === "1")

		//whereDf.show()


		businessLoad.select("hours").show()

		val businessParkingSchema = StructType(Array(
			StructField("garage", BooleanType, true),
			StructField("street", BooleanType, true),
			StructField("validated", BooleanType, true),
			StructField("lot", BooleanType, true),
			StructField("valet", BooleanType, true)
		))

		val AmbienceSchema = StructType(Array(
			StructField("romantic", BooleanType, true),
			StructField("intimate", BooleanType, true),
			StructField("touristy", BooleanType, true),
			StructField("hipster", BooleanType, true),
			StructField("divey", BooleanType, true),
			StructField("classy", BooleanType, true),
			StructField("trendy", BooleanType, true),
			StructField("upscale", BooleanType, true),
			StructField("casual", BooleanType, true)
		))

		val GoodForMealSchema = StructType(Array(
			StructField("dessert", BooleanType, true),
			StructField("latenight", BooleanType, true),
			StructField("lunch", BooleanType, true),
			StructField("dinner", BooleanType, true),
			StructField("brunch", BooleanType, true),
			StructField("breakfast", BooleanType, true)
		))


		// Extraction des champs imbriqués à partir de la colonne attributes, qui est un JSON
		val attributesDF = businessLoad
		.select(
			col("business_id"),
			col("attributes").getItem("AgesAllowed").alias("AgesAllowed"),
			col("attributes").getItem("Alcohol").alias("Alcohol"),
			col("attributes").getItem("BestNights").alias("BestNights"),
			col("attributes").getItem("BikeParking").alias("BikeParking"),
			col("attributes").getItem("BusinessAcceptsBitcoin").alias("BusinessAcceptsBitcoin"),
			col("attributes").getItem("BusinessAcceptsCreditCards").alias("BusinessAcceptsCreditCards"),
			col("attributes").getItem("ByAppointmentOnly").alias("ByAppointmentOnly"),
			col("attributes").getItem("BYOB").alias("BYOB"),
			col("attributes").getItem("BYOBCorkage").alias("BYOBCorkage"),
			col("attributes").getItem("Caters").alias("Caters"),
			col("attributes").getItem("CoatCheck").alias("CoatCheck"),
			col("attributes").getItem("Corkage").alias("Corkage"),
			col("attributes").getItem("DietaryRestrictions").alias("DietaryRestrictions"),
			col("attributes").getItem("DogsAllowed").alias("DogsAllowed"),
			col("attributes").getItem("DriveThru").alias("DriveThru"),
			col("attributes").getItem("GoodForDancing").alias("GoodForDancing"),
			col("attributes").getItem("GoodForKids").alias("GoodForKids"),
			col("attributes").getItem("HappyHour").alias("HappyHour"),
			col("attributes").getItem("HasTV").alias("HasTV"),
			col("attributes").getItem("Music").alias("Music"),
			col("attributes").getItem("NoiseLevel").alias("NoiseLevel"),
			col("attributes").getItem("OutdoorSeating").alias("OutdoorSeating"),
			col("attributes").getItem("RestaurantsAttire").alias("RestaurantsAttire"),
			col("attributes").getItem("RestaurantsDelivery").alias("RestaurantsDelivery"),
			col("attributes").getItem("RestaurantsGoodForGroups").alias("RestaurantsGoodForGroups"),
			col("attributes").getItem("RestaurantsPriceRange2").alias("RestaurantsPriceRange2"),
			col("attributes").getItem("RestaurantsReservations").alias("RestaurantsReservations"),
			col("attributes").getItem("RestaurantsTableService").alias("RestaurantsTableService"),
			col("attributes").getItem("RestaurantsTakeOut").alias("RestaurantsTakeOut"),
			col("attributes").getItem("Smoking").alias("Smoking"),
			col("attributes").getItem("WheelchairAccessible").alias("WheelchairAccessible"),
			col("attributes").getItem("WiFi").alias("WiFi"),
			// Pour les champs JSON imbriqués comme 'BusinessParking', parsez-les séparément
			from_json(col("attributes").getItem("BusinessParking"), businessParkingSchema).alias("BusinessParking"),
			from_json(col("attributes").getItem("Ambience"), AmbienceSchema).alias("Ambience"),
			from_json(col("attributes").getItem("GoodForMeal"), GoodForMealSchema).alias("GoodForMeal"),

		)

		// Créez des colonnes supplémentaires pour chaque champ du JSON BusinessParking
		val expandedAttributesDF = attributesDF
		.select(
			col("AgesAllowed"),
			col("Alcohol"),
			col("BestNights"),
			col("BikeParking"),
			col("BusinessAcceptsBitcoin"),
			col("BusinessAcceptsCreditCards"),
			col("ByAppointmentOnly"),
			col("BYOB"),
			col("BYOBCorkage"),
			col("Caters"),
			col("CoatCheck"),
			col("Corkage"),
			col("DietaryRestrictions"),
			col("DogsAllowed"),
			col("DriveThru"),
			col("GoodForDancing"),
			col("GoodForKids"),
			col("HappyHour"),
			col("HasTV"),
			col("Music"),
			col("NoiseLevel"),
			col("OutdoorSeating"),
			col("RestaurantsAttire"),
			col("RestaurantsDelivery"),
			col("RestaurantsGoodForGroups"),
			col("RestaurantsPriceRange2"),
			col("RestaurantsReservations"),
			col("RestaurantsTableService"),
			col("RestaurantsTakeOut"),
			col("Smoking"),
			col("WheelchairAccessible"),
			col("WiFi"),
			col("BusinessParking.*"),
			col("Ambience.*"),
			col("GoodForMeal.*") // Utilisez '*' pour sélectionner tous les champs du StructType
		)

		// Ajouter un identifiant unique à chaque ligne d'attributs
		val attributesWithIdDF = expandedAttributesDF
		.withColumn("Attribute_id", monotonically_increasing_id())

		// Afficher le DataFrame résultant pour vérification
		attributesWithIdDF.show(false)




		var scheduleDF = businessLoad
		.select(
			col("business_id"),
			col("attributes").getItem("Open24Hours").alias("Open24Hours"),
			col("hours").getItem("Monday").alias("Monday"),
			col("hours").getItem("Tuesday").alias("Tuesday"),
			col("hours").getItem("Wednesday").alias("Wednesday"),
			col("hours").getItem("Thursday").alias("Thursday"),
			col("hours").getItem("Friday").alias("Friday"),
			col("hours").getItem("Saturday").alias("Saturday"),
			col("hours").getItem("Sunday").alias("Sunday")
		)

		scheduleDF.show()

		// Ajouter un identifiant unique à scheduleDF
		scheduleDF = scheduleDF
		.withColumn("schedule_id", monotonically_increasing_id())

		// Joindre scheduleDFWithId avec businessDF
		businessDF = businessDF.as("business")
		.join(scheduleDF.as("schedule"), col("business.business_id") === col("schedule.business_id"))
		.select(
			col("business.business_id"),
			col("schedule.schedule_id"),
			col("business.location_id"),
			col("business.stars"),
			col("business.review_count"),
			// Ajoutez ici toutes les autres colonnes nécessaires de businessDF et scheduleDF
		)

		scheduleDF = scheduleDF.drop("business_id")



		businessDF.show()
		// Afficher le DataFrame résultant pour vérification
		scheduleDF.show(false)




		val dl = new DataLoader();

		dl.loadToOracle(businessDF, "ld754555.business")
		dl.loadToOracle(reviewDF, "ld754555.review")
		dl.loadToOracle(userDF, "ld754555.user")
		dl.loadToOracle(businessCategoriesDF, "ld754555.businessCategories")
		dl.loadToOracle(categoriesDF, "ld754555.categories")
		dl.loadToOracle(yearDF, "ld754555.year")
		dl.loadToOracle(monthDF, "ld754555.month")
		dl.loadToOracle(attributesDF, "ld754555.attributes")
		dl.loadToOracle(scheduleDF, "ld754555.schedule")
		dl.loadToOracle(locationDF, "ld754555.location")


		spark.stop()

	}
}
