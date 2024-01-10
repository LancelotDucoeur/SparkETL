import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SimpleApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
		
		val usersFile = "../donnee/dataset/yelp_academic_dataset_tip.csv"
		
	// Définition du schéma du fichier CSV
			val schema = new StructType()
			.add("business_id", StringType, true)
			.add("compliment_count", IntegerType, true)
			.add("date", DateType, true)
			.add("text", StringType, true)
			.add("user_id", StringType, true)

// Lecture du fichier CSV avec le schéma spécifié
var users = spark.read.format("csv").option("header", "true").schema(schema).load(usersFile).cache()

// Conversion du type de la colonne 'date'
users = users.withColumn("date", col("date").cast(DateType))

// Affichage du DataFrame
users.show()

// Arrêt du programme
//System.exit(0);
		// Extraction des amis, qui formeront une table "user_id - friend_id"
		//var friends = users.select("user_id", "friends")

		var review = users.select("business_id","date","text","user_id")
		
		//friends = friends.withColumn("friends", explode(org.apache.spark.sql.functions.split(col("friends"), ",")))
		//friends = friends.withColumnRenamed("friends", "friend_id")
		// Pour supprimer les lignes sans friend_id
		//friends = friends.filter(col("friend_id").notEqual("None"))
		
		// Suppression de la colonne "friends" dans le DataFrame users
		//users = users.drop(col("friends"))
		
		// Extraction des années en temps qu'"élite", qui formeront une table "user_id - year"
		//var elite = users.select("user_id", "elite")
		//elite = elite.withColumn("elite", explode(org.apache.spark.sql.functions.split(col("elite"), ",")))
		//elite = elite.withColumnRenamed("elite", "year")
		// Pour supprimer les lignes sans year
		//elite = elite.filter(col("year").notEqual(""))
		//elite = elite.withColumn("year", col("year").cast(IntegerType))
		
		// Suppression de la colonne "elite" dans le DataFrame users
		//users = users.drop(col("elite"))
		
		// Affichage du schéma des DataFrame
		review.printSchema()
		// friends.printSchema()
		// elite.printSchema()
		
		//val reviewsFile = "/chemin_dossier/yelp_academic_dataset_tip.csv"
		// Chargement du fichier JSON
	//	var reviews = spark.read.json(reviewsFile).cache()
		// Changement du type d'une colonne
	//	reviews = reviews.withColumn("date", col("date").cast(DateType))
		
		// Affichage du schéma du DataFrame
	//	reviews.printSchema()
		//System.exit(0);
		// Paramètres de la connexion BD
		// Class.forName("org.postgresql.Driver")
		// val url = "jdbc:postgresql://stendhal:5432/tpid2020"
		// import java.util.Properties
		// val connectionProperties = new Properties()
		// connectionProperties.setProperty("driver", "org.postgresql.Driver")
		// connectionProperties.setProperty("user", "user")
		// connectionProperties.setProperty("password","password")
		
		// // Enregistrement du DataFrame users dans la table "user"
		// users.write
		// 	.mode(SaveMode.Overwrite).jdbc(url, "yelp.\"user\"", connectionProperties)
		
		// // Enregistrement du DataFrame friends dans la table "friend"
		// friends.write
		// 	.mode(SaveMode.Overwrite).jdbc(url, "yelp.friend", connectionProperties)
		// // Enregsitrement du DataFrame elite dans la table "elite"
		// elite.write
		// 	.mode(SaveMode.Overwrite).jdbc(url, "yelp.elite", connectionProperties)
		
		// // Enregistrement du DataFrame reviews dans la table "review"
		// reviews.write
		// 	.mode(SaveMode.Overwrite)
		// 	.jdbc(url, "yelp.review", connectionProperties)
		
		// spark.stop()



		// Paramètres de la connexion BD pour Oracle
Class.forName("oracle.jdbc.driver.OracleDriver")
val url = "jdbc:oracle:thin:@stendhal.iem:1521:enss2023" // Remplacez [hostname], [port] et [DBName] par vos informations

import java.util.Properties
val connectionProperties = new Properties()
connectionProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
connectionProperties.setProperty("user", "rb188509") // Remplacez par votre nom d'utilisateur
connectionProperties.setProperty("password", "rb188509") // Remplacez par votre mot de passe

// Enregistrement du DataFrame users dans la table "user"
review.write
    .mode(SaveMode.Overwrite).jdbc(url, "rb188509.review", connectionProperties) // Remplacez nom_schema par le nom de votre schéma



spark.stop()

	}
}
