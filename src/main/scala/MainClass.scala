
// package com.example.music_recommender


object MainClass extends SparkMachinery {
    def main (args: Array[String]) {
        
        import spark.implicits._

        val data = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(conf.getString("data.path") + "/user_artist_data.txt")

        data.show(5)


        val rawUserArtistData = spark.read
            .textFile(conf.getString("data.path") + "/user_artist_data.txt")
            .repartition(4)     // Distribute the data on all 4 cores

        rawUserArtistData.take(5).foreach(println)

        val userArtistDf = rawUserArtistData.map(line => {
            val Array(user, artist, _*) = line.split(' ')
            (user.toInt, artist.toInt)
        }).toDF("user", "artist")

        userArtistDf.show()
    }
}