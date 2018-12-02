
// package com.example.music_recommender


object MainClass extends SparkMachinery {
    def main (args: Array[String]) {
        
        import spark.implicits._

        val data = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(conf.getString("data.path") + "/user_artist_data.txt")

        data.show(5)
    }
}