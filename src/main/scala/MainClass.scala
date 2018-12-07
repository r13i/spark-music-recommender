
// package com.example.music_recommender

import org.apache.spark.sql.functions.{min, max}

object MainClass extends SparkMachinery {
    import spark.implicits._

    def main (args: Array[String]) {

        // DataFrame with ||User ID|Artist ID||
        val rawUserArtistData = spark.read
            .textFile(conf.getString("data.path") + "/user_artist_data.txt")
            .repartition(4)     // Distribute the data on all available cores

        val userArtistDf = rawUserArtistData.map { line =>
            val Array(user, artist, _*) = line.split(' ')
            (user.toInt, artist.toInt)
        }.toDF("user", "artist").cache()

        userArtistDf.agg(min("user"), max("user"), min("artist"), max("artist")).show()


        // DataFrame with ||ID|Artist||
        val rawArtistData = spark.read.textFile(conf.getString("data.path") + "/artist_data.txt")
        // Because there are corrupted entries in the Artist <-> ID data, we will do a `flatMap`
        // instead of a `map` to drop the bad data
        val artistById = rawArtistData.flatMap { line =>
            val (id, name) = line.span(_ != '\t')
            if (name.isEmpty) {
                None
            } else {
                try {
                    Some((id.toInt, name.trim))
                } catch {
                    case _: NumberFormatException => null
                }
            }
        }.toDF("id", "artist")

        artistById.show


        // Mapping bad artist aliases to good ones
        val rawArtistAlias = spark.read.textFile(conf.getString("data.path") + "/artist_alias.txt")
        // Because there are corrupted entries in the alias data, we will do a `flatMap`
        // instead of a `map` to drop the bad data
        val artistAlias = rawArtistAlias.flatMap { line =>
            val Array(artist, alias) = line.split('\t')
            if (artist.isEmpty) {
                None
            } else {
                Some((artist.toInt, alias.toInt))
            }
        }.collect().toMap

        artistById.filter($"id" isin (1208690, 1003926)).show()
    }
}