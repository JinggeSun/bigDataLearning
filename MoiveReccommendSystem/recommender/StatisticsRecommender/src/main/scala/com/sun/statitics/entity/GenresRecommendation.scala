package com.sun.statitics.entity

case class GenresRecommendation(
                                 genres:String,
                                 recs:Seq[Recommendation]
                               )
