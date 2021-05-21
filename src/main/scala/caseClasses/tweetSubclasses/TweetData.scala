package caseClasses.tweetSubclasses

import caseClasses.tweetSubclasses.dataSubclasses.{TweetContextAnnotation, TweetEntities, TweetMetrics, TweetReference}

case class TweetData(
                      reply_settings: String,
                      text: String,
                      author_id: String,
                      id: String,
                      created_at: String,
                      possibly_sensitive: Boolean,
                      in_reply_to_user_id: String,
                      source: String,
                      entities: TweetEntities,
                      referenced_tweets: Array[TweetReference],
                      public_metrics: TweetMetrics,
                      context_annotations: Option[Array[TweetContextAnnotation]]
                    )
