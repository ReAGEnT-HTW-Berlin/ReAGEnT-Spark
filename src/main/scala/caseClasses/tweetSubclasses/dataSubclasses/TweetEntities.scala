package caseClasses.tweetSubclasses.dataSubclasses

import caseClasses.tweetSubclasses.dataSubclasses.tweetEntitiesSubclasses.{TweetHashtag, TweetMention, TweetUrl}

case class TweetEntities(
                          hashtags: Option[Array[TweetHashtag]],
                          mentions: Option[Array[TweetMention]],
                          urls: Option[Array[TweetUrl]]
                        )
