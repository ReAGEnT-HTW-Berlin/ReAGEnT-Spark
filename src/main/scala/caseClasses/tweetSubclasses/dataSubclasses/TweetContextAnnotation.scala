package caseClasses.tweetSubclasses.dataSubclasses

import caseClasses.tweetSubclasses.dataSubclasses.tweetContextSubclasses.{TweetContextDomain, TweetContextEntity}

case class TweetContextAnnotation(
                                   domain: TweetContextDomain,
                                   entity: TweetContextEntity
                                 )
