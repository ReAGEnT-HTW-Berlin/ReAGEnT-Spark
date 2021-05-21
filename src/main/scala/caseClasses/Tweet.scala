package caseClasses

import caseClasses.tweetSubclasses.{TweetData, TweetIncludes, TweetMatchingRule}

case class Tweet(
                  _id: String,
                  data: TweetData,
                  includes: TweetIncludes,
                  matching_rules: Array[TweetMatchingRule]
                )
