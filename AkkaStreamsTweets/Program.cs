using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using AkkaStreamsTweets.Redis;
using StackExchange.Redis;
using System;
using System.Linq;
using Tweetinvi;
using Tweetinvi.Models;

namespace AkkaStreamsTweets
{
    static class Program
    {
        private const string ConsumerKey = "";
        private const string ConsumerSecret = "";
        private const string AccessToken = "";
        private const string AccessTokenSecret = "";

        static void Main(string[] args)
        {
            var tweetSource = Source.ActorRef<ITweet>(100, OverflowStrategy.DropHead);
            var formatFlow = Flow.Create<ITweet>().Select(tweet => tweet.ToJson());

            var connection = ConnectionMultiplexer.Connect("127.0.0.1:6379");

            var writeToRedisSink = Sink.FromGraph(new RedisPubSubSink(connection, "redis-pub-sub"));
            var readFromRedisSource = Source.FromGraph(new RedisPubSubSource(connection, "redis-pub-sub"));

            using (var sys = ActorSystem.Create("Reactive-Tweets"))
            {
                using (var mat = sys.Materializer())
                {
                    // read from redis again
                    readFromRedisSource.To(Sink.ForEach<string>(Console.WriteLine)).Run(mat);

                    // read from twitter and write to redis pubsub
                    var actor = tweetSource.Via(formatFlow).To(writeToRedisSink).Run(mat);

                    // Start Twitter stream
                    Auth.SetCredentials(new TwitterCredentials(ConsumerKey, ConsumerSecret, AccessToken, AccessTokenSecret));

                    var stream = Stream.CreateFilteredStream();
                    stream.AddTrack("Trump");
                    stream.MatchingTweetReceived += (_, arg) =>
                    {
                        actor.Tell(arg.Tweet); // push the tweets into the stream
                    };
                    stream.StartStreamMatchingAllConditions();

                    Console.ReadLine();
                }
            }
        }
    }
}