using Akka.Streams;
using Akka.Streams.Stage;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;
using Akka;

namespace AkkaStreamsTweets.Redis
{
    public class RedisPubSubSource : GraphStageWithMaterializedValue<SourceShape<string>, Task>
    {
        private ConnectionMultiplexer _redis;
        private string _channel;

        public RedisPubSubSource(ConnectionMultiplexer redis, string channel)
        {
            _redis = redis;
            _channel = channel;
            Shape = new SourceShape<string>(Out);
        }

        public Outlet<string> Out { get; } = new Outlet<string>("RedisPubSubSink.Out");

        public override SourceShape<string> Shape { get; }

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            return new LogicAndMaterializedValue<Task>(new Logic(_redis, _channel, inheritedAttributes, this), completion.Task);
        }

        private sealed class Logic : GraphStageLogic
        {
            private ISubscriber _subscriber;
            private string _channel;
            private Attributes _inheritedAttributes;
            private RedisPubSubSource _source;
            private readonly Queue<string> _buffer = new Queue<string>();

            public Logic(ConnectionMultiplexer redis, string channel, Attributes inheritedAttributes, RedisPubSubSource source) : base(source.Shape)
            {
                _subscriber = redis.GetSubscriber();
                _channel = channel;
                _inheritedAttributes = inheritedAttributes;
                _source = source;

                SetHandler(source.Out, onPull: () =>
                {

                });
            }

            public override void PreStart()
            {
                var callback = GetAsyncCallback<(RedisChannel channel, string bs)>(data =>
                {
                    _buffer.Enqueue(data.bs);

                     Deliver();
                });

                _subscriber.Subscribe(_channel, (channel, value) =>
                {
                    callback.Invoke((channel, value));
                });
            }

            private void Deliver()
            {
                var elem = _buffer.Dequeue();
                Push(_source.Out, elem);
            }
        }
    }
}
