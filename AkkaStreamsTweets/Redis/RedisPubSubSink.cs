using Akka;
using Akka.Streams;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace AkkaStreamsTweets.Redis
{
    public class RedisPubSubSink : GraphStageWithMaterializedValue<SinkShape<string>, Task>
    {
        private ConnectionMultiplexer _redis;
        private string _channel;

        public RedisPubSubSink(ConnectionMultiplexer redis, string channel)
        {
            _redis = redis;
            _channel = channel;
            Shape = new SinkShape<string>(In);
        }

        public Inlet<string> In { get; } = new Inlet<string>("RedisPubSubSink.In");

        public override SinkShape<string> Shape { get; }

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            return new LogicAndMaterializedValue<Task>(new Logic(_redis, _channel, inheritedAttributes, this), completion.Task);
        }

        private sealed class Logic : GraphStageLogic
        {
            private ISubscriber _subscriber;
            private string _channel;
            private RedisPubSubSink _sink;
            private Action<(Task, string)> _eventsSend;
            private bool _isSendInProgress;
            private readonly Decider _decider;

            public Logic(ConnectionMultiplexer redis, string channel, Attributes inheritedAttributes, RedisPubSubSink sink) : base(sink.Shape)
            {
                _subscriber = redis.GetSubscriber();
                _channel = channel;
                _sink = sink;
                _decider = inheritedAttributes.GetDeciderOrDefault();

                SetHandler(sink.In, onPush: () =>
                {
                    TrySend(Grab(sink.In));
                },
                onUpstreamFinish: () =>
                {
                    // It is most likely that we receive the finish event before the task from the last element has finished
                    // so if the task is still running we need to complete the stage later
                    if (!_isSendInProgress)
                        Finish();
                },
                onUpstreamFailure: ex =>
                {
                    // We have set KeepGoing to true so we need to fail the stage manually
                    FailStage(ex);
                });
            }

            public override void PreStart()
            {
                // Keep going even if the upstream has finished so that we can process the task from the last element
                SetKeepGoing(true);

                _eventsSend = GetAsyncCallback<(Task task, string)>(OnEventsSend);

                // Request the first element
                Pull(_sink.In);
            }

            private void TrySend(string message)
            {
                _isSendInProgress = true;
                _subscriber.PublishAsync(_channel, message).ContinueWith(task => _eventsSend((task, message)));
            }

            private void OnEventsSend((Task task, string message) t)
            {
                _isSendInProgress = false;

                if (t.task.IsFaulted || t.task.IsCanceled)
                {
                    switch (_decider(t.task.Exception))
                    {
                        case Directive.Stop:
                            // Throw
                            FailStage(t.task.Exception);
                            break;
                        case Directive.Resume:
                            // Try again
                            TrySend(t.message);
                            break;
                        case Directive.Restart:
                            // Take the next element or complete
                            PullOrComplete();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    PullOrComplete();
                }
            }

            private void PullOrComplete()
            {
                if (IsClosed(_sink.In))
                    Finish();
                else
                    Pull(_sink.In);
            }

            private void Finish()
            {
                CompleteStage();
            }
        }
    }
}
