using System;
using System.Threading;
using Confluent.Kafka;
using Quix.Sdk.Streaming;

namespace Kafka_Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var topic = "steve";
            var brokerList = "pkc-4nmjv.francecentral.azure.confluent.cloud:9092";

            // var x = new DescribeConfigsOptions()
            // {
            //     RequestTimeout = new TimeSpan(200)
            // };

            var config = new ConsumerConfig
            {
                
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "EWUITZQPW62A52SC",
                SaslPassword = "4jtTwh8sozR6DWoZCEVBLQgJuQ6mGvkHWeIq7MRDlaJSGUR79tfY/oJ8jH7mgtzE",
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            const int commitPeriod = 5;

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                       // Note: All handlers are called on the main .Consume thread.
                       
                       .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                       
                       //TODO handle these partition events
                       
                       //.SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                       // .SetPartitionsAssignedHandler((c, partitions) =>
                       // {
                       //     // Since a cooperative assignor (CooperativeSticky) has been configured, the
                       //     // partition assignment is incremental (adds partitions to any existing assignment).
                       //     Console.WriteLine(
                       //         "Partitions incrementally assigned: [" +
                       //         string.Join(',', partitions.Select(p => p.Partition.Value)) +
                       //         "], all: [" +
                       //         string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                       //         "]");
                       //
                       //     // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                       //     // to assign to, e.g.:
                       //     // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                       // })
                       // .SetPartitionsRevokedHandler((c, partitions) =>
                       // {
                       //     // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                       //     // assignment is incremental (may remove only some partitions of the current assignment).
                       //     var remaining = c.Assignment.Where(atp =>
                       //         partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                       //     Console.WriteLine(
                       //         "Partitions incrementally revoked: [" +
                       //         string.Join(',', partitions.Select(p => p.Partition.Value)) +
                       //         "], remaining: [" +
                       //         string.Join(',', remaining.Select(p => p.Partition.Value)) +
                       //         "]");
                       // })
                       // .SetPartitionsLostHandler((c, partitions) =>
                       // {
                       //     // The lost partitions handler is called when the consumer detects that it has lost ownership
                       //     // of its assignment (fallen out of the group).
                       //     Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                       // })
                       .Build())
            {
                // subscribe to the topic
                consumer.Subscribe(topic);

                try
                {
                    // connect to quix

                    var streamingClient = new QuixStreamingClient();
                    var outputTopic = streamingClient.OpenOutputTopic("ConfluentKafkaDataTopic"); //config?!?
                    var outputStream = outputTopic.CreateStream($"StreamPrefix-{DateTime.UtcNow:yyyy-MM-dd__hh-mm-ss}"); // config for a stream prefix?

                    //App.Run();

                    var cancellationTokenSource = new CancellationTokenSource();
                    var cancellationToken = cancellationTokenSource.Token;
                    void ThreadWorker()
                    {
                        while (!cancellationToken.IsCancellationRequested)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cancellationToken);

                                if (consumeResult.IsPartitionEOF)
                                {
                                    outputStream.Events.AddTimestamp(DateTime.UtcNow)
                                        .AddValue("EOF", "True")
                                        .AddTag("Topic", consumeResult.Topic)
                                        .AddTag("Partition", consumeResult.Partition.Value.ToString())
                                        .AddTag("Offset", consumeResult.Offset.Value.ToString())
                                        .Write();

                                    Console.WriteLine(
                                        $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                    continue;
                                }

                                outputStream.Parameters.Buffer.AddTimestamp(DateTime.UtcNow)
                                    .AddValue("value", consumeResult.Message.Value ?? "")
                                    .AddTag("TopicPartitionOffset", consumeResult.TopicPartitionOffset.ToString())
                                    .Write();


                                Console.WriteLine(
                                    $"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                                //todo determine how often to send commit offsets.
                                //todo how to ensure only once message handling
                                if (consumeResult.Offset % commitPeriod == 0)
                                {
                                    // The Commit method sends a "commit offsets" request to the Kafka
                                    // cluster and synchronously waits for the response. This is very
                                    // slow compared to the rate at which the consumer is capable of
                                    // consuming messages. A high performance application will typically
                                    // commit offsets relatively infrequently and be designed handle
                                    // duplicate messages in the event of failure.
                                    try
                                    {
                                        outputStream.Events.AddTimestamp(DateTime.UtcNow)
                                            .AddValue("Committing", "Committing Offset")
                                            .AddTag("Offset", consumeResult.Offset.ToString())
                                            .AddTag("CommitPeriod", commitPeriod.ToString())
                                            .Write();

                                        consumer.Commit(consumeResult);
                                    }
                                    catch (KafkaException e)
                                    {
                                        Console.WriteLine($"Commit error: {e.Error.Reason}");
                                    }
                                }
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Consume error: {e.Error.Reason}");
                            }
                        }
                    }

                    
                    Thread workerThread = new Thread(ThreadWorker);
                    workerThread.Start();
                    
                    App.Run(cancellationToken, () =>
                    {
                        // stop the main worker thread
                        cancellationTokenSource.Cancel();
                    });

                    // wait for the worker thread for 5 seconds
                    workerThread.Join(TimeSpan.FromSeconds(5));

                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }
    }
}