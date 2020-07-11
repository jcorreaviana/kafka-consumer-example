using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaConsumerWorker
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //CreateHostBuilder(args).Build().Run();

            var config = new ConsumerConfig
            {  
                GroupId = "consumer-test",
                BootstrapServers = "172.19.0.3:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<string, string>(config).Build();

            consumer.Subscribe("kafka-topic");

            CancellationTokenSource cancellationToken = new CancellationTokenSource(); 

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancellationToken.Cancel();
            };

            Console.WriteLine("Connected to Consumer...");

            try
            {
                while(true)
                {
                    try
                    {
                        var message = consumer.Consume(cancellationToken.Token);

                        Console.WriteLine($"Message (Key/Value): {message.Key}:{message.Value}");
                    }

                    catch(ConsumeException ex)
                    {
                        Console.WriteLine($"Error: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
 
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<Worker>();
                });
    }
}
