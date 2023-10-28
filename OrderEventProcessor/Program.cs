using Npgsql;
using System;
using System.Data;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Generic;

namespace OrderEventProcessor
{
    internal class Program
    {
        private static Dictionary<string, string> orders = new Dictionary<string, string>();
        static void Main(string[] args)
        {
            OpenConnection();
            Console.ReadKey();
        }

        private static void OpenConnection()
        {
            using(NpgsqlConnection con=GetConnection())
            {
                con.Open();
                if (con.State == ConnectionState.Open)
                {
                    Console.WriteLine("Connected to the server");
                    CreateTable(con);
                    ConnectRabbit();
                }
            }
        }

        private static void CreateTable(NpgsqlConnection con)
        {
            var command = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS orders(ID VARCHAR(5) PRIMARY KEY NOT NULL, product VARCHAR(5) NOT NULL, total DECIMAL NOT NULL, currency VARCHAR(3) NOT NULL);", con);
            command.ExecuteNonQuery();
            Console.WriteLine("Table created");
        }

        private static NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection(@"Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres");
        }

        private static void ConnectRabbit()
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost", 
                UserName = "guest",
                Password = "guest",
            };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            string exchangeName = "order";
            string orderQueueName = "order_queue";
            string paymentQueueName = "payment_queue";

            channel.ExchangeDeclare(exchangeName, ExchangeType.Headers, true);
            channel.QueueDeclare(orderQueueName, true, false, false);
            channel.QueueDeclare(paymentQueueName, true, false, false);

            // Bind queues to the exchange with routing keys
            channel.QueueBind(orderQueueName, exchangeName, "order");
            channel.QueueBind(paymentQueueName, exchangeName, "payment");

            var orderConsumer = new EventingBasicConsumer(channel);
            var paymentConsumer = new EventingBasicConsumer(channel);

            orderConsumer.Received += (model, ea) =>
            {
                // Insert order record into PostgreSQL
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                ConvertMessage(message);
                Console.WriteLine($"Received {message.ToString()}");
            };

            paymentConsumer.Received += (model, ea) =>
            {
                // Process PaymentEvent messages
                // Check if related order records are fully paid and write a console message
            };

            channel.BasicConsume(orderQueueName, true, orderConsumer);
            channel.BasicConsume(paymentQueueName, true, paymentConsumer);

            Console.ReadLine(); 
        }

        private static void ConvertMessage(string message)
        {

            Console.WriteLine(orders);
        }
    }
}