using Npgsql;
using System;
using System.Data;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Generic;
// using Newtonsoft.Json;
using System.Text.Json;

namespace OrderEventProcessor
{
    internal class Program
    {
        private static Dictionary<string, string> orders = new Dictionary<string, string>();
        static void Main(string[] args)
        {
            OpenConnection();
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
                    ConnectRabbit(con);
                }
            }
        }

        private static void CreateTable(NpgsqlConnection con)
        {
            var command = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS orders(ID VARCHAR(5) PRIMARY KEY NOT NULL, product VARCHAR(6) NOT NULL, total DECIMAL NOT NULL, currency VARCHAR(3) NOT NULL, status VARCHAR(20));", con);
            command.ExecuteNonQuery();
            Console.WriteLine("Table created");
        }

        private static NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection(@"Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres");
        }

        private static void ConnectRabbit(NpgsqlConnection con)
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

            channel.ExchangeDeclare(exchangeName, ExchangeType.Direct, false);
            channel.QueueDeclare(orderQueueName, true, false, false);

            // Bind queues to the exchange with routing keys
            channel.QueueBind(orderQueueName, exchangeName, "order");

            var receivedEvent = new EventingBasicConsumer(channel);

            receivedEvent.Received += (model, ea) =>
            {
                // Insert order record into PostgreSQL
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var headers = ea.BasicProperties.Headers;

                if (headers != null && headers.ContainsKey("X-MsgType"))
                {
                    var msgTypeHeader = Encoding.UTF8.GetString((byte[])headers["X-MsgType"]);

                    if (msgTypeHeader == "OrderEvent")
                    {
                        InsertData(message, con);
                    }
                    else if (msgTypeHeader == "PaymentEvent")
                    {
                        FindData(message, con);
                    }
                }
                channel.BasicAck(ea.DeliveryTag, false);
            };

            channel.BasicConsume(orderQueueName, true, receivedEvent);

            Console.ReadLine(); 
        }

        private static void InsertData(string message, NpgsqlConnection con)
        {
            // var values = JsonConvert.DeserializeObject<Dictionary<string, string>>(message);
            var values = JsonSerializer.Deserialize<Dictionary<string, object>>(message); 
            
            if (values != null)
            {
                var command = new NpgsqlCommand($"INSERT INTO orders (ID, product, total, currency) VALUES (@id, @product, @total, @currency) ON CONFLICT (ID) DO UPDATE " +
                                                $"SET product = @product, total = @total, currency = @currency;", con);
                command.Parameters.AddWithValue("id", values["id"].ToString());
                command.Parameters.AddWithValue("product", values["product"].ToString());
                decimal total = decimal.Parse(values["total"].ToString());
                command.Parameters.AddWithValue("total", total);
                command.Parameters.AddWithValue("currency", values["currency"].ToString());
                command.ExecuteNonQuery();
            }
            
        }

        private static void FindData(string message, NpgsqlConnection con)
        {
            var values = JsonSerializer.Deserialize<Dictionary<string, object>>(message);

            if (values != null)
            {
                var command = new NpgsqlCommand($"SELECT * FROM orders WHERE ID = @id;", con);
                command.Parameters.AddWithValue("id", values["orderId"].ToString());
                var result = command.ExecuteScalar();

                Console.WriteLine(result);
            }
        }
    }
}