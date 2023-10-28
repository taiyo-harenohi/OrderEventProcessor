using Npgsql;
using System;
using System.Data;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Generic;
using System.Text.Json;
using System.Reflection.PortableExecutable;

namespace OrderEventProcessor
{
    internal class Program
    {
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
                    CreateTable(con);
                    ConnectRabbit(con);
                }
            }
        }

        private static void CreateTable(NpgsqlConnection con)
        {
            var command = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS orders(ID VARCHAR(5) PRIMARY KEY NOT NULL, product VARCHAR(6) NOT NULL, total DECIMAL NOT NULL, currency VARCHAR(3) NOT NULL, status VARCHAR(20));", con);
            command.ExecuteNonQuery();
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
                    Console.WriteLine(msgTypeHeader);

                    if (msgTypeHeader == "OrderEvent")
                    {
                        InsertData(message, con);
                    }
                    if (msgTypeHeader == "PaymentEvent")
                    {
                        UpdateData(FindData(message, con), con);
                    }
                }
                //channel.BasicAck(ea.DeliveryTag, false);
            };

            channel.BasicConsume(orderQueueName, true, receivedEvent);

            Console.ReadLine(); 
        }

        private static void InsertData(string message, NpgsqlConnection con)
        {
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

        private static Dictionary<string,string> FindData(string message, NpgsqlConnection con)
        {
            string status = "";
            Dictionary<string, string> orders = new Dictionary<string, string>();
            var values = JsonSerializer.Deserialize<Dictionary<string, object>>(message);

            if (values != null)
            {
                var command = new NpgsqlCommand($"SELECT * FROM orders WHERE ID = @id;", con);
                command.Parameters.AddWithValue("id", values["orderId"].ToString());
                var result = command.ExecuteReader();

                while (result.Read())
                {
                    status = CheckData(values, result[2].ToString());
                    if (status != null)
                    {
                        Console.WriteLine("Order: {0}\t Product: {1}\t Total: {2}\t Status: {3}", result[0], result[1], result[2], status);
                        orders.Add("id", result[0].ToString());
                        orders.Add("product", result[1].ToString());
                        orders.Add("total", result[2].ToString());
                        orders.Add("currency", result[3].ToString());
                        orders.Add("status", status);
                    }
                }
                result.Close();
                result.DisposeAsync();
            }
            return orders;
        }

        private static string CheckData(Dictionary<string, object> values, string compare)
        {
            if (Convert.ToDecimal(values["amount"].ToString()) != Convert.ToDecimal(compare))
            {
                return "PARTIALLY PAID";
            }
            else
            {
                return "PAID";
            }
        }

        private static void UpdateData(Dictionary<string, string> orders, NpgsqlConnection con)
        {
            var update = new NpgsqlCommand($"UPDATE orders SET product = @product, total = @total, currency = @currency, status = @status;", con);
            update.Parameters.AddWithValue("id", orders["id"]);
            update.Parameters.AddWithValue("product", orders["product"]);
            decimal total = decimal.Parse(orders["total"]);
            update.Parameters.AddWithValue("total", total);
            update.Parameters.AddWithValue("currency", orders["currency"]);
            update.Parameters.AddWithValue("status", orders["status"]);
            update.ExecuteNonQuery();
        }
    }
}