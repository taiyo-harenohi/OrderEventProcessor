using Npgsql;
using System.Data;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;

namespace OrderEventProcessor
{
    internal class Program
    {
        // variables for Tasks management
        private static Dictionary<string, Task> waitingOrder = new Dictionary<string, Task>();
        private static Dictionary<string, string> waitingInfo = new Dictionary<string, string>();

        static async Task Main(string[] args)
        {
            await OpenConnectionAsync();
        }

        /// <summary>
        /// Method for openning connection to the SQL database
        /// </summary>
        private static async Task OpenConnectionAsync()
        {
            using (NpgsqlConnection con = GetConnection())
            {
                await con.OpenAsync();
                if (con.State == ConnectionState.Open)
                {
                    await CreateTableAsync(con);
                    ConnectRabbit(con);
                }
            }
        }

        /// <summary>
        /// Method for creating a table if it doesn't exist already
        /// </summary>
        /// <param name="con"> connection to the SQL Database ; Type NpgsqlConnection </param>
        private static async Task CreateTableAsync(NpgsqlConnection con)
        {
            var command = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS orders(ID VARCHAR(5) PRIMARY KEY NOT NULL, product VARCHAR(6) NOT NULL, total DECIMAL NOT NULL, currency VARCHAR(3) NOT NULL, status VARCHAR(20));", con);
            await command.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Method for establishing connection string
        /// </summary>
        private static NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection(@"Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres");
        }

        /// <summary>
        /// Method for connecting to the RabbitMq as a consumer
        /// </summary>
        /// <param name="con"> connection to the SQL Database ; Type NpgsqlConnection </param>
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
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var headers = ea.BasicProperties.Headers;

                // detect which header the message has
                if (headers != null && headers.ContainsKey("X-MsgType"))
                {
                    var msgTypeHeader = Encoding.UTF8.GetString((byte[])headers["X-MsgType"]);

                    if (msgTypeHeader == "OrderEvent")
                    {
                        InsertDataAsync(message, con);
                    }
                    if (msgTypeHeader == "PaymentEvent")
                    {
                        FindDataAsync(message, con);
                    }
                }
            };

            channel.BasicConsume(orderQueueName, true, receivedEvent);

            // so the connection can end
            Console.ReadLine();
        }

        /// <summary>
        /// Method for inserting new data into the table or updating the current ones
        /// </summary>
        /// <param name="message"> Payload of the message sent ; Type string </param>
        /// <param name="con"> connection to the SQL Database ; Type NpgsqlConnection </param>
        /// <returns></returns>
        private static async Task InsertDataAsync(string message, NpgsqlConnection con)
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
                await command.ExecuteNonQueryAsync();


                // if there is paymentEvent waiting
                if (waitingOrder.ContainsKey(values["id"].ToString()))
                {
                    waitingOrder[values["id"].ToString()].Dispose();
                    await FindDataAsync(waitingInfo[values["id"].ToString()], con);

                    waitingOrder.Remove(values["id"].ToString());
                }
            }

        }

        /// <summary>
        /// Method for PaymentEvent to find out if there is row with the id existing
        /// </summary>
        /// <param name="message"> Payload of the message sent ; Type string </param>
        /// <param name="con"> connection to the SQL Database ; Type NpgsqlConnection </param>        
        /// <returns></returns>
        private static async Task FindDataAsync(string message, NpgsqlConnection con)
        {
            string status = "";
            Dictionary<string, string> orders = new Dictionary<string, string>();
            var values = JsonSerializer.Deserialize<Dictionary<string, object>>(message);

            if (values != null)
            {
                var command = new NpgsqlCommand($"SELECT * FROM orders WHERE ID = @id;", con);
                command.Parameters.AddWithValue("id", values["orderId"].ToString());
                var result = await command.ExecuteReaderAsync();

                if (!result.HasRows)
                {
                    // Create a new Task
                    var waitingTask = Task.Run(() => WaitingPayment());
                    waitingOrder[values["orderId"].ToString()] = waitingTask;
                    waitingInfo[values["orderId"].ToString()] = message;
                }
                else
                {
                    while (await result.ReadAsync())
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
                    await result.CloseAsync();
                    await result.DisposeAsync();
                    await UpdateDataAsync(orders, con);
                }
                await result.CloseAsync();
                await result.DisposeAsync();
            }
        }

        /// <summary>
        /// Warning message to let the user know there is payment without order
        /// </summary>
        public static void WaitingPayment()
        {
            Console.WriteLine("Warning: Couldn't find the corresponding order; waiting for the correct Order Event");
        }

        /// <summary>
        /// Method for deciding if the total and amount is the same
        /// </summary>
        /// <param name="values"> All the values from the Payload </param>
        /// <param name="compare"> value that is inside of the table for the current order </param>
        /// <returns></returns>
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

        /// <summary>
        /// Method for updating data with the new status
        /// </summary>
        /// <param name="orders"> All the data from the row + new status </param>
        /// <param name="con"> connection to the SQL Database ; Type NpgsqlConnection </param>
        /// <returns></returns>
        private static async Task UpdateDataAsync(Dictionary<string, string> orders, NpgsqlConnection con)
        {
            var update = new NpgsqlCommand($"UPDATE orders SET product = @product, total = @total, currency = @currency, status = @status WHERE ID = @id;", con);
            update.Parameters.AddWithValue("id", orders["id"]);
            update.Parameters.AddWithValue("product", orders["product"]);
            decimal total = decimal.Parse(orders["total"]);
            update.Parameters.AddWithValue("total", total);
            update.Parameters.AddWithValue("currency", orders["currency"]);
            update.Parameters.AddWithValue("status", orders["status"]);
            await update.ExecuteNonQueryAsync();
        }
    }
}
