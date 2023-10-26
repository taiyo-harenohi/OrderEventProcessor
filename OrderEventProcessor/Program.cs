using Npgsql;
using System;
using System.Data;

namespace OrderEventProcessor
{
    internal class Program
    {
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
    }
}