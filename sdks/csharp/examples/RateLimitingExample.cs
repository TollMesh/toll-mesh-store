using System;
using System.Threading.Tasks;
using TollMesh.Cache;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ClientConfig { Host = "localhost", Port = 8080 };

        using (var client = new Client(config))
        {
            Console.WriteLine(new string('=', 60));
            Console.WriteLine("TollMeshCache - Rate Limiting Example (C#)");
            Console.WriteLine(new string('=', 60));

            try
            {
                // Example 1: Basic rate limiting
                Console.WriteLine("\n1. Basic Rate Limiting (100 req/min)");
                Console.WriteLine(new string('-', 60));

                for (int i = 0; i < 3; i++)
                {
                    var result = await client.ConsumeAsync(
                        "user-123", 100, TimeSpan.FromMinutes(1));

                    Console.WriteLine($"Request {i + 1}:");
                    Console.WriteLine($"  Status: {(result.Ok ? "ALLOWED" : "LIMITED")}");
                    Console.WriteLine($"  Remaining: {result.Remaining}");
                }

                // Example 2: Tier-based
                Console.WriteLine("\n2. Tier-Based Rate Limiting");
                Console.WriteLine(new string('-', 60));

                var tiers = new[] { ("free", 10), ("pro", 100), ("enterprise", 1000) };
                foreach (var (tier, limit) in tiers)
                {
                    var result = await client.ConsumeAsync(
                        $"user-tier-{tier}", limit, TimeSpan.FromMinutes(1));

                    var status = result.Ok ? "✓ OK" : "✗ LIMITED";
                    Console.WriteLine($"{tier.ToUpper().PadRight(12)}: {status} ({result.Remaining} remaining)");
                }

                // Example 3: Health check
                Console.WriteLine("\n3. Server Health");
                Console.WriteLine(new string('-', 60));

                var health = await client.HealthAsync();
                Console.WriteLine($"Status: {health.Status}");
                Console.WriteLine($"Node: {health.Node}");
                Console.WriteLine($"Peers: {health.Peers}");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("\n" + new string('=', 60));
                Console.WriteLine("Example complete!");
            }
        }
    }
}
