using System;
using TollMesh.Cache;
using Xunit;

namespace TollMeshCache.Tests
{
    public class ClientConfigTests
    {
        [Fact]
        public void DefaultsMatchDocumentedValues()
        {
            var config = new ClientConfig();
            Assert.Equal("localhost", config.Host);
            Assert.Equal(8080, config.Port);
            Assert.Equal(TimeSpan.FromSeconds(5), config.Timeout);
            Assert.True(config.VerifySSL);
            Assert.Equal("http", config.Scheme);
            Assert.Null(config.ApiKey);
        }

        [Fact]
        public void CustomValuesAreRespected()
        {
            var config = new ClientConfig
            {
                Host = "api.example.com",
                Port = 443,
                Scheme = "https",
                ApiKey = "secret-key",
            };
            Assert.Equal("api.example.com", config.Host);
            Assert.Equal(443, config.Port);
            Assert.Equal("https", config.Scheme);
            Assert.Equal("secret-key", config.ApiKey);
        }

        [Theory]
        [InlineData("localhost", 8080, "http", "http://localhost:8080")]
        [InlineData("api.example.com", 443, "https", "https://api.example.com:443")]
        public void GetBaseUrlBuildsCorrectly(string host, int port, string scheme, string expected)
        {
            var config = new ClientConfig { Host = host, Port = port, Scheme = scheme };
            Assert.Equal(expected, config.GetBaseUrl());
        }
    }

    public class ClientConstructionTests
    {
        [Fact]
        public void ConstructsWithDefaultConfig()
        {
            using var client = new Client();
            Assert.NotNull(client);
        }

        [Fact]
        public void ConstructsWithCustomConfig()
        {
            using var client = new Client(new ClientConfig { Host = "example.com", Port = 9000 });
            Assert.NotNull(client);
        }

        [Fact]
        public void DisposeDoesNotThrow()
        {
            var client = new Client();
            var exception = Record.Exception(() => client.Dispose());
            Assert.Null(exception);
        }

        [Fact]
        public void DisposeIsIdempotent()
        {
            var client = new Client();
            client.Dispose();
            var exception = Record.Exception(() => client.Dispose());
            Assert.Null(exception);
        }
    }
}
