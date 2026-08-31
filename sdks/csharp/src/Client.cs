using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace TollMesh.Cache
{
    public class ClientConfig
    {
        public string Host { get; set; } = "localhost";
        public int Port { get; set; } = 8080;
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);
        public bool VerifySSL { get; set; } = true;
        public string? ApiKey { get; set; }
        public string Scheme { get; set; } = "http";

        public string GetBaseUrl() => $"{Scheme}://{Host}:{Port}";
    }

    public class ConsumeResult
    {
        [JsonPropertyName("ok")]
        public bool Ok { get; set; }

        [JsonPropertyName("remaining")]
        public int Remaining { get; set; }

        [JsonPropertyName("reset_at")]
        public long ResetAt { get; set; }

        [JsonPropertyName("error")]
        public string? Error { get; set; }
    }

    public class SeenResult
    {
        [JsonPropertyName("seen")]
        public bool Seen { get; set; }

        [JsonPropertyName("error")]
        public string? Error { get; set; }
    }

    public class CacheValue
    {
        [JsonPropertyName("value")]
        public string? Value { get; set; }

        [JsonPropertyName("exists")]
        public bool Exists { get; set; }

        [JsonPropertyName("error")]
        public string? Error { get; set; }
    }

    public class HealthResponse
    {
        [JsonPropertyName("status")]
        public string Status { get; set; } = string.Empty;

        [JsonPropertyName("node")]
        public string Node { get; set; } = string.Empty;

        [JsonPropertyName("peers")]
        public int Peers { get; set; }
    }

    public class Client : IDisposable
    {
        private readonly ClientConfig _config;
        private readonly HttpClient _httpClient;

        public Client(ClientConfig? config = null)
        {
            _config = config ?? new ClientConfig();
            _httpClient = new HttpClient { Timeout = _config.Timeout };

            if (_config.ApiKey != null)
            {
                _httpClient.DefaultRequestHeaders.Add("X-API-Key", _config.ApiKey);
            }
        }

        public async Task<ConsumeResult> ConsumeAsync(string key, int limit, TimeSpan window)
        {
            var body = new { key, limit, window = (long)window.TotalMilliseconds };
            return await PostAsync<ConsumeResult>("/consume", body);
        }

        public async Task<SeenResult> SeenAsync(string key, TimeSpan ttl)
        {
            var body = new { key, ttl = (long)ttl.TotalMilliseconds };
            return await PostAsync<SeenResult>("/seen", body);
        }

        public async Task<CacheValue> CacheGetAsync(string ns, string key)
        {
            var body = new { @namespace = ns, key };
            return await PostAsync<CacheValue>("/cache/get", body);
        }

        public async Task CacheSetAsync(string ns, string key, string value, TimeSpan? ttl = null)
        {
            var body = new { @namespace = ns, key, value, ttl = ttl?.TotalMilliseconds };
            await PostAsync<object>("/cache/set", body);
        }

        public async Task<HealthResponse> HealthAsync()
        {
            return await GetAsync<HealthResponse>("/health");
        }

        private async Task<T> PostAsync<T>(string endpoint, object body)
        {
            var url = _config.GetBaseUrl() + endpoint;
            var json = JsonSerializer.Serialize(body);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync(url, content);
            return await HandleResponse<T>(response);
        }

        private async Task<T> GetAsync<T>(string endpoint)
        {
            var url = _config.GetBaseUrl() + endpoint;
            var response = await _httpClient.GetAsync(url);
            return await HandleResponse<T>(response);
        }

        private async Task<T> HandleResponse<T>(HttpResponseMessage response)
        {
            var content = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"HTTP {response.StatusCode}: {content}");
            }

            return JsonSerializer.Deserialize<T>(content) ?? throw new Exception("Failed to deserialize response");
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }
}
