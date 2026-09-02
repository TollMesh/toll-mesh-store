using System;
using System.Collections.Generic;
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

    public class Job
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = string.Empty;

        [JsonPropertyName("queue")]
        public string Queue { get; set; } = string.Empty;

        [JsonPropertyName("payload")]
        public string Payload { get; set; } = string.Empty;

        [JsonPropertyName("status")]
        public string Status { get; set; } = string.Empty;

        [JsonPropertyName("priority")]
        public int Priority { get; set; }

        [JsonPropertyName("retry_count")]
        public int RetryCount { get; set; }

        [JsonPropertyName("max_retries")]
        public int MaxRetries { get; set; }

        [JsonPropertyName("result")]
        public string? Result { get; set; }

        [JsonPropertyName("error")]
        public string Error { get; set; } = string.Empty;

        [JsonPropertyName("created_at")]
        public long CreatedAt { get; set; }

        [JsonPropertyName("deadline_at")]
        public long DeadlineAt { get; set; }
    }

    public class SortedSetMember
    {
        [JsonPropertyName("member")]
        public string Member { get; set; } = string.Empty;

        [JsonPropertyName("score")]
        public double Score { get; set; }

        [JsonPropertyName("timestamp")]
        public long Timestamp { get; set; }

        [JsonPropertyName("node")]
        public string Node { get; set; } = string.Empty;
    }

    public class ZScoreResponse
    {
        [JsonPropertyName("score")]
        public double? Score { get; set; }

        [JsonPropertyName("exists")]
        public bool Exists { get; set; }
    }

    public class ZRankResponse
    {
        [JsonPropertyName("rank")]
        public long? Rank { get; set; }

        [JsonPropertyName("exists")]
        public bool Exists { get; set; }
    }

    public class ZRangeResponse
    {
        [JsonPropertyName("members")]
        public List<SortedSetMember>? Members { get; set; }
    }

    public class ZCardResponse
    {
        [JsonPropertyName("card")]
        public long Card { get; set; }
    }

    public class StreamEntry
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = string.Empty;

        [JsonPropertyName("timestamp")]
        public long Timestamp { get; set; }

        [JsonPropertyName("fields")]
        public Dictionary<string, string> Fields { get; set; } = new();

        [JsonPropertyName("node")]
        public string Node { get; set; } = string.Empty;

        [JsonPropertyName("sequence")]
        public long Sequence { get; set; }
    }

    public class XRangeResponse
    {
        [JsonPropertyName("entries")]
        public List<StreamEntry>? Entries { get; set; }
    }

    public class XLenResponse
    {
        [JsonPropertyName("length")]
        public long Length { get; set; }
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

        // ===== Job Queues =====

        public async Task<Job> EnqueueAsync(string queue, string payload, int priority = 5, int maxRetries = 3, TimeSpan? deadline = null)
        {
            var body = new
            {
                queue,
                payload,
                priority,
                max_retries = maxRetries,
                deadline = deadline.HasValue ? (long?)deadline.Value.TotalMilliseconds : null,
            };
            return await PostAsync<Job>("/queue/enqueue", body);
        }

        public async Task<Job> ClaimAsync(string queue, string workerId)
        {
            var body = new { queue, worker_id = workerId };
            return await PostAsync<Job>("/queue/claim", body);
        }

        public async Task CompleteAsync(string queue, string jobId, string result = "")
        {
            var body = new { queue, job_id = jobId, result };
            await PostAsync<object>("/queue/complete", body);
        }

        public async Task FailAsync(string queue, string jobId, string error)
        {
            var body = new { queue, job_id = jobId, error };
            await PostAsync<object>("/queue/fail", body);
        }

        public async Task<Job> JobStatusAsync(string queue, string jobId)
        {
            return await GetAsync<Job>("/queue/status", new() { ["queue"] = queue, ["job_id"] = jobId });
        }

        public async Task<Dictionary<string, JsonElement>> QueueStatsAsync(string queue)
        {
            return await GetAsync<Dictionary<string, JsonElement>>("/queue/stats", new() { ["queue"] = queue });
        }

        // ===== Sorted Sets =====

        public async Task ZAddAsync(string key, string member, double score)
        {
            var body = new { key, member, score };
            await PostAsync<object>("/zset/add", body);
        }

        public async Task ZRemAsync(string key, string member)
        {
            var body = new { key, member };
            await PostAsync<object>("/zset/remove", body);
        }

        public async Task<ZScoreResponse> ZScoreAsync(string key, string member)
        {
            return await GetAsync<ZScoreResponse>("/zset/score", new() { ["key"] = key, ["member"] = member });
        }

        public async Task<ZRankResponse> ZRankAsync(string key, string member)
        {
            return await GetAsync<ZRankResponse>("/zset/rank", new() { ["key"] = key, ["member"] = member });
        }

        public async Task<ZRankResponse> ZRevRankAsync(string key, string member)
        {
            return await GetAsync<ZRankResponse>("/zset/revrank", new() { ["key"] = key, ["member"] = member });
        }

        public async Task<List<SortedSetMember>> ZRangeAsync(string key, double min = double.NegativeInfinity, double max = double.PositiveInfinity, long limit = 100)
        {
            var response = await GetAsync<ZRangeResponse>("/zset/range", new()
            {
                ["key"] = key,
                ["min"] = min.ToString(),
                ["max"] = max.ToString(),
                ["limit"] = limit.ToString(),
            });
            return response.Members ?? new List<SortedSetMember>();
        }

        /// <summary>
        /// Get members with scores in [min, max], descending order (highest first).
        /// Following Redis's ZREVRANGEBYSCORE convention, max comes before min.
        /// </summary>
        public async Task<List<SortedSetMember>> ZRevRangeAsync(string key, double max = double.PositiveInfinity, double min = double.NegativeInfinity, long limit = 100)
        {
            var response = await GetAsync<ZRangeResponse>("/zset/revrange", new()
            {
                ["key"] = key,
                ["max"] = max.ToString(),
                ["min"] = min.ToString(),
                ["limit"] = limit.ToString(),
            });
            return response.Members ?? new List<SortedSetMember>();
        }

        public async Task<long> ZCardAsync(string key)
        {
            var response = await GetAsync<ZCardResponse>("/zset/card", new() { ["key"] = key });
            return response.Card;
        }

        // ===== Streams =====

        public async Task<StreamEntry> XAddAsync(string stream, Dictionary<string, string> fields)
        {
            var body = new { stream, fields };
            return await PostAsync<StreamEntry>("/stream/add", body);
        }

        public async Task<List<StreamEntry>> XRangeAsync(string stream, string start = "0", string end = "-", long limit = 100)
        {
            var response = await GetAsync<XRangeResponse>("/stream/range", new()
            {
                ["stream"] = stream,
                ["start"] = start,
                ["end"] = end,
                ["limit"] = limit.ToString(),
            });
            return response.Entries ?? new List<StreamEntry>();
        }

        public async Task<long> XLenAsync(string stream)
        {
            var response = await GetAsync<XLenResponse>("/stream/len", new() { ["stream"] = stream });
            return response.Length;
        }

        public async Task XGroupCreateAsync(string stream, string group)
        {
            var body = new { stream, group };
            await PostAsync<object>("/stream/group/create", body);
        }

        /// <summary>
        /// Read unacknowledged entries for a consumer in a group. First call for a
        /// given consumer registers it in the group. Entries remain re-deliverable
        /// until acknowledged with XAckAsync.
        /// </summary>
        public async Task<List<StreamEntry>> XReadGroupAsync(string group, string consumer, string stream, long limit = 100)
        {
            var body = new { stream, group, consumer, limit };
            var response = await PostAsync<XRangeResponse>("/stream/group/read", body);
            return response.Entries ?? new List<StreamEntry>();
        }

        public async Task XAckAsync(string stream, string group, string consumer, string entryId)
        {
            var body = new { stream, group, consumer, id = entryId };
            await PostAsync<object>("/stream/group/ack", body);
        }

        private async Task<T> PostAsync<T>(string endpoint, object body)
        {
            var url = _config.GetBaseUrl() + endpoint;
            var json = JsonSerializer.Serialize(body);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync(url, content);
            return await HandleResponse<T>(response);
        }

        private async Task<T> GetAsync<T>(string endpoint, Dictionary<string, string>? query = null)
        {
            var url = _config.GetBaseUrl() + endpoint;
            if (query != null && query.Count > 0)
            {
                var pairs = new List<string>();
                foreach (var kv in query)
                {
                    pairs.Add($"{Uri.EscapeDataString(kv.Key)}={Uri.EscapeDataString(kv.Value)}");
                }
                url += "?" + string.Join("&", pairs);
            }

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
