require 'httpclient'
require 'json'

module TollMeshCache
  class Error < StandardError; end
  class RateLimitError < Error; end
  class ReplayError < Error; end
  class CacheMissError < Error; end

  class ClientConfig
    attr_accessor :host, :port, :timeout, :verify_ssl, :api_key, :scheme

    def initialize(host: 'localhost', port: 8080, timeout: 5, verify_ssl: true, scheme: 'http')
      @host = host
      @port = port
      @timeout = timeout
      @verify_ssl = verify_ssl
      @scheme = scheme
      @api_key = nil
    end

    def base_url
      "#{@scheme}://#{@host}:#{@port}"
    end
  end

  class Client
    def initialize(config = nil)
      @config = config || ClientConfig.new
      @http = HTTPClient.new
      @http.receive_timeout = @config.timeout
    end

    def consume(key, limit, window_ms)
      body = {
        key: key,
        limit: limit,
        window: window_ms
      }
      post('/consume', body)
    end

    def seen(key, ttl_ms)
      body = { key: key, ttl: ttl_ms }
      post('/seen', body)
    end

    def cache_get(namespace, key)
      # /cache/get is a GET endpoint taking query params (see
      # api/http.go handleCacheGet), not a POST with a JSON body.
      get('/cache/get', query: { namespace: namespace, key: key })
    end

    def cache_set(namespace, key, value, ttl_ms = nil)
      body = {
        namespace: namespace,
        key: key,
        value: value
      }
      body[:ttl] = ttl_ms if ttl_ms
      post('/cache/set', body)
    end

    def health
      get('/health')
    end

    def get_peers
      response = get('/peers')
      response['peers'] || []
    end

    # ===== Job Queues =====

    def enqueue(queue, payload, priority: 5, max_retries: 3, deadline_ms: nil)
      body = { queue: queue, payload: payload, priority: priority, max_retries: max_retries }
      body[:deadline] = deadline_ms if deadline_ms
      post('/queue/enqueue', body)
    end

    def claim(queue, worker_id)
      post('/queue/claim', { queue: queue, worker_id: worker_id })
    end

    def complete(queue, job_id, result = '')
      post('/queue/complete', { queue: queue, job_id: job_id, result: result })
    end

    def fail_job(queue, job_id, error)
      post('/queue/fail', { queue: queue, job_id: job_id, error: error })
    end

    def job_status(queue, job_id)
      get('/queue/status', query: { queue: queue, job_id: job_id })
    end

    def queue_stats(queue)
      get('/queue/stats', query: { queue: queue })
    end

    # ===== Sorted Sets =====

    def zadd(key, score, member)
      post('/zset/add', { key: key, member: member, score: score })
    end

    def zrem(key, member)
      post('/zset/remove', { key: key, member: member })
    end

    def zscore(key, member)
      response = get('/zset/score', query: { key: key, member: member })
      [response['score'], response['exists']]
    end

    def zrank(key, member)
      response = get('/zset/rank', query: { key: key, member: member })
      [response['rank'], response['exists']]
    end

    def zrevrank(key, member)
      response = get('/zset/revrank', query: { key: key, member: member })
      [response['rank'], response['exists']]
    end

    def zrange(key, min: -Float::INFINITY, max: Float::INFINITY, limit: 100)
      response = get('/zset/range', query: { key: key, min: min, max: max, limit: limit })
      response['members'] || []
    end

    def zrevrange(key, max: Float::INFINITY, min: -Float::INFINITY, limit: 100)
      response = get('/zset/revrange', query: { key: key, max: max, min: min, limit: limit })
      response['members'] || []
    end

    def zcard(key)
      response = get('/zset/card', query: { key: key })
      response['card'] || 0
    end

    # ===== Streams =====

    def xadd(stream, fields)
      post('/stream/add', { stream: stream, fields: fields })
    end

    def xrange(stream, start = '0', end_id = '-', limit: 100)
      response = get('/stream/range', query: { stream: stream, start: start, end: end_id, limit: limit })
      response['entries'] || []
    end

    def xlen(stream)
      response = get('/stream/len', query: { stream: stream })
      response['length'] || 0
    end

    def xgroup_create(stream, group)
      post('/stream/group/create', { stream: stream, group: group })
    end

    def xreadgroup(group, consumer, stream, limit: 100)
      response = post('/stream/group/read', { stream: stream, group: group, consumer: consumer, limit: limit })
      response['entries'] || []
    end

    def xack(stream, group, consumer, entry_id)
      post('/stream/group/ack', { stream: stream, group: group, consumer: consumer, id: entry_id })
    end

    def close
      @http.close if @http
    end

    private

    def post(endpoint, body)
      url = @config.base_url + endpoint
      headers = {
        'Content-Type' => 'application/json',
        'User-Agent' => 'tollmeshcache-ruby/1.0.0'
      }
      headers['X-API-Key'] = @config.api_key if @config.api_key

      response = @http.post(url, JSON.generate(body), headers)
      handle_response(response)
    end

    def get(endpoint, query: nil)
      url = @config.base_url + endpoint
      headers = { 'User-Agent' => 'tollmeshcache-ruby/1.0.0' }
      headers['X-API-Key'] = @config.api_key if @config.api_key

      response = @http.get(url, query: query, header: headers)
      handle_response(response)
    end

    def handle_response(response)
      if response.status >= 400
        data = JSON.parse(response.body) rescue { 'code' => response.status }
        code = data['code'] || response.status
        # /consume, /seen, /cache/* use "message"; the job queue, sorted
        # set, and stream endpoints use ErrorResponse{"error": ...} from
        # api/http.go.
        message = data['message'] || data['error'] || "HTTP #{response.status}"

        case code
        when 429
          raise RateLimitError, message
        when 1001
          raise ReplayError, message
        when 1002
          raise CacheMissError, message
        else
          raise Error, message
        end
      end

      JSON.parse(response.body)
    end
  end
end
