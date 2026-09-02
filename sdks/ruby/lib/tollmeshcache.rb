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

    # ===== Pub/Sub =====

    def subscribe(subscriber_id, topic, pattern: '')
      post('/pubsub/subscribe', { subscriber_id: subscriber_id, topic: topic, pattern: pattern })
    end

    def unsubscribe(subscriber_id, topic)
      post('/pubsub/unsubscribe', { subscriber_id: subscriber_id, topic: topic })
    end

    def publish(topic, publisher, payload)
      response = post('/pubsub/publish', { topic: topic, publisher: publisher, payload: payload })
      response['delivered_count'] || 0
    end

    def poll(subscriber_id, limit: 10, timeout_ms: 5000)
      response = post('/pubsub/poll', { subscriber_id: subscriber_id, limit: limit, timeout_ms: timeout_ms })
      response['messages'] || []
    end

    def get_topics
      response = get('/pubsub/topics')
      response['topics'] || []
    end

    def get_topic_subscribers(topic)
      response = get('/pubsub/subscribers', query: { topic: topic })
      response['subscribers'] || []
    end

    def pubsub_stats
      get('/pubsub/stats')
    end

    # ===== Transactions =====

    def begin_transaction(txn_id)
      post('/txn/begin', { txn_id: txn_id })
    end

    def add_transaction_operation(txn_id, type, namespace, key, value = '')
      post('/txn/operation', { txn_id: txn_id, type: type, namespace: namespace, key: key, value: value })
    end

    def commit_transaction(txn_id)
      post('/txn/commit', { txn_id: txn_id })
    end

    def rollback_transaction(txn_id)
      post('/txn/rollback', { txn_id: txn_id })
    end

    def transaction_status(txn_id)
      response = get('/txn/status', query: { txn_id: txn_id })
      response['status']
    end

    # ===== Persistence =====

    def create_snapshot
      post('/persistence/snapshot', {})
    end

    def get_latest_snapshot
      get('/persistence/snapshot/latest')
    rescue Error
      nil
    end

    def restore_from_latest_snapshot
      post('/persistence/restore', {})
    end

    def persistence_stats
      get('/persistence/stats')
    end

    # ===== Scripting: Pipelines (safe operation composition) =====

    def register_pipeline(name, steps)
      post('/pipeline/register', { name: name, steps: steps })
    end

    def execute_pipeline(name)
      post('/pipeline/execute', { name: name })
    end

    def execute_inline_pipeline(steps)
      post('/pipeline/execute-inline', { steps: steps })
    end

    def get_pipeline(name)
      get('/pipeline/get', query: { name: name })
    end

    def list_pipelines
      response = get('/pipeline/list')
      response['pipelines'] || []
    end

    def delete_pipeline(name)
      post('/pipeline/delete', { name: name })
    end

    # ===== Scripting: WASM (real arbitrary Go code execution) =====

    def compile_script(name, source)
      post('/script/compile', { name: name, source: source })
    end

    def execute_script(name, input = '')
      response = post('/script/execute', { name: name, input: input })
      response['output'] || ''
    end

    def execute_inline_script(source, input = '')
      response = post('/script/execute-inline', { source: source, input: input })
      response['output'] || ''
    end

    def get_script(name)
      get('/script/get', query: { name: name })
    end

    def list_scripts
      response = get('/script/list')
      response['scripts'] || []
    end

    def delete_script(name)
      post('/script/delete', { name: name })
    end

    # ===== Search =====

    def index_document(id, content, metadata: nil, vector: nil)
      body = { id: id, content: content }
      body[:metadata] = metadata if metadata
      body[:vector] = vector if vector
      post('/search/index', body)
    end

    def search_bm25(query, top_k: 10)
      response = get('/search/bm25', query: { query: query, topk: top_k })
      response['results'] || []
    end

    def search_vector(vector, top_k: 10)
      response = post('/search/vector', { vector: vector, topk: top_k })
      response['results'] || []
    end

    def search_hybrid(query, vector, top_k: 10)
      response = post('/search/hybrid', { query: query, vector: vector, topk: top_k })
      response['results'] || []
    end

    def delete_search_document(id)
      post('/search/delete', { id: id })
    end

    # ===== Ranking =====

    def rank(items, strategy: 'bm25', boosts: nil)
      body = { items: items, strategy: strategy }
      body[:boosts] = boosts if boosts
      response = post('/rank', body)
      response['items'] || []
    end

    # ===== Metrics =====

    def get_metrics
      get('/metrics')
    end

    def get_prometheus_metrics
      url = @config.base_url + '/metrics/prometheus'
      response = @http.get(url, header: { 'User-Agent' => 'tollmeshcache-ruby/1.0.0' })
      response.body
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
