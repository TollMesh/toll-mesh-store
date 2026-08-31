require 'spec_helper'
require_relative '../lib/tollmeshcache'

RSpec.describe TollMeshCache::Client do
  let(:config) { TollMeshCache::ClientConfig.new(host: 'localhost', port: 8080) }
  let(:client) { TollMeshCache::Client.new(config) }

  after { client.close }

  describe '#consume' do
    it 'returns consume result' do
      result = client.consume('user-123', 100, 60000)
      expect(result).to be_a(Hash)
      expect(result).to have_key('ok')
      expect(result).to have_key('remaining')
      expect(result).to have_key('reset_at')
    end
  end

  describe '#seen' do
    it 'returns seen result' do
      result = client.seen('nonce-123', 300000)
      expect(result).to be_a(Hash)
      expect(result).to have_key('seen')
    end
  end

  describe '#cache_get' do
    it 'returns cache value' do
      result = client.cache_get('namespace', 'key')
      expect(result).to be_a(Hash)
      expect(result).to have_key('exists')
    end
  end

  describe '#health' do
    it 'returns health response' do
      result = client.health
      expect(result).to be_a(Hash)
      expect(result).to have_key('status')
      expect(result).to have_key('node')
      expect(result).to have_key('peers')
    end
  end

  describe 'config' do
    it 'builds base url' do
      expect(config.base_url).to eq('http://localhost:8080')
    end
  end
end
