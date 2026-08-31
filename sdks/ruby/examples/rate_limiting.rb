#!/usr/bin/env ruby

require_relative '../lib/tollmeshcache'

config = TollMeshCache::ClientConfig.new(host: 'localhost', port: 8080)
client = TollMeshCache::Client.new(config)

puts "=" * 60
puts "TollMeshCache - Rate Limiting Example (Ruby)"
puts "=" * 60

begin
  puts "\n1. Basic Rate Limiting (100 req/min)"
  puts "-" * 60

  3.times do |i|
    result = client.consume("user-123", 100, 60000)
    puts "Request #{i+1}:"
    puts "  Status: #{result['ok'] ? 'ALLOWED' : 'LIMITED'}"
    puts "  Remaining: #{result['remaining']}"
  end

  puts "\n2. Tier-Based Rate Limiting"
  puts "-" * 60

  tiers = { free: 10, pro: 100, enterprise: 1000 }
  tiers.each do |tier, limit|
    result = client.consume("user-tier-#{tier}", limit, 60000)
    status = result['ok'] ? '✓ OK' : '✗ LIMITED'
    puts "#{tier.to_s.upcase.ljust(12)}: #{status} (#{result['remaining']} remaining)"
  end

  puts "\n3. Server Health"
  puts "-" * 60

  health = client.health
  puts "Status: #{health['status']}"
  puts "Node: #{health['node']}"
  puts "Peers: #{health['peers']}"

rescue TollMeshCache::RateLimitError => e
  puts "Rate limited: #{e.message}"
rescue TollMeshCache::Error => e
  puts "Error: #{e.message}"
ensure
  client.close
  puts "\n" + "=" * 60
  puts "Example complete!"
  puts "=" * 60
end
