Gem::Specification.new do |s|
  s.name          = "tollmeshcache"
  s.version       = "1.1.1"
  s.authors       = ["TollMesh Team"]
  s.email         = ["team@tollmesh.io"]
  s.summary       = "Ruby SDK for TollMeshCache"
  s.description   = "Distributed CRDT-based caching and coordination"
  s.homepage      = "https://github.com/toll-mesh/store"
  s.license       = "Apache-2.0"

  s.files         = Dir.glob("lib/**/*.rb") + ["README.md", "LICENSE"]
  s.require_paths = ["lib"]
  s.required_ruby_version = ">= 2.7.0"

  s.add_dependency "httpclient", "~> 2.8"
  s.add_dependency "json", "~> 2.6"

  s.add_development_dependency "bundler", "~> 2.3"
  s.add_development_dependency "rake", "~> 13.0"
  s.add_development_dependency "rspec", "~> 3.11"
  s.add_development_dependency "rubocop", "~> 1.31"
  s.add_development_dependency "grpc", "~> 1.48"
  s.add_development_dependency "grpc-tools", "~> 1.48"
end
