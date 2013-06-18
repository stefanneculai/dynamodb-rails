# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'dynamo/version'

Gem::Specification.new do |spec|
  spec.name          = "awesome-dynamodb"
  spec.version       = Dynamo::VERSION
  spec.authors       = ["Stefan"]
  spec.email         = ["stefan.neculai@gmail.com"]
  spec.description   = %q{TODO: Write a gem description}
  spec.summary       = %q{TODO: Write a gem summary}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"

  spec.add_dependency(%q<activemodel>, [">= 0"])
  spec.add_dependency(%q<tzinfo>, [">= 0"])
  spec.add_dependency(%q<rake>, [">= 0"])
  spec.add_dependency(%q<aws-sdk>, [">= 0"])
  spec.add_dependency(%q<rspec>, [">= 0"])
  spec.add_dependency(%q<bundler>, [">= 0"])
  spec.add_dependency(%q<jeweler>, [">= 0"])
  spec.add_dependency(%q<yard>, [">= 0"])
  spec.add_dependency(%q<redcarpet>, ["= 1.17.2"])
  spec.add_dependency(%q<github-markup>, [">= 0"])
  spec.add_dependency(%q<pry>, [">= 0"])
end
