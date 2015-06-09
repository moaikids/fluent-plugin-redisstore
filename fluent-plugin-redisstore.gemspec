# -*- encoding: utf-8 -*-
Gem::Specification.new do |gem|
    gem.name        = "fluent-plugin-redisstore"
    gem.version     = "0.0.2"
    gem.authors     = ["moaikids"]
    gem.licenses    = ["Apache License Version 2.0"]
    gem.summary     = %q{Redis(zset/set/list/string) output plugin for Fluentd}
    gem.description = %q{Redis(zset/set/list/string) output plugin for Fluentd...}
    gem.homepage    = "https://github.com/moaikids/fluent-plugin-redisstore"

    gem.files         = `git ls-files`.split($\)
    gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
    gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
    gem.require_paths = ["lib"]

    gem.add_development_dependency "rake"
    gem.add_development_dependency "shoulda"
    gem.add_runtime_dependency "fluentd"
end
