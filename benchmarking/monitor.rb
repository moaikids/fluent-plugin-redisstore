#!/usr/bin/env ruby

require 'rubygems'
require 'terminal-table'
require 'pp'
require 'redis'
require 'hiredis'

class RedisInfo
  def initialize(host,port)
    @redis = Redis.new(:host => host, :port => port, driver: :hiredis)
  end
  def get_total_commands_processed
    @redis.info["total_commands_processed"].to_i
  end
end

chk = RedisInfo.new("127.0.0.1","6379")
previous = 0

res = []
(1..30).each do |i|
  current = chk.get_total_commands_processed
  res << current - previous
  previous = current
  sleep 1
end

res.shift
puts "#{1.0 * res.inject(:+) / res.size} req/s"
