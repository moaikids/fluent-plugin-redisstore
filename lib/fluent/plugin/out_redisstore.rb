module Fluent
  class RedisOutput < BufferedOutput
    Fluent::Plugin.register_output('redisstore', self)
    attr_reader :host, :port, :db_number, :redis, :key_prefix, :key_suffix, :store_type, :key_name, :score_name, :value_name, :key_expire, :value_expire, :value_length, :list_order

    def initialize
      super
      require 'redis'
      require 'msgpack'
    end

    def configure(conf)
      super

      @host = conf.has_key?('host') ? conf['host'] : 'localhost'
      @port = conf.has_key?('port') ? conf['port'].to_i : 6379
      @db_number = conf.has_key?('db_number') ? conf['db_number'].to_i : nil

      @key_prefix = conf.has_key?('key_prefix') ? conf['key_prefix'] : ''
      @key_suffix = conf.has_key?('key_suffix') ? conf['key_suffix'] : ''
      @store_type = conf.has_key?('store_type') ? conf['store_type'] : 'zset'
      @key_name = conf['key_name']
      @score_name = conf['score_name']
      @value_name = conf['value_name']
      @key_expire = conf.has_key?('key_expire') ? conf['key_expire'].to_i : -1
      @value_expire = conf.has_key?('value_expire') ? conf['value_expire'].to_i : -1
      @value_length = conf.has_key?('value_length') ? conf['value_length'].to_i : -1
      @list_order = conf.has_key?('list_order') ? conf['list_order'] : 'asc'
    end

    def start
      super

      @redis = Redis.new(:host => @host, :port => @port,
                         :thread_safe => true, :db => @db_number)
    end

    def shutdown
      @redis.quit
    end

    def format(tag, time, record)
      identifier = [tag, time].join(".")
      [identifier, record].to_msgpack
    end

    def write(chunk)
      @redis.pipelined {
        chunk.open { |io|
          begin
            MessagePack::Unpacker.new(io).each { |message|
              begin
                (tag, record) = message
                if @store_type == 'zset'
                  operation_for_zset(record)
                elsif @store_type == 'set'
                  operation_for_set(record)
                elsif @store_type == 'list'
                  operation_for_list(record)
                elsif @store_type == 'string'
                  operation_for_string(record)
                end
              rescue NoMethodError => e
                puts e
              end
            }
          rescue EOFError
            # EOFError always occured when reached end of chunk.
          end
        }
      }
    end

    def operation_for_zset(record)
      now = Time.now.to_i
      k = traverse(record, @key_name).to_s
      if @score_name
        s = traverse(record, @score_name)
      else
        s = now
      end
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix
      
      @redis.multi do
        @redis.zadd sk , s, v
        if @key_expire > 0
          @redis.expire sk , @key_expire
        end
        if @value_expire > 0
          @redis.zremrangebyscore sk , '-inf' , (now - @value_expire)
        end
      end
      if @value_length > 0
        len = @redis.zcount sk, '-inf', '+inf'
        if len > @value_length
          i = len - @value_length - 1
          if i >= 0
            @redis.zremrangebyrank sk, 0, i
          end
        end
      end
    end

    def operation_for_set(record)
      k = traverse(record, @key_name).to_s
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix
              
      @redis.sadd sk, v
      if @key_expire > 0
        @redis.expire sk, @key_expire
      end
    end

    def operation_for_list(record)
      k = traverse(record, @key_name).to_s
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix

      @redis.multi do
        if @list_order == 'asc'
          @redis.rpush sk, v
        else
          @redis.lpush sk, v
        end             
        if @key_expire > 0
          @redis.expire sk, @key_expire
        end
      end
      if @value_length > 0
        len = @redis.llen sk
        if len > @value_length
          if @list_order == 'asc'
            i = len - @value_length - 1
            if i > 0
              @redis.lrem sk, 0, i
            end 
          else
            @redis.lrem sk, @value_length,  -1
          end
        end
      end 
    end

    def operation_for_string(record)
      k = traverse(record, @key_name).to_s
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix
         
      @redis.multi do        
        @redis.set sk, v
        if @key_expire > 0
          @redis.expire sk, @key_expire
        end
      end
    end


    def traverse(data, key)
      val = data
      key.split('.').each{ |k|
        if val.has_key?(k)
          val = val[k]
        else
          return nil
        end
      }
      return val
    end
  end
end
