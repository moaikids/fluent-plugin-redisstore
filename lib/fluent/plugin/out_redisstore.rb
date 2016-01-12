module Fluent
  class RedisOutput < BufferedOutput
    Fluent::Plugin.register_output('redisstore', self)
    attr_reader :host, :port, :db_number, :redis, :timeout, :key_prefix, :key_suffix, :store_type, :key_name, :fixed_key_value, :score_name, :value_name, :key_expire, :value_expire, :value_length, :order

    def initialize
      super
      require 'redis'
      require 'msgpack'
    end

    def configure(conf)
      super

      @driver = conf.has_key?('driver') ? conf['driver'] : nil

      @host = conf.has_key?('host') ? conf['host'] : 'localhost'
      @port = conf.has_key?('port') ? conf['port'].to_i : 6379
      @db_number = conf.has_key?('db_number') ? conf['db_number'].to_i : nil
      @timeout = conf.has_key?('timeout') ? conf['timeout'].to_f : 5.0

      @key_prefix = conf.has_key?('key_prefix') ? conf['key_prefix'] : ''
      @key_suffix = conf.has_key?('key_suffix') ? conf['key_suffix'] : ''
      @store_type = conf.has_key?('store_type') ? conf['store_type'] : 'zset'
      @key_name = conf['key_name']
      @fixed_key_value = conf.has_key?('fixed_key_value') ? conf['fixed_key_value'] : nil
      @score_name = conf['score_name']
      @increment_score = conf.has_key?('increment_score') ? conf['increment_score'] : 1.0
      @value_name = conf['value_name']
      @key_expire = conf.has_key?('key_expire') ? conf['key_expire'].to_i : -1
      @value_expire = conf.has_key?('value_expire') ? conf['value_expire'].to_i : -1
      @value_length = conf.has_key?('value_length') ? conf['value_length'].to_i : -1
      @order = conf.has_key?('order') ? conf['order'] : 'asc'
    end

    def start
      super

      opt = {
        :host => @host,
        :port => @port,
        :db => @db_number,
        :timeout => @timeout,
        :thread_safe => true,
      }

      if @driver
        opt[:driver] = @driver.to_sym
      end

      @redis = Redis.new(opt)
    end

    def shutdown
      super

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
                elsif @store_type == 'zincrby'
                  operation_for_zincrby(record)
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
      if @fixed_key_value
        k = @fixed_key_value
      else
        k = traverse(record, @key_name).to_s
      end
      if @score_name
        s = traverse(record, @score_name)
      else
        s = now
      end
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix

      @redis.zadd sk , s, v
      if @key_expire > 0
        @redis.expire sk , @key_expire
      end
      if @value_expire > 0
        @redis.zremrangebyscore sk , '-inf' , (now - @value_expire)
      end
      if @value_length > 0
        script = generate_zremrangebyrank_script(sk, @value_length, @order)
        @redis.eval script
      end
    end

    def operation_for_zincrby(record)
      if @fixed_key_value
        k = @fixed_key_value
      else
        k = traverse(record, @key_name).to_s
      end
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix
      
      @redis.zincrby sk , @increment_score, v
      if @key_expire > 0
        @redis.expire sk , @key_expire
      end
      if @value_length > 0
        script = generate_zremrangebyrank_script(sk, @value_length, @order)
        @redis.eval script
      end
    end

    def operation_for_set(record)
      if @fixed_key_value
        k = @fixed_key_value
      else
        k = traverse(record, @key_name).to_s
      end
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix

      @redis.sadd sk, v
      if @key_expire > 0
        @redis.expire sk, @key_expire
      end
    end

    def operation_for_list(record)
      if @fixed_key_value
        k = @fixed_key_value
      else
        k = traverse(record, @key_name).to_s
      end
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix

      if @order == 'asc'
        @redis.rpush sk, v
      else
        @redis.lpush sk, v
      end
      if @key_expire > 0
        @redis.expire sk, @key_expire
      end
      if @value_length > 0
        script = generate_ltrim_script(sk, @value_length, @order)
        @redis.eval script
      end
    end

    def operation_for_string(record)
      if @fixed_key_value
        k = @fixed_key_value
      else
        k = traverse(record, @key_name).to_s
      end
      v = traverse(record, @value_name)
      sk = @key_prefix + k + @key_suffix

      @redis.set sk, v
      if @key_expire > 0
        @redis.expire sk, @key_expire
      end
    end

    def generate_zremrangebyrank_script(key, maxlen, order)
      script  = "local key = '" + key.to_s + "'\n"
      script += "local maxlen = " + maxlen.to_s + "\n"
      script += "local order ='" + order.to_s + "'\n"
      script += "local len = tonumber(redis.call('ZCOUNT', key, '-inf', '+inf'))\n"
      script += "if len > maxlen then\n"
      script += "    if order == 'asc' then\n"
      script += "       local l = len - maxlen\n"
      script += "       if l >= 0 then\n"
      script += "           return redis.call('ZREMRANGEBYRANK', key, 0, l)\n"
      script += "       end\n"
      script += "    else\n"
      script += "       return redis.call('ZREMRANGEBYRANK', key, maxlen, -1)\n"
      script += "    end\n"
      script += "end\n"
      return script
    end

    def generate_ltrim_script(key, maxlen, order)
      script  = "local key = '" + key.to_s + "'\n"
      script += "local maxlen = " + maxlen.to_s + "\n"
      script += "local order ='" + order.to_s + "'\n"
      script += "local len = tonumber(redis.call('LLEN', key))\n"
      script += "if len > maxlen then\n"
      script += "    if order == 'asc' then\n"
      script += "        local l = len - maxlen\n"
      script += "        return redis.call('LTRIM', key, l, -1)\n"
      script += "    else\n"
      script += "        return redis.call('LTRIM', key, 0, maxlen - 1)\n"
      script += "    end\n"
      script += "end\n"
      return script
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
