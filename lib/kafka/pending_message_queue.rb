# frozen_string_literal: true

module Kafka

  class PendingMessageQueue
    attr_reader :size, :bytesize

    def initialize(id)
      @redis = Redis.new(host: "127.0.0.1", port: 6379, db: 15)
      @namespace = "pending_message_queue_#{id}"
    end

    def write(message)
      @redis.rpush(@namespace, Marshal::dump(message))
    end

    def size
      @redis.llen(@namespace)
    end

    def bytesize
      @redis.lrange(@namespace, 0, -1).inject(0) do |sum, v|
        Marshal::load(v).bytesize
      end
    end

    def empty?
      size == 0
    end

    def clear
      @redis.del(@namespace)
    end

    def replace(messages)
      clear
      messages.each {|message| write(message) }
    end

    # Yields each message in the queue.
    #
    # @yieldparam [PendingMessage] message
    # @return [nil]
    def each(&block)

      @redis.lrange(@namespace, 0, 1).each do |m|
        block.call(Marshal::load(m))
      end
    end
  end
end
