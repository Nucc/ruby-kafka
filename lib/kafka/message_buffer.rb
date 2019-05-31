# frozen_string_literal: true

require "kafka/protocol/message"

require 'redis'

module Kafka

  # Buffers messages for specific topics/partitions.
  class MessageBuffer
    include Enumerable

    attr_reader :size, :bytesize

    def initialize(name)
      @redis = Redis.new(host: "127.0.0.1", port: 6379, db: 15)
      @namespace = "kafka_buffer_#{name}"
    end

    def size
      @redis.zrange(@namespace, 0, -1).inject(0) do |sum, topics_and_partitions|
        sum += @redis.llen("#{@namespace}_#{topics_and_partitions}")
      end
    end

    def bytesize
      bytesize = 0
      @redis.zrange(@namespace, 0, -1).each do |topics_and_partitions|
        @redis.lrange("#{@namespace}_#{topics_and_partitions}", 0, -1).each do |m|
          bytesize += Marshal::load(m).bytesize
        end
      end
      bytesize
    end

    def write(value:, key:, topic:, partition:, create_time: Time.now, headers: {})
      message = Protocol::Record.new(key: key, value: value, create_time: create_time, headers: headers)

      @redis.zadd(@namespace, 1, "#{topic}_#{partition}")
      @redis.rpush("#{@namespace}_#{topic}_#{partition}", Marshal::dump(message))
    end

    def concat(messages, topic:, partition:)
      @redis.zadd(@namespace, 1, "#{topic}_#{partition}")

      Array(messages).each do |message|
        @redis.rpush("#{@namespace}_#{topic}_#{partition}", Marshal::dump(message))
      end
    end

    def to_h
      h = {}
      @redis.zrange(@namespace, 0, -1).each do |topics_and_partitions|
        topic, partition = topics_and_partitions.split("_")
        h[topic] ||= {}
        h[topic][partition] = @redis.lrange("#{@namespace}_#{topic}_#{partitions}", 0, -1)
      end
      h
    end

    def empty?
      size == 0
    end

    def each
      @redis.zrange(@namespace, 0, -1).each do |topics_and_partitions|
        topic, partition = topics_and_partitions.split("_")

        while (message = @redis.lpop("#{@namespace}_#{topics_and_partitions}")) do
          yield topic, partition, Marshal::load(message)
        end
      end
    end

    # Clears buffered messages for the given topic and partition.
    #
    # @param topic [String] the name of the topic.
    # @param partition [Integer] the partition id.
    #
    # @return [nil]
    def clear_messages(topic:, partition:)
      @redis.zrem(@namespace, "#{topic}_#{partition}")
      @redis.del("#{@namespace}_#{topic}_#{partition}")
    end

    def messages_for(topic:, partition:)
      @redis.lrange("#{@namespace}_#{topic}_#{partition}", 0, -1).map do |m|
        Marshal::unload(m)
      end
    end

    # Clears messages across all topics and partitions.
    #
    # @return [nil]
    def clear
      @redis.zrange(@namespace, 0, -1).each do |topics_and_partitions|
        @redis.del("#{@namespace}_#{topics_and_partitions}")
      end
      @redis.del(@namespace)
    end

    private

    def buffer_for(topic, partition)
      @redis.lrange("#{@namespace}_#{topic}_#{partition}", 0, -1).map do |m|
        Marshal::load(m)
      end
    end
  end
end
