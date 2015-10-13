# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "socket"

# This output allows you to pull metrics from your logs and ship them to
# KairosDB. KairosDB is an open source tool for storing metrics.
#
# An example use case: Some applications emit aggregated stats in the logs
# every 10 seconds. Using the grok filter and this output, it is possible to
# capture the metric values from the logs and emit them to KairosDB.
class LogStash::Outputs::KairosDB < LogStash::Outputs::Base
  
  milestone 1 

  config_name "kairosdb"

  EXCLUDE_ALWAYS = [ "@timestamp", "@version" ]

  # The hostname or IP address of the KairosDB server.
  config :host, :validate => :string, :default => "localhost"

  # The port to connect to on the KairosDB server.
  config :port, :validate => :number, :default => 4242

  # Interval between reconnect attempts to Carbon.
  config :reconnect_interval, :validate => :number, :default => 2

  # Should metrics be resent on failure?
  config :resend_on_failure, :validate => :boolean, :default => false

  # The metric(s) to use. This supports dynamic strings like %{host}
  # for metric names and also for values. This is a hash field with key 
  # being the metric name, value being the metric value. Example:
  # [source,ruby]
  #     metrics => { "%{host}/uptime" => "%{uptime_1m}" }
  #
  # The value will be coerced to a floating point value. Values which cannot be
  # coerced will be set to zero (0). You may use either `metrics` or `fields_are_metrics`,
  # but not both.
  config :metrics, :validate => :hash, :default => {}

  # Are all included fields, apart from the excluded fields, individual metrics?
  config :fields_are_metrics, :validate => :boolean, :default => false

  # Include only regex matched metric names.
  config :include_metrics, :validate => :array, :default => [ ".*" ]

  # Exclude regex matched metric names, by default exclude unresolved %{field} strings.
  config :exclude_metrics, :validate => :array, :default => [ "%\{[^}]+\}" ]

  # Use this field for the timestamp instead of '@timestamp' which is the
  # default. Useful when backfilling or just getting more accurate data into
  # kairosdb since you probably have a cache layer infront of Logstash.
  config :timestamp_field, :validate => :string, :default => '@timestamp'

  # When hashes are passed in as values they are broken out into a dotted notation
  # For instance if you configure this plugin with
  # # [source,ruby]
  #     metrics => "mymetrics"
  #
  # and "mymetrics" is a nested hash of '{a => 1, b => { c => 2 }}'
  # this plugin will generate two metrics: a => 1, and b.c => 2 .
  # This config setting changes the separator from the '.' default.
  config :nested_object_separator, :validate => :string, :default => "."

  def register
    @include_metrics.collect!{|regexp| Regexp.new(regexp)}
    @exclude_metrics.collect!{|regexp| Regexp.new(regexp)}
    connect
  end # def register

  def connect
    # TODO(sissel): Test error cases. Catch exceptions. Find fortune and glory. Retire to yak farm.
    begin
      @socket = TCPSocket.new(@host, @port)
    rescue Errno::ECONNREFUSED => e
      @logger.warn("Connection refused to kairosdb server, sleeping...",
                   :host => @host, :port => @port)
      sleep(@reconnect_interval)
      retry
    end
  end # def connect

  public
  def receive(event)
    return unless output?(event)

	# Extract the metrics from the event
    messages = @fields_are_metrics ?
      messages_from_event_fields(event, @include_metrics, @exclude_metrics) :
      messages_from_event_metrics(event, @metrics)

	# Remove empty messages  
	messages = messages.compact
	
    if messages.empty?
      @logger.debug("Message is empty, not sending anything to KairosDB")
    else
      message = messages.join("\n")
      @logger.debug("Sending carbon messages", :messages => messages, :host => @host, :port => @port)

      # Catch exceptions like ECONNRESET and friends, reconnect on failure.
      begin
        @socket.puts(message)
      rescue Errno::EPIPE, Errno::ECONNRESET, IOError => e
        @logger.warn("Connection to kairosdb server died",
                     :exception => e, :host => @host, :port => @port)
        sleep(@reconnect_interval)
        connect
        retry if @resend_on_failure
      end
    end
  end # def receive

  private

  def messages_from_event_fields(event, include_metrics, exclude_metrics)
    timestamp = event_timestamp(event)
    @logger.debug? && @logger.debug("got metrics event", :metrics => event.to_hash)
    event.to_hash.flat_map do |metric,value|
      next if EXCLUDE_ALWAYS.include?(metric)
      next unless include_metrics.empty? || include_metrics.any? { |regexp| metric.match(regexp) }
      next if exclude_metrics.any? {|regexp| metric.match(regexp)}

      metrics_lines_for_event(event, metric, value, timestamp)
    end
  end

  def messages_from_event_metrics(event, metrics)
    timestamp = event_timestamp(event)
    metrics.flat_map do |metric, value|
      @logger.debug("processing", :metric => metric, :value => value)
      metric = event.sprintf(metric)
      next unless @include_metrics.any? {|regexp| metric.match(regexp)}
      next if @exclude_metrics.any? {|regexp| metric.match(regexp)}

      metrics_lines_for_event(event, metric, value, timestamp)
    end
  end

  def event_timestamp(event)
    event[@timestamp_field].to_i
  end

  def metrics_lines_for_event(event, metric, value, timestamp)
    if event[metric].is_a?(Hash)
      dotify(event[metric], metric).map do |k,v|
        metrics_line(k, v, timestamp)
      end
    else
      metrics_line(event.sprintf(metric), event.sprintf(value).to_f, timestamp)
    end
  end

  # KairosDB message format: put <metric> <timestamp> <value>
  def metrics_line(name, value, timestamp)
    "put #{name} #{timestamp} #{value}"
  end

  # Take a nested ruby hash of the form {:a => {:b => 2}, c: => 3} and
  # turn it into a hash of the form
  # { "a.b" => 2, "c" => 3}
  def dotify(hash,prefix=nil)
    hash.reduce({}) do |acc,kv|
      k,v = kv
      pk = prefix ? "#{prefix}#{@nested_object_separator}#{k}" : k.to_s
      if v.is_a?(Hash)
        acc.merge!(dotify(v, pk))
      elsif v.is_a?(Array)
        # There's no right answer here, so we do nothing
        @logger.warn("Array values not supported for kairosdb metrics! Ignoring #{hash} @ #{prefix}")
      else
        acc[pk] = v
      end
      acc
    end
  end

end # class LogStash::Outputs::KairosDB
