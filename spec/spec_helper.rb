require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/kairosdb"
require_relative "support/server"

class LogStash::Outputs::KairosDB
  attr_reader :socket

  def connect
    @socket = Mocks::Server.new
  end
end
