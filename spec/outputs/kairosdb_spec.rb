require_relative '../spec_helper'

describe LogStash::Outputs::KairosDB do

  let(:port) { 4939 }
  let(:server) { subject.socket }

  before :each do
    subject.register
    subject.receive(event)
  end

  context "with a default run" do

    subject { LogStash::Outputs::KairosDB.new("host" => "localhost", "port" => port, "metrics" => [ "hurray.%{foo}", "%{bar}" ]) }
    let(:event) { LogStash::Event.new("foo" => "fancy", "bar" => 42) }

    it "generate one element" do
      expect(server.size).to eq(1)
    end

    it "include all metrics" do
      line = server.pop
      expect(line).to match(/^put hurray.fancy \d{10,} 42.0\n$/)
    end
  end

  context "if fields_are_metrics => true" do
    context "match all keys" do

      subject { LogStash::Outputs::KairosDB.new("host" => "localhost",
                                                      "port" => port,
                                                      "fields_are_metrics" => true,
                                                      "include_metrics" => [".*"]) }

      let(:event) { LogStash::Event.new("foo" => "123", "bar" => "42") }
	  it "should create the proper formatted lines" do
		lines = [server.pop, server.pop].sort 
		expect(lines[0]).to match(/^put bar \d{10,} 42.0\n$/)
		expect(lines[1]).to match(/^put foo \d{10,} 123.0\n$/)
	  end
    end

    context "no match" do

      subject { LogStash::Outputs::KairosDB.new("host" => "localhost",
                                                      "port" => port,
                                                      "fields_are_metrics" => true,
                                                      "include_metrics" => ["notmatchinganything"]) }

      let(:event) { LogStash::Event.new("foo" => "123", "bar" => "42") }

      it "generate no event" do
        expect(server.empty?).to eq(true)
      end
    end

    context "match a key with invalid metric_format" do

      subject { LogStash::Outputs::KairosDB.new("host" => "localhost",
                                                      "port" => port,
                                                      "fields_are_metrics" => true,
                                                      "include_metrics" => ["foo"]) }

      let(:event) { LogStash::Event.new("foo" => "123") }

      it "match the foo key" do
        line = server.pop
        expect(line).to match(/^put foo \d{10,} 123.0\n$/)
      end
    end
  end

  context "fields are metrics = false" do
    context "metrics_format not set" do
      context "match one key with metrics list" do

        subject { LogStash::Outputs::KairosDB.new("host" => "localhost",
                                                        "port" => port,
                                                        "fields_are_metrics" => false,
                                                        "include_metrics" => ["foo"],
                                                        "metrics" => [ "custom.foo", "%{foo}" ]) }

        let(:event) { LogStash::Event.new("foo" => "123") }

        it "match the custom.foo key" do
          line = server.pop
          expect(line).to match(/^put custom.foo \d{10,} 123.0\n$/)
        end

        context "when matching a nested hash" do
          let(:event) { LogStash::Event.new("custom.foo" => {"a" => 3, "c" => {"d" => 2}}) }

          it "should create the proper formatted lines" do
            lines = [server.pop, server.pop].sort # Put key 'a' first
            expect(lines[0]).to match(/^put custom.foo.a \d{10,} 3\n$/)
            expect(lines[1]).to match(/^put custom.foo.c.d \d{10,} 2\n$/)
          end
        end
      end
    end
  end

  context "timestamp_field used is timestamp_new" do

    let(:timestamp_new) { (Time.now + 3).to_i }

    subject { LogStash::Outputs::KairosDB.new("host" => "localhost",
                                                    "port" => port,
                                                    "timestamp_field" => "timestamp_new",
                                                    "metrics" => ["foo", "1"]) }

    let(:event) { LogStash::Event.new("foo" => "123", "timestamp_new" => timestamp_new) }

    it "timestamp matches timestamp_new" do
      line = server.pop
      expect(line).to match(/^put foo #{timestamp_new} 1.0\n$/)
    end
  end

  describe "dotifying a hash" do
    let(:event) { LogStash::Event.new( "metrics" => hash) }
    let(:dotified) { LogStash::Outputs::KairosDB.new().send(:dotify, hash) }

    context "with a complex hash" do
      let(:hash) { {:a => 2, :b => {:c => 3, :d => 4, :e => {:f => 5}}} }

      it "should dottify correctly" do
        expect(dotified).to eql({"a" => 2, "b.c" => 3, "b.d" => 4, "b.e.f" => 5})
      end
    end

    context "with a simple hash" do
      let(:hash) { {:a => 2, 5 => 4} }

      it "should do nothing more than stringify the keys" do
        expect(dotified).to eql("a" => 2, "5" => 4)
      end
    end

    context "with an array value" do
      let(:hash) { {:a => 2, 5 => 4, :c => [1,2,3]} }

      it "should ignore array values" do
        expect(dotified).to eql("a" => 2, "5" => 4)
      end
    end
  end
end
