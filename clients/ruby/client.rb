require 'net/http'
require 'json'
require 'thread'

pubsub_URL = 'http://pubsub.example.org:2069/'

def do_stuff_with(event)
  print("Got a pubsub event!:\n")
  print(event, "\n")
end

def listen(url)
  ps_thread = Thread.new do
    begin
      uri = URI.parse(url)
      Net::HTTP.start(uri.host, uri.port, :use_ssl => url.match(/^https:/) ? true : false) do |http|
        request = Net::HTTP::Get.new uri.request_uri
        http.request request do |response|
          body = ''
          response.read_body do |chunk|
            body += chunk
            if chunk.end_with? "\n"
              event = JSON.parse(body.chomp)
              body = ''
              if event['stillalive']  # pingback
                print("ping? PONG!\n")
              else
                do_stuff_with(event)
              end
            end
          end
        end
      end
    rescue Errno::ECONNREFUSED => e
      restartable = true
      STDERR.puts e
      sleep 3
    rescue Exception => e
      STDERR.puts e
      STDERR.puts e.backtrace
    end
  end
  return ps_thread
end

begin
  ps_thread = listen(pubsub_URL)
  print("Pubsub thread started, waiting for results...\n")
  while ps_thread.alive?
    sleep 10
  end
end
