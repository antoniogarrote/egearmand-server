# example extracted from xing-gearman-ruby gem: git://github.com/xing/gearman-ruby.git
require 'rubygems'
require 'gearman'

Gearman::Util.debug = true

servers = ['localhost:4730']
worker = Gearman::Worker.new(servers)

worker.add_ability('chunked_transfer') do |data, job|
  5.times do |i|
    sleep 1
    job.send_data("CHUNK #{i}")
  end
  "EOD"
#  worker.remove_ability("chunked_transfer")
end
loop { worker.work }
