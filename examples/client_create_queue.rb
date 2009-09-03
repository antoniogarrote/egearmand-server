require 'rubygems'
require 'gearman'
require 'json'

Gearman::Util.debug = true

servers = ['localhost:4730']

client = Gearman::Client.new(servers)
taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new("/egearmand/rabbitmq/declare", { :name => "test_queue", 
                                                          :routing_key => "test_queue" }.to_json)

task.on_data {|d| puts d }
task.on_complete {|d| puts "OK created" }

taskset.add_task(task)
taskset.wait(3000)
