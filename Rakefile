require 'rake/clean'

INCLUDE = "contrib"

ERLC_FLAGS = "-I#{INCLUDE} +warn_unused_vars +warn_unused_import"

SRC = FileList['src/**/*.erl']
OBJ = SRC.pathmap("%{ebin}X.beam")

CLEAN.include("ebin/*.beam")

directory 'ebin'

rule ".beam" => ["%{ebin}X.erl"] do |t|
  sh "erlc +debug_info -D EUNIT -pa ebin -W #{ERLC_FLAGS} -o ebin #{t.source}"
end
rule ".hrl" => ["%{ebin}X.hrl"] do |t|
  sh "cp #{t.source} ebin/"
end

task :clean do
  sh "rm -rf ebin/*.beam"
  sh "rm -rf ebin/*.hrl"
end

task :compile => ['ebin'] + OBJ

task :copy_records do
 sh "cp src/*.hrl ebin/"
end

task :default => [:compile, :copy_records]
