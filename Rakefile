require 'rake/clean'

INCLUDE = "contrib"

ERLC_FLAGS = "-I#{INCLUDE} +warn_unused_vars +warn_unused_import +debug_info "

SRC = FileList['src/**/*.erl']
OBJ = SRC.pathmap("%{ebin}X.beam")

CLEAN.include("ebin/*")

directory 'ebin'

rule ".beam" => ["%{ebin}X.erl"] do |t|
  sh "erlc -D EUNIT -pa ebin -W #{ERLC_FLAGS} -o ebin #{t.source}"
end
rule ".hrl" => ["%{ebin}X.hrl"] do |t|
  sh "cp #{t.source} ebin/"
end

task :clean do
  sh "rm -rf ebin/*.beam"
  sh "rm -rf ebin/*.hrl"
end

task :compile => ['ebin'] + OBJ

task :deps do
  sh "cd contrib/erlang-rfc4627/ && make"
  sh "cp contrib/erlang-rfc4627/ebin/*.beam ebin/"
end

task :copy do
  sh "cp src/*.hrl ebin/"
  sh "cp src/*.app ebin/"
end

task :default => [:compile, :deps, :copy]
