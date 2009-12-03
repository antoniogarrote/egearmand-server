require 'rake/clean'

INCLUDE = "contrib"

#ERLC_FLAGS = "-I#{INCLUDE} +warn_unused_vars +warn_unused_import +debug_info "
ERLC_FLAGS = "-I#{INCLUDE} +warn_unused_vars +warn_unused_import "

SRC = FileList['src/*.erl']
OBJ = SRC.pathmap("%{ebin}X.beam")

EXT_SRC = FileList['src/extensions/**/*.erl']
EXT_OBJ = EXT_SRC.pathmap("%{ebin}X.beam")

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

task :extensions_compile => ['ebin'] + EXT_OBJ

task :deps do
  sh "cd contrib/erlang-rfc4627/ && make"
  sh "cp contrib/erlang-rfc4627/ebin/*.beam ebin/"
end

task :copy do
  sh "cp src/*.hrl ebin/"
  sh "cp src/*.app ebin/"
end

task :extensions_copy do
  begin
    sh "cp src/extensions/*.hrl ebin/"
  rescue
    puts "No records in extensions to copy"
  end
end

task :prepare_records do
  sh "cp src/states.hrl src/extensions/states.hrl"
end

task :clean_records do
  sh "rm src/extensions/states.hrl"
end

task :extensions => [:prepare_records, :extensions_compile, :clean_records, :deps, :extensions_copy]

task :default => [:compile, :copy]
