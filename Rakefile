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

task :mv_beams do
  sh "mkdir -p ebin/xd"
  sh "mv ebin/combinator_server.beam ebin/xd/"
  sh "mv ebin/xing_directory_sup.beam ebin/xd/"
  sh "mv ebin/combinator_sup.beam ebin/xd/"
  sh "mv ebin/generators_sup.beam ebin/xd/"
  sh "mv ebin/generator_server.beam ebin/xd/"
  sh "mv ebin/generators.beam ebin/xd/"
  sh "mv ebin/xing_directory_app.beam ebin/xd/"
  sh "mv ebin/xio.beam ebin/xd/"
  sh "mv ebin/log.beam ebin/xd/"
  sh "mv ebin/file_logger_event_handler.beam ebin/xd/"
  sh "cp src/xd/xing_directory.app ebin/"
end
task :templates do
  sh "cd ebin; erl -pa ../contrib/erlyweb-0.7.2/ebin -pa ../contrib/scalaris/ebin -s view compileTemplates -s init stop; mv ../templates/*.beam ../ebin/"
end

task :copy_records do
 sh "cp src/*.hrl ebin/"
end

task :default => [:compile, :copy_records]
