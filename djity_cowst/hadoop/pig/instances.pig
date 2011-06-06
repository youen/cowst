SET job.name 'instances_$name';
set default_parallel '115';
RMF $instances_tsv



iplh = LOAD '$infobox_properties_local_histogram' USING PigStorage() AS (local:long,prop:long,rang:long,count:long,ins:long);
iph = LOAD '$infobox_properties_histogram' USING PigStorage() AS (prop:long,rang:long,count:long,ins:long);
pagerank            = LOAD '$pagerank_out' USING BinStorage() AS (ins:long,rank:double) ;


ins1 = FOREACH iplh GENERATE ins;
ins2 = FOREACH iph GENERATE  ins;
ins = UNION ins1,ins2;
ins = DISTINCT ins;

select_pagerank = JOIN pagerank BY ins, ins by $0;

instances = FOREACH select_pagerank GENERATE $0 , $1;

STORE instances INTO '$instances_tsv' USING PigStorage();


