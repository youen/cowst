SET default_parallel '115';
SET job.name 'pre_pagerank_$name';


RMF $page_contribs
RMF $zero_contribs
RMF $pagerank_in

edges = load '$page_links' USING BinStorage() as (from:long, to:long);
pages = group edges by from;
page_contribs = foreach pages generate group, 1.0 / (double)SIZE(edges) as contrib;
store page_contribs into '$page_contribs' USING BinStorage();
zero_contribs = foreach pages generate group, (double)0 as contrib;
store zero_contribs into '$zero_contribs' USING BinStorage();

pr_from  = FOREACH edges GENERATE from, 1.0;
pr_to  = FOREACH edges GENERATE to, 1.0;
pr = UNION pr_from, pr_to;
pr = DISTINCT pr ;

STORE pr INTO '$pagerank_in' USING PigStorage();

