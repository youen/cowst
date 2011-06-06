SET default_parallel '115';
SET job.name 'pagerank_$name';

RMF $pagerank_out
RMF $pagerank_avg_out

page_rank =  LOAD '$pagerank_in' USING PigStorage() AS (page:long, rank:float) ;
page_contribs = LOAD '$page_contribs'  USING BinStorage() AS (page:long, contrib:double);

pages_page_rank = JOIN page_contribs BY page, page_rank BY page;
contribs = FOREACH pages_page_rank GENERATE page_contribs::page, (double)page_contribs::contrib*(double)page_rank::rank AS contrib; 

edges = LOAD '$page_links' USING BinStorage() AS (from:long, to:long);
joined_divy_groups = JOIN edges BY from, contribs BY page_contribs::page;
page_rank_contributions = FOREACH joined_divy_groups GENERATE edges::to, contribs::contrib;

zero_contribs = LOAD '$zero_contribs' USING BinStorage() AS (page:long, contrib:double);
page_rank_contributions_with_zero = UNION page_rank_contributions, zero_contribs;
group_page_ranks = GROUP page_rank_contributions_with_zero BY edges::to;
next_page_rank = FOREACH group_page_ranks GENERATE group AS page, 0.15+(0.85*SUM(page_rank_contributions_with_zero.contribs::contrib)) AS rank; 


store next_page_rank into '$pagerank_out' USING PigStorage();

join_in_out = JOIN next_page_rank BY $0, page_rank by page;

diff = FOREACH join_in_out GENERATE page_rank::page, ((next_page_rank::rank>page_rank::rank)?(next_page_rank::rank - page_rank::rank):(page_rank::rank - next_page_rank::rank)) AS diff,1 as dummy_group;
diff_group = GROUP diff BY dummy_group;
avg = FOREACH diff_group GENERATE 'average :', AVG(diff.diff);

STORE avg INTO '$pagerank_avg_out' USING PigStorage();


