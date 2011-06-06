REGISTER pig/pigrdf.jar;
SET default_parallel '125';
SET job.name 'parsen3pagelinks_$name';


RMF $page_links



n3                 = LOAD '$page_links_nt' USING pigrdf.RDFEncoder() AS (sub:long,pred:long,obj:long) ;


page_links  = FOREACH n3 GENERATE sub,obj;



STORE page_links  INTO '$page_links' USING BinStorage() ;




