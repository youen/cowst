REGISTER pig/pigrdf.jar;
SET default_parallel '125';
SET job.name 'parsen3_$name';


RMF $infobox_properties_md5
RMF $infobox_properties_list


n3                 = LOAD '$infobox_properties_nt' USING pigrdf.RDFEncoder() AS (sub:long,pred:long,obj:long) ;


STORE n3   INTO '$infobox_properties_md5' USING BinStorage() ;


list  = FOREACH n3 GENERATE  $1;
list = DISTINCT list;

STORE list   INTO '$infobox_properties_list' USING PigStorage() ;

