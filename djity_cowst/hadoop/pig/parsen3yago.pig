REGISTER pig/pigrdf.jar;
SET default_parallel '125';
SET job.name 'parsen3yago_$name';


RMF $yago_subclassof_md5
RMF $yago_subclassof_tsv
RMF $yago_types_md5
RMF $yago_size



n3                 = LOAD '$yago_links_nt' USING pigrdf.RDFEncoder() AS (sub:long,pred:long,obj:long) ;

SPLIT n3 INTO 
                  -- rdf:type
	n3_type   IF ((pred==2488141658018628857L) OR (pred==-8123476989456627667L)),
	              -- rdfs:subClassOf
	n3_subclassof IF ((pred==-8742859611446415633L) OR (pred==6455191263346059777L));


n3_subclassof = FOREACH n3_subclassof GENERATE sub,obj;

STORE n3_subclassof   INTO '$yago_subclassof_md5' USING BinStorage() ;
STORE n3_subclassof   INTO '$yago_subclassof_tsv' USING PigStorage() ;

n3_type = FOREACH n3_type GENERATE sub, obj;

STORE n3_type   INTO '$yago_types_md5' USING BinStorage() ;

type_group = GROUP n3_type BY obj;
class_size = FOREACH type_group GENERATE group,COUNT(n3_type);

STORE class_size  INTO '$yago_size' USING BinStorage() ;




