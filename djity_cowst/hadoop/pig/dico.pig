REGISTER pig/pigrdf.jar;
SET default_parallel '125';
SET job.name 'dico_$name';

RMF $dico


subclassof       = LOAD '$subclassof_nt' USING pigrdf.RDFLoader() AS (sub:chararray,pred:chararray,obj:chararray) ;
templates       = LOAD '$templates_nt' USING pigrdf.RDFLoader() AS (sub:chararray,pred:chararray,obj:chararray) ;
properties       = LOAD '$infobox_properties_nt' USING pigrdf.RDFLoader() AS (sub:chararray,pred:chararray,obj:chararray) ;
instances = LOAD '$instances_tsv' USING PigStorage() AS (ins:long);

res1 = FOREACH properties  GENERATE $1;
res2 = FOREACH templates  GENERATE $2;
res3 = FOREACH subclassof  GENERATE $0;
res4 = FOREACH subclassof  GENERATE $2;


res = UNION res1,res2,res3,res4;

res = DISTINCT res;
dico1 = FOREACH res GENERATE pigrdf.MD5Encoder($0),$0;


ins = FOREACH properties GENERATE  $0;
ins = DISTINCT ins;

dico2 = FOREACH ins GENERATE pigrdf.MD5Encoder($0),$0;
dico2_selected  = JOIN dico2 BY $0 , instances BY $0;
dico2 = FOREACH dico2_selected GENERATE $0,$1;

dico = UNION dico1,dico2;

STORE dico   INTO '$dico' USING PigStorage() ;

