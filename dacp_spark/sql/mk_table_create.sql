use manke_dw;

create  external  table  t_bibi_anime_tfidf_cos_sim(
season_id  int ,
compare_season_id int,
cos_sim  double
) stored as parquet
location '/user/hive/warehouse/manke_dw.db/t_bibi_anime_tfidf_cos_sim/';



create  external  table  t_bibi_anime_cvidf_cos_sim(
season_id int,
compare_season_id  int,
cos_sim  double
) stored  as  parquet
location '/user/hive/warehouse/manke_dw.db/t_bibi_anime_cvidf_cos_sim/';


create   external   table   t_bibi_anime_w2v_lsh_sim(
season_id  int ,
compare_season_id int,
euclidean_distance  double
) stored  as   parquet
location '/user/hive/warehouse/manke_dw.db/t_bibi_anime_w2v_lsh_sim/';


create  table  bibi_long_comments(
review_id  int,
media_id  int ,
comment_detail  string
)
Partitioned by  (day String)
stored as  orc;



create  table  bibi_short_comments(
review_id  int,
media_id  int ,
comment_detail  string
)
Partitioned by  (day String)
stored as  orc;