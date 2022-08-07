create table if not exists t_user(userid int,sex string,age string,occupation string,zipcode string)
row format SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");
create table if not exists t_rating(userid int,movieid bigint,rate int,times bigint)
row format SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");
create table if not exists t_movie(movieid bigint,moviename string,movietype string)
row format SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");

load data local inpath '/hadoop-data/users.dat' overwrite into table t_user;
load data local inpath '/hadoop-data/ratings.dat' overwrite into table t_rating;
load data local inpath '/hadoop-data/movies.dat' overwrite into table t_movie;

select sex, name, avg as avgrate, total from (select t_user.sex as sex, t_movie.moviename as name, avg(t_rating.rate) as avg, count(t_user.userid) as total from t_user left join t_rating on (t_user.userid = t_rating.userid) left join t_movie on (t_rating.movieid = t_movie.movieid) where t_user.sex='M' group by t_movie.moviename, t_user.sex) t1 where total>50 order by avg desc limit 10;