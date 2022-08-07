create table if not exists t_user(userid int,sex string,age string,occupation string,zipcode string)
row format SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");
create table if not exists t_rating(userid int,movieid bigint,rate int,times bigint)
row format SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");
create table if not exists t_movie(movieid bigint,moviename string,movietype string)
row format SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");

load data local inpath '/hadoop-data/users.dat' overwrite into table t_user;
load data local inpath '/hadoop-data/ratings.dat' overwrite into table t_rating;
load data local inpath '/hadoop-data/movies.dat' overwrite into table t_movie;

select t_user.age as age, avg(t_rating.rate) as avgrate from t_user left join t_rating on (t_user.userid = t_rating.userid) where t_rating.movieid=2116 group by t_user.age;