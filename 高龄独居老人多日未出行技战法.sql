--筛选出前四天中的数据,结果是70岁以上,每天的数据按rn字段降序, person_number字段表示几人合住(是否独居)
drop table if exists tmp_iot.tmp_solitary_elderly;
create table tmp_iot.tmp_solitary_elderly as 
select c.id_card,c.trait_img, c.abs_time,c.abs_timestamp,c.place_id,c.place_name,c.monitor_direction,c.device_name,c.residence_address,c.people_name,c.gender,c.age,c.idcard_pic,c.confidence,c.rn,count(1) as person_number,c.dt_date 
   from (select b1.id_card,b1.trait_img, b1.abs_time,b1.abs_timestamp,b1.place_id,b1.place_name,b1.monitor_direction,b1.device_name,b1.residence_address,b1.people_name,b1.gender,b1.age,b1.idcard_pic,b1.confidence,b1.rn,b1.dt_date
               from (select a1.id_card,a1.trait_img, a1.abs_time,a1.abs_timestamp,a1.place_id,a1.place_name,a1.monitor_direction,a1.device_name,a1.confidence,a2.residence_address,a2.people_name,a2.gender,cast(datediff('2019-09-17',a2.birth_date)/365.25 as int) age,a2.idcard_pic,row_number() over(partition by a1.id_card,a1.dt_date order by a1.abs_time desc) as rn,a1.dt_date
                            from (select a.id_card, a.trait_img, a.abs_time,a.abs_timestamp,a.place_id,a.place_name,a.monitor_direction,a.device_name,a.confidence,a.dt_date
                                      from dw_iot.dw_face_data a
                                      where a.dt_date>=date_sub('2019-09-17',3) and a.dt_date<='2019-09-17' and a.id_card is not null) a1
                            left join dw_iot.dw_people a2 
                            on a2.dt_date='2019-09-17' and a1.id_card=a2.credential_no 
                            where datediff('2019-09-17',birth_date)/365.25>=70) b1
         left join dw_iot.dw_people b2 
         on b2.dt_date='2019-09-17' and b1.residence_address=b2.residence_address) c 
   group by c.id_card,c.trait_img, c.abs_time,c.abs_timestamp,c.place_id,c.place_name,c.monitor_direction,c.device_name,c.residence_address,c.people_name,c.gender,c.age,c.idcard_pic,c.confidence,c.rn,c.dt_date;
   
   
--取出第一天每人的最后一条记录,并且在其后三天没有记录的独居老人数据,告警
insert overwrite table dm_people.dm_solitary_elderly partition (dt_date='2019-09-17')
select 29,uuid(),unix_timestamp()*1000,from_unixtime(unix_timestamp()+60*60*8),a1.id_card,a1.trait_img, a1.abs_time,a1.abs_timestamp,a1.place_id,a1.place_name,a1.monitor_direction,a1.device_name,a1.residence_address,a1.people_name,a1.gender,a1.age,a1.idcard_pic,a1.confidence,''
     from      (select c.id_card,c.trait_img, c.abs_time,c.abs_timestamp,c.place_id,c.place_name,c.monitor_direction,c.device_name,c.residence_address,c.people_name,c.gender,c.age,c.idcard_pic,c.confidence,c.rn,c.person_number,c.dt_date from tmp_iot.tmp_solitary_elderly c where c.dt_date=date_sub('2019-09-17',3) and c.rn=1 and c.person_number=1 ) a1
     left join (select c.id_card,c.trait_img, c.abs_time,c.abs_timestamp,c.place_id,c.place_name,c.monitor_direction,c.device_name,c.residence_address,c.people_name,c.gender,c.age,c.idcard_pic,c.confidence,c.rn,c.person_number,c.dt_date from tmp_iot.tmp_solitary_elderly c where c.dt_date>=date_sub('2019-09-17',2) and c.dt_date<='2019-09-17') a2
     on a1.id_card = a2.id_card  and a1.abs_time >= a2.abs_time - 72 * 60 * 60 * 1000
     where a2.id_card is null;