
create table Invictus_Fitness_Astana_2(EnterDate varchar(255),
ExitDate varchar(255), 
Club_name varchar(255), 
Club_shortname varchar(255), 
Club_symbol varchar(255), 
Club_number varchar(255), 
Club_email varchar(255), 
Club_Phone_Number varchar(255), 
Club_latitude varchar(255), 
Club_longitude varchar(255), 
Club_timeone varchar(255), 
Club_open_date varchar(255), 
Club_adress_line_1 varchar(255), 
Club_adress_line_2 varchar(255), 
Club_adress_city varchar(255), 
Club_adress_postalCode varchar(255),
Club_adress_country varchar(255),
Club_adress_country_symbol varchar(255),
Club_adress_stateSymbol varchar(255),
Club_type varchar(255), 
Club_isHidden varchar(255), 
Club_clubPhotoUrl varchar(255), 
Club_ID varchar(255),
Club_timestamp int,
Club_isDelated varchar(255),
UserId varchar(255),
id varchar(255),
Timestamp_ int,
isDeleted varchar(255))
select  DISTINCT * from Invictus_Fitness_Astana_1
select * from Invictus_Fitness_Astana_1

select * from Invictus_Fitness_Astana_2
group by EnterDate

select MAX (ExitDate) AS "Latest Date of EXIT", 
MIN (EnterDate) AS "Earliest Date of ENTER"
from Invictus_Fitness_Astana_2


where Timestamp_= '7408272'
delete from Invictus_Fitness_Astana_1

