with client_user as (select * from users where users.username = :username),
     pc1_data_agg as (select case
                                 when individual.created_date_time between :from_date::date and :to_date::date  then 'Yes'
                                 when individual.last_modified_date_time between :from_date::date and :to_date::date is not null then 'Yes'
                                 else 'No' end as activity
                      from sakhiapp.individual
                               inner join  client_user on client_user.id = individual.last_modified_by_id
                      where 1=1

                      union all

                      select case
                                 when household.created_date_time between :from_date::date and :to_date::date  then 'Yes'
                                 when household.last_modified_date_time between :from_date::date and :to_date::date is not null then 'Yes'
                                 else 'No' end as activity
                      from sakhiapp.household
                               inner join  client_user on client_user.id = household.last_modified_by_id
                      where 1=1

                      union all

                      select case
                                 when individual_pregnancy.created_date_time between :from_date::date and :to_date::date  then 'Yes'
                                 when individual_pregnancy.last_modified_date_time between :from_date::date and :to_date::date is not null then 'Yes'
                                 else 'No' end as activity
                      from sakhiapp.individual_pregnancy
                               inner join  client_user on client_user.id = individual_pregnancy.last_modified_by_id
                      where 1=1

                      union all

                      select case
                                 when individual_child.created_date_time between :from_date::date and :to_date::date  then 'Yes'
                                 when individual_child.last_modified_date_time between :from_date::date and :to_date::date is not null then 'Yes'
                                 else 'No' end as activity
                      from sakhiapp.individual_child
                               inner join  client_user on client_user.id = individual_child.last_modified_by_id
                      where 1=1
     ),

     pc1_data as (
         select count(*)
                    as total from pc1_data_agg where activity = 'Yes'
     ),
     
     hbnc_data as (select individual.id                                                          as indID,
                         enc.encounter_date_time::date,
                         enc."Place of delivery" as place_of_delivery,
                         enc."Delivery type" as delivery_type,
                         row_number()
                         over (partition by enc.individual_id order by enc.encounter_date_time asc) as visit_number
     from sakhiapp.individual
     left join sakhiapp.individual_child enl on enl.individual_id = individual.id and enl.is_voided = false 
     left join sakhiapp.individual_child_hbnc enc on enc.program_enrolment_id = enl.id and enc.is_voided = false 
     inner join client_user u1 on u1.id = enc.last_modified_by_id
     inner join client_user u2 on u2.id = enc.created_by_id
     where individual.is_voided = false 
     ),
     
     hbyc_data as (select individual.id                                                          as indID,
                         enc.encounter_date_time::date,
                         row_number()
                         over (partition by enc.individual_id order by enc.encounter_date_time asc) as visit_number
     from sakhiapp.individual
     left join sakhiapp.individual_child enl on enl.individual_id = individual.id and enl.is_voided = false 
     left join sakhiapp.individual_child_hbyc enc on enc.program_enrolment_id = enl.id and enc.is_voided = false 
     inner join client_user u1 on u1.id = enc.last_modified_by_id
     inner join client_user u2 on u2.id = enc.created_by_id
     where individual.is_voided = false 
     ),
     
     enc_data as (select individual.id                                                          as indID,
                         enc.encounter_date_time::date,
                         enc."Whether ANC done under PMSMA",
                         enc."180 Iron Folic Acid Tablet issued or not?",
                         enc."TD 1 Dose"::date,
                         enc."TD 2 Dose"::date,
                         enc."TD Booster Dose"::date,
                         individual."Date of Sterilization"         date_of_sterilisation,
                         individual."gender"                        gender,
                         abor."Date of abortion"                    date_of_abortion,
                         abor."Date of Discharge (Abortion)"        date_of_discharge,
                         row_number()
                         over (partition by enc.individual_id order by enc.encounter_date_time asc) as visit_number

                  from sakhiapp.individual
                           left join sakhiapp.individual_pregnancy pe on individual.id = pe.individual_id  and pe.is_voided = false
                           left join sakhiapp.individual_pregnancy_anc enc on enc.program_enrolment_id = pe.id and enc.is_voided = false and encounter_date_time is not null
                           left join sakhiapp.individual_pregnancy_abortion abor on abor.individual_id = individual.id
                           inner join client_user u1 on u1.id = enc.created_by_id 
                           inner join client_user u2 on u2.id = enc.last_modified_by_id
                  where individual.is_voided = false 
                    
     ),

     first_visit_data as (select distinct indID,
                                          encounter_date_time as date_of_1st_ANC
                          from enc_data
                          where visit_number = 1),
     second_visit_data as (select distinct indID,
                                           encounter_date_time as date_of_2nd_ANC,
                                           "180 Iron Folic Acid Tablet issued or not?"
                           from enc_data
                           where visit_number = 2 ),

     fourth_visit_data as (select distinct indID,
                                           encounter_date_time as date_of_4th_ANC
                           from enc_data
                           where visit_number = 4
                             and  encounter_date_time between :from_date::date and :to_date::date),

     seventh_visit_data as (select distinct indID,
                                            encounter_date_time as date_of_7th_ANC
                            from enc_data
                            where visit_number = 7
                              and  encounter_date_time between :from_date::date and :to_date::date),
                              
     eighth_visit_data as (select distinct indID,
                                            encounter_date_time as date_of_8th_ANC
                            from enc_data
                            where visit_number = 8
                              and  encounter_date_time between :from_date::date and :to_date::date),

     first_hbnc_data as (select distinct indID,
                         encounter_date_time as date_of_1st_hbnc
                         from hbnc_data 
                         where visit_number = 1
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     second_hbnc_data as (select distinct indID,
                         encounter_date_time as date_of_2nd_hbnc
                         from hbnc_data 
                         where visit_number = 2
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     third_hbnc_data as (select distinct indID,
                         encounter_date_time as date_of_3rd_hbnc
                         from hbnc_data 
                         where visit_number = 3
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     fourth_hbnc_data as (select distinct indID,
                         encounter_date_time as date_of_4th_hbnc
                         from hbnc_data 
                         where visit_number = 4
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     fifth_hbnc_data as (select distinct indID,
                         encounter_date_time as date_of_5th_hbnc
                         from hbnc_data 
                         where visit_number = 5
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     sixth_hbnc_data as (select distinct indID,
                         encounter_date_time as date_of_6th_hbnc,
                         place_of_delivery,
                         delivery_type
                         from hbnc_data 
                         where visit_number = 6
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     seventh_hbnc_data as (select distinct indID,
                         encounter_date_time as date_of_7th_hbnc,
                         place_of_delivery,
                         delivery_type
                         from hbnc_data 
                         where visit_number = 7
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     first_hbyc_data as (select distinct indID,
                         encounter_date_time as date_of_1st_hbyc
                         from hbyc_data 
                         where visit_number = 1
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     fifth_hbyc_data as (select distinct indID,
                         encounter_date_time as date_of_5th_hbyc
                         from hbyc_data 
                         where visit_number = 5
                         and encounter_date_time between :from_date::date and :to_date::date
     ),
     
     mother_child_data as (
     select individual_a_id as mother_id ,
     individual_b_id as child_id
     from individual_relationship 
        inner join individual_relationship_type irt on irt.id = individual_relationship.relationship_type_id
        inner join sakhiapp.individual ind1 on ind1.id = individual_a_id and ind1.is_voided is false 
        inner join sakhiapp.individual ind2 on ind2.id = individual_b_id and ind2.is_voided is false 
     where relationship_type_id in (664,673)
     ),
     
cte_for_c34 as (
select individual.id                                        ind_id,
max(checklist_item.completion_date)                      as last_completion_date,
min(checklist_item.completion_date)                      as first_completion_date,
count(*)                                                    all_vaccines_done,
extract(year from age(current_date, individual.date_of_birth)) AS age_in_years
from checklist_item  
inner join checklist_item_detail on checklist_item_detail.id  = checklist_item.checklist_item_detail_id
inner join concept on checklist_item_detail.concept_id = concept.id
inner join checklist on checklist_item.checklist_id = checklist.id
inner join sakhiapp.individual_child program_enrolment on checklist.program_enrolment_id = program_enrolment.id
inner join sakhiapp.individual on program_enrolment.individual_id = individual.id
inner join client_user on (individual.created_by_id = client_user.id or individual.last_modified_by_id = client_user.id)
where completion_date is not null 
and concept.name in ('BCG','Pentavalent 3','OPV 3','Measles 1','JE 1')
and individual.is_voided is false 
and program_enrolment.program_exit_date_time is null
group by 1,5),

cte_for_c35 as (
select individual.id                                                    ind_id,
max(checklist_item.completion_date)                                  as last_completion_date,
min(checklist_item.completion_date)                                  as first_completion_date,
count(*)                                                                all_vaccines_done,
extract(year from age(current_date, individual.date_of_birth))       AS age_in_years
from checklist_item  
inner join checklist_item_detail on checklist_item_detail.id  = checklist_item.checklist_item_detail_id
inner join concept on checklist_item_detail.concept_id = concept.id
inner join checklist on checklist_item.checklist_id = checklist.id
inner join sakhiapp.individual_child program_enrolment on checklist.program_enrolment_id = program_enrolment.id
inner join sakhiapp.individual on program_enrolment.individual_id = individual.id
inner join client_user on (client_user.id = individual.created_by_id or client_user.id = individual.last_modified_by_id)
where completion_date is not null 
and concept.name in ('DPT Booster 1','DPT Booster 2','OPV Booster','Measles 2','JE 2')
and individual.is_voided is false 
and program_enrolment.program_exit_date_time is null
group by 1,5),
     
 delivery_abortion_fpFollowup_data as (
  select 
  i.id          ind_id,
  fpf."Date of sterilisation for family planning method"    date_of_sterlization,
  del."Date of delivery"                                    date_of_delivery,
  fpenl.enrolment_date_time                                 fp_enl_date_time,
  fpf."Date of Copper T"                                    date_of_copperT,
  del.encounter_date_time                                   delivery_encounter_date_time,
  fpf.encounter_date_time                                   fpf_encounter_date_time,
  i.created_by_id                                           created_by_id,
  i.last_modified_by_id                                     last_modified_by_id
  from sakhiapp.individual i 
  inner join sakhiapp.individual_family_planning fpenl on fpenl.individual_id = i.id
  inner join sakhiapp.individual_pregnancy enl on enl.individual_id = i.id
  inner join sakhiapp.individual_pregnancy_delivery del on del.individual_id = i.id
  inner join sakhiapp.individual_family_planning_fp_followup fpf on fpf.individual_id = i.id   
  where i.is_voided is false
  and enl.program_exit_date_time is null 
  and fpenl.program_exit_date_time is null
  and del.cancel_date_time is null 
),

     anc_done_data as (select distinct indID,
                                       encounter_date_time as date_of_ANC
                       from enc_data
                       where "Whether ANC done under PMSMA" = 'Yes'
                         and  encounter_date_time between :from_date::date and :to_date::date),

     first_TD_data as (select distinct indID,
                                       "TD 1 Dose" as date_of_1st_TD
                       from enc_data
                       where "TD 1 Dose" is not null),

     second_TD_data as (select distinct indID,
                                        "TD 2 Dose" as date_of_2nd_TD
                        from enc_data
                        where "TD 2 Dose" is not null
                          and "TD 1 Dose" is not null
                          and  encounter_date_time between :from_date::date and :to_date::date),

     td_booster_data as (select distinct indID,
                                         "TD Booster Dose" as date_of_TD_booster
                         from enc_data
                         where "TD Booster Dose" is not null
                           and  encounter_date_time between :from_date::date and :to_date::date),
                           
	 tuberculosis_data as (
							select i.id  				ind_id,
							i.created_by_id  			created_by_id,
							i.last_modified_by_id 		last_modified_by_id,
							enc."TB case status date", 
							enc."TB case status" ,
							enc."Type of TB",
							enc.encounter_date_time		encounter_date_time,
							enc."Treatment completion date" 		as treatment_completed_date,
							row_number() over (partition by enc.individual_id order by enc.encounter_date_time desc) as visit_number
							from sakhiapp.individual i
							inner join sakhiapp.individual_tuberculosis enl on enl.individual_id = i.id 
							inner join sakhiapp.individual_tuberculosis_tuberculosis_follow_up enc on enc.individual_id = i.id
),
	
	tb_confirmed_data as (
	select 
	enc.ind_id,
	enc.encounter_date_time,
	enc."TB case status date"           as confirmed_date
	from tuberculosis_data enc
	where enc."TB case status" = 'Confirmed'
	),
	
	tb_completed_data as (
	select 
	enc.ind_id,
	treatment_completed_date
	from tuberculosis_data enc
	where enc."TB case status" = 'Treatment completed'
	and treatment_completed_date::date between :from_date::date and :to_date::date
	),

	cte_for_18_14 as (
							select td_data.ind_id  																					as	ind_id,
							td_data.created_by_id  																					as	created_by_id,
							td_data.last_modified_by_id 																			as	last_modified_by_id,
							max(td_data.encounter_date_time) filter (where visit_number = 1 ) 										as  latest_encounter_date_time,
							td_data."Type of TB",
							td_data.encounter_date_time																				as  encounter_date_time
							from  tuberculosis_data td_data
							where td_data.encounter_date_time is not null 
							group by 1,2,3,5,6
	)
	

select 'C1.1'                                                           "Code",
       100                                                          as "Rate",
       count(distinct fourth_visit_data.indID)                      as "Beneficiary count",
       (count(distinct fourth_visit_data.indID) * 100)::text        as "Total claim amount",
       fourth_visit_data.date_of_4th_ANC::text                      as "Work completion date",
       first_visit_data.date_of_1st_ANC::text                       as "Date of registration"
from fourth_visit_data
         left join first_visit_data on fourth_visit_data.indID = first_visit_data.indID
group by fourth_visit_data.date_of_4th_ANC,
         first_visit_data.date_of_1st_ANC

union all

select 'C1.3'                                                           "Code",
       100                                                          as "Rate",
       count(distinct fourth_visit_data.indID)                      as "Beneficiary count",
       (count(distinct fourth_visit_data.indID) * 100)::text        as "Total claim amount",
       fourth_visit_data.date_of_4th_ANC::text                      as "Work completion date",
       second_visit_data.date_of_2nd_ANC::text                      as "Date of registration"
from fourth_visit_data
         left join second_visit_data on second_visit_data.indID = fourth_visit_data.indID
where "180 Iron Folic Acid Tablet issued or not?" = 'Yes'
group by fourth_visit_data.date_of_4th_ANC,
         second_visit_data.date_of_2nd_ANC

union all

select 'C1.5'                                                    as "Code",
       100                                                       as "Rate",
       count(distinct second_TD_data.indID)                      as "Beneficiary count",
       (count(distinct second_TD_data.indID) * 100) ::text       as "Total claim amount",
       second_TD_data.date_of_2nd_TD::text                       as "Work completion date",
       first_TD_data.date_of_1st_TD::text                        as "Date of registration"


from second_TD_data
         left join first_TD_data on second_TD_data.indID = first_TD_data.indID
group by second_TD_data.date_of_2nd_TD,
         first_TD_data.date_of_1st_TD

union all

select 'C1.5'                                                    as "Code",
       100                                                       as "Rate",
       count(distinct td_booster_data.indID)                     as "Beneficiary count",
       (count(distinct td_booster_data.indID) * 100)::text       as "Total claim amount",
       td_booster_data.date_of_TD_booster::text                  as "Work completion date",
       td_booster_data.date_of_TD_booster::text                  as "Date of registration"


from td_booster_data
group by td_booster_data.date_of_TD_booster

union all

select 'C1.8'                                                               as "Code",
       200                                                                  as "Rate",
       count(distinct individual.id)                                        as "Beneficiary count",
       (count(distinct individual.id) * 200)::text                          as "Total claim amount",
       individual."Date of death"::date::text                               as "Work completion date",
       individual."Date of death"::date::text                               as "Date of registration"


from sakhiapp.individual 

where individual."Date of death" between :from_date::date and :to_date::date

group by 5,6

union all

select 'C1.9'                                                           as "Code",
       100                                                              as "Rate",
       count(distinct eighth_visit_data.indID)                          as "Beneficiary count",
       (count(distinct eighth_visit_data.indID) * 100)::text            as "Total claim amount",
       eighth_visit_data.date_of_8th_ANC::text                          as "Work completion date",
       first_visit_data.date_of_1st_ANC::text                           as "Date of registration"


from eighth_visit_data
         left join first_visit_data on eighth_visit_data.indID = first_visit_data.indID
group by eighth_visit_data.date_of_8th_ANC,
         first_visit_data.date_of_1st_ANC
        
union all         

select 'I1.1'                                                                   as "Code",
        100                                                                     as "Rate",
        count(distinct i.id)                                                    as "Beneficiary count",
        (count(distinct i.id) * 100)::text                                      as "Total claim amount",
        anc.encounter_date_time::date::text                                     as "Work completion date",
        anc.encounter_date_time::date::text                                     as "Date of registration"

from sakhiapp.individual i 
inner join sakhiapp.individual_pregnancy_anc anc on anc.individual_id = i.id 

where anc."Whether ANC done under PMSMA" = 'Yes'
and anc.encounter_date_time::date between :from_date::date and :to_date::date
and i.is_voided is false

group by 5,6

union all
         
select 'I2.1'                                                                               as  "Code",
        300                                                                                 as  "Rate",
        count(distinct i.id)                                    as "Beneficiary count",
        (count(distinct i.id) * 300 )::text                         as "Total claim amount",
        i."Date of Sterilization"::text                               as "Work completion date",
        i."Date of Sterilization"::text                             as "Date of registration"
        
from sakhiapp.individual i

where i.gender = 'Female'
and i."Date of Sterilization"::date between :from_date::date and :to_date::date
group by 5,6

union all 
         
select 'I2.2'                                                                               as  "Code",
        400                                                                                 as  "Rate",
        count(distinct i.id)                                    as "Beneficiary count",
        (count(distinct i.id) * 400 )::text                         as "Total claim amount",
        i."Date of Sterilization"::text                               as "Work completion date",
        i."Date of Sterilization"::text                             as "Date of registration"
        
from sakhiapp.individual i

where i.gender = 'Male'
and i."Date of Sterilization"::date between :from_date::date and :to_date::date
group by 5,6

union all 

select 'I2.3'                                                         as "Code",
        400                                                           as "Rate",
        count(distinct ind_id)                                        as "Beneficiary count",
        (count(distinct ind_id) * 400)::text                          as "Total claim amount",
        date_of_sterlization::text                                    as "Work completion date",
        date_of_delivery::text                                        as "Date of registration"
        
        from delivery_abortion_fpFollowup_data encData
        inner join client_user on (client_user.id = encData.created_by_id or client_user.id = encData.last_modified_by_id)
        
        where date_of_sterlization is not null 
        and date_of_delivery is not null 
        and delivery_encounter_date_time between :from_date::date and :to_date::date
        and date_of_sterlization::date between :from_date::date and :to_date::date
        
        group by 5,6
        
union all

select 'I2.4'                                                   as "Code",
        300                                                     as "Rate",
        count(distinct i.id )                               as "Beneficiary count",
        (count(distinct i.id ) * 300 )::text                as "Total claim amount",
        fpenl.enrolment_date_time::date::text                               as "Work completion date",
        abrt."Date of abortion"::text                                   as "Date of registration"

from sakhiapp.individual i 
inner join sakhiapp.individual_family_planning fpenl on fpenl.individual_id = i.id
inner join sakhiapp.individual_pregnancy_abortion abrt on abrt.individual_id = i.id 
inner join client_user on (client_user.id = i.created_by_id or client_user.id = i.last_modified_by_id)

where fpenl.program_exit_date_time is null 
and fpenl.enrolment_date_time::date between :from_date::date and :to_date::date
and i.is_voided is false 
and abrt."Date of abortion" is not null

group by 5,6

union all

select 'I2.5'                                                   as "Code",
        150                                                     as "Rate",
        count(distinct ind_id )                                 as "Beneficiary count",
        (count(distinct ind_id ) * 150 )::text                  as "Total claim amount",
        date_of_copperT::date::text                             as "Work completion date",
        date_of_delivery::text                                  as "Date of registration"

from delivery_abortion_fpFollowup_data encData
inner join client_user on (client_user.id = encData.created_by_id or client_user.id = encData.last_modified_by_id)

where date_of_copperT is not null 
and date_of_delivery is not null 
and date_of_copperT::date between :from_date::date and :to_date::date

group by 5,6

union all 

select 'I2.7'                                                                       as "Code",
        500                                                                         as "Rate",
        count(distinct i.id)                                                        as "Beneficiary count",
        (count(distinct i.id ) * 500 )::text                                        as "Total claim amount",
        enl."2nd Child date of birth"::date::text                                   as "Work completion date",
        enl."1st Child date of birth"::date::text                                   as "Date of registration"

from sakhiapp.individual i
inner join sakhiapp.individual_family_planning enl on enl.individual_id = i.id 
inner join client_user on (client_user.id = i.created_by_id or client_user.id = i.last_modified_by_id)

where regexp_replace(enl."Gap between 1st child and 2nd child", '\D','','g')::numeric >= 36
and enl.enrolment_date_time::date between :from_date::date and :to_date::date


group by 5,6

union all

select 'I2.8'                                                                       as "Code",
        1000                                                                        as "Rate",
        count(distinct i.id)                                                        as "Beneficiary count",
        (count(distinct i.id ) * 1000 )::text                                       as "Total claim amount",
        enl."2nd Child date of birth"::date::text                                   as "Work completion date",
        enl."1st Child date of birth"::date::text                                   as "Date of registration"

from sakhiapp.individual i
inner join sakhiapp.individual_family_planning enl on enl.individual_id = i.id 
inner join sakhiapp.individual_family_planning_fp_followup fp on fp.individual_id = i.id 
inner join client_user on (client_user.id = i.created_by_id or client_user.id = i.last_modified_by_id)

where enl.program_exit_date_time is null 
and enl.enrolment_date_time is not null 
and enl."Total number of children" = 2
and fp."Date of sterilisation for family planning method" between :from_date::date and :to_date::date 

group by 5,6

union all

select 'C3.4'                                                   as "Code",
        100                                                     as "Rate",
        count(distinct ind_id)                                  as "Beneficiary count",
        (count(distinct ind_id) * 100)::text                    as "Total claim amount",
        last_completion_date::date::text                        as "Work completion data",
        first_completion_date::date::text                       as "Date of registration"
        
        from cte_for_c34 ctedata
        where all_vaccines_done = 5
        and last_completion_date::date between :from_date::date and :to_date::date
        and age_in_years <= 1
        group by first_completion_date, last_completion_date 
        
union all 

select 'C3.5'                                                   as "Code",
        75                                                      as "Rate",
        count(distinct ind_id)                                  as "Beneficiary count",
        (count(distinct ind_id) * 75)::text                     as "Total claim amount",
        last_completion_date::date::text                        as "Work completion data",
        first_completion_date::date::text                       as "Date of registration"
        
        from cte_for_c35
        where all_vaccines_done >= 4
        and last_completion_date::date between :from_date::date and :to_date::date
        and age_in_years <= 2
        group by first_completion_date, last_completion_date

union all 

select 'C3.6'                                                               as "Code",
        50                                                                  as "Rate",
        count(distinct individual.id)                                       as "Beneficiary count",
        (count(distinct individual.id) * 50)::text                          as "Total claim amount",
        checklist_item.completion_date::date::text                          as "Work completion data",
        checklist_item.completion_date::date::text                          as "Date of registration"
        
from checklist_item  
inner join checklist_item_detail on checklist_item_detail.id  = checklist_item.checklist_item_detail_id
inner join concept on checklist_item_detail.concept_id = concept.id
inner join checklist on checklist_item.checklist_id = checklist.id
inner join sakhiapp.individual_child program_enrolment on checklist.program_enrolment_id = program_enrolment.id
inner join sakhiapp.individual on program_enrolment.individual_id = individual.id
inner join client_user on (client_user.id = individual.created_by_id or client_user.id = individual.last_modified_by_id)

where completion_date is not null 
and concept.name in ('DPT Booster 2')
and individual.is_voided is false 
and program_enrolment.program_exit_date_time is null 

group by 5,6

union all 

select 'C4.1'                                                           as "Code",
        250                                                             as "Rate",
        count(distinct sixth_hbnc_data.indID)                           as "Beneficiary count",
        (count(distinct sixth_hbnc_data.indID) * 250)::text             as "Total claim amount",
        sixth_hbnc_data.date_of_6th_hbnc::text                          as "Work completion data",
        second_hbnc_data.date_of_2nd_hbnc::text                         as "Date of registration"
        
from sixth_hbnc_data
    left join second_hbnc_data on sixth_hbnc_data.indID = second_hbnc_data.indID
    
where (place_of_delivery <> 'Home' or place_of_delivery <> 'In-transit')
and delivery_type <> 'C-section'

group by sixth_hbnc_data.date_of_6th_hbnc,second_hbnc_data.date_of_2nd_hbnc

union all

select 'C4.1'                                                           as "Code",
        250                                                             as "Rate",
        count(distinct sixth_hbnc_data.indID)                           as "Beneficiary count",
        (count(distinct sixth_hbnc_data.indID) * 250)::text             as "Total claim amount",
        sixth_hbnc_data.date_of_6th_hbnc::text                          as "Work completion data",
        third_hbnc_data.date_of_3rd_hbnc::text                          as "Date of registration"
        
from sixth_hbnc_data
    left join third_hbnc_data on sixth_hbnc_data.indID = third_hbnc_data.indID
    
where (place_of_delivery <> 'Home' or place_of_delivery <> 'In-transit')
and delivery_type = 'C-section'

group by sixth_hbnc_data.date_of_6th_hbnc,third_hbnc_data.date_of_3rd_hbnc

union all

select 'C4.1'                                                           as "Code",
        250                                                             as "Rate",
        count(distinct seventh_hbnc_data.indID)                         as "Beneficiary count",
        (count(distinct seventh_hbnc_data.indID) * 250)::text           as "Total claim amount",
        seventh_hbnc_data.date_of_7th_hbnc::text                        as "Work completion data",
        first_hbnc_data.date_of_1st_hbnc::text                          as "Date of registration"
        
from seventh_hbnc_data
    left join first_hbnc_data on seventh_hbnc_data.indID = first_hbnc_data.indID
    
where (place_of_delivery = 'Home' or place_of_delivery = 'In-transit')

group by seventh_hbnc_data.date_of_7th_hbnc,first_hbnc_data.date_of_1st_hbnc

union all

select 'C4.7'                                                       as "Code",
        50                                                          as "Rate",
        count(distinct fifth_hbyc_data.indID)                       as "Beneficiary count",
        (count(distinct fifth_hbyc_data.indID) * 50)::text          as "Total claim amount",
        fifth_hbyc_data.date_of_5th_hbyc::text                      as "Work completion data",
        first_hbnc_data.date_of_1st_hbnc::text                      as "Date of registration"
        
from fifth_hbyc_data
    left join first_hbnc_data on fifth_hbyc_data.indID = first_hbnc_data.indID

group by fifth_hbyc_data.date_of_5th_hbyc,first_hbnc_data.date_of_1st_hbnc

union all 

select 'C7.1'                                                   as "Code",
        10                                                      as "Rate",
        count(distinct individual.id)                           as "Beneficiary count",
        (count(distinct individual.id) *10)::text               as "Total claim amount",
        cbac.encounter_date_time::date::text                    as "Work completion date",
        cbac.encounter_date_time::date::text                    as "Date of registration"
        
from sakhiapp.individual
inner join sakhiapp.individual_cbac cbac on cbac.individual_id = individual.id 
inner join client_user on (client_user.id = individual.created_by_id or client_user.id = individual.last_modified_by_id)

where individual.is_voided is false 
and cbac.encounter_date_time between :from_date::date and :to_date::date 

group by 5,6

union all 

select 'I1.4'                                                   as "Code",
        300                                                     as "Rate",
        count(distinct individual.id)                           as "Beneficiary count",
        (count(distinct individual.id) * 300)::text             as "Total claim amount",
        checklist_item."completion_date"::date::text            as "Work completion data",
        del."Date of Admission"::text                           as "Date of registration"

from checklist_item
                             inner join checklist_item_detail on checklist_item_detail.id  = checklist_item.checklist_item_detail_id
                             inner join checklist on checklist_item.checklist_id = checklist.id
                             inner join sakhiapp.individual_child program_enrolment on checklist.program_enrolment_id = program_enrolment.id
                             inner join sakhiapp.individual individual on program_enrolment.individual_id = individual.id
                             join concept on checklist_item_detail.concept_id = concept.id
                             join mother_child_data mcd on (individual.id = mcd.mother_id or individual.id = mcd.child_id)
                             left join sakhiapp.individual_pregnancy_delivery del on del.individual_id = mcd.mother_id
                             inner join client_user usr on (usr.id = individual.created_by_id or usr.id = individual.last_modified_by_id )

where checklist_item."completion_date"::date  between :from_date::date and :to_date::date
and individual.is_voided is false
and concept.name = 'BCG'
and (del."Place of delivery" <> 'Home' or del."Place of delivery" <> 'In-transit' ) 

group by checklist_item."completion_date", del."Date of Admission"

union all 

select 'I1.5'                                                   as "Code",
        200                                                     as "Rate",
        count(distinct individual.id)                           as "Beneficiary count",
        (count(distinct individual.id) * 200)::text             as "Total claim amount",
        checklist_item."completion_date"::date::text            as "Work completion data",
        del."Date of Admission"::text                           as "Date of registration"

from checklist_item
                             join checklist_item_detail on checklist_item_detail.id  = checklist_item.checklist_item_detail_id
                             join checklist on checklist_item.checklist_id = checklist.id
                             join sakhiapp.individual_child program_enrolment on checklist.program_enrolment_id = program_enrolment.id
                             join sakhiapp.individual individual on program_enrolment.individual_id = individual.id
                             join concept on checklist_item_detail.concept_id = concept.id
                             join mother_child_data mcd on (individual.id = mcd.mother_id or individual.id = mcd.child_id)
                             left join sakhiapp.individual_pregnancy_delivery del on del.individual_id = mcd.mother_id
                             inner join client_user usr on (usr.id = individual.created_by_id or usr.id = individual.last_modified_by_id )

where checklist_item."completion_date"::date  between :from_date::date and :to_date::date
and individual.is_voided is false
and concept.name = 'BCG'
and (del."Place of delivery" <> 'Home' or del."Place of delivery" <> 'In-transit' ) 

group by checklist_item."completion_date", del."Date of Admission"

union all

select 'C2.4'                                                   as "Code",
        100                                                     as "Rate",
        count(distinct individual.id)                           as "Beneficiary count",
        (count(distinct individual.id) * 100)::text             as "Total claim amount",
        individual."Date of ANTRA injection"::text              as "Work completion data",
        individual."Date of ANTRA injection"::text              as "Date of registration"
        
from sakhiapp.individual 
inner join client_user usr on (usr.id = individual.created_by_id or usr.id = individual.last_modified_by_id )

where individual."Date of ANTRA injection" between :from_date::date and :to_date::date
and individual.is_voided is false 

group by individual."Date of ANTRA injection", individual."Date of ANTRA injection"

union all 

select 'I8.1'                                                        as "Code",
        500                                                          as "Rate",
        count(distinct i.id)                                         as "Beneficiary count",
        (count(distinct i.id) * 500)::text                           as "Total claim amount",
        enc."TB case status date"::date::text                        as "Work completion data",
        enc."TB case status date"::date::text                        as "Date of registration"

from sakhiapp.individual i 
inner join sakhiapp.individual_tuberculosis enl on enl.individual_id = i.id 
inner join sakhiapp.individual_tuberculosis_tuberculosis_follow_up enc on enc.individual_id = i.id 
inner join client_user on (client_user.id = i.created_by_id or client_user.id = i.last_modified_by_id)

where enl.program_exit_date_time is null 
and enc."TB case status date" is not null 
and enc."TB case status date"::date between :from_date::date and :to_date::date
and enc."TB case status" = 'Confirmed'
and i.is_voided is false

group by 5,6

union all

select 'I8.13'                                                       as "Code",
        300                                                          as "Rate",
        count(distinct i.id)                                         as "Beneficiary count",
        (count(distinct i.id) * 300)::text                           as "Total claim amount",
        ace.enrolment_date_time::date::text                          as "Work completion data",
        ace.enrolment_date_time::date::text                          as "Date of registration"

from sakhiapp.individual i 
inner join sakhiapp.individual_acute_encephalitis ace on ace.individual_id = i.id 
inner join client_user on (client_user.id = i.created_by_id or client_user.id = i.last_modified_by_id)

where ace.enrolment_date_time::date between :from_date::date and :to_date::date
and ace."Referred to" <> 'Other private hospital'


group by 5,6

union all

select 'I8.14'                                                       							as "Code",
		2000                                                          							as "Rate",
		count(distinct td.ind_id)                                         	    as "Beneficiary count",
        (count(distinct td.ind_id) * 2000)::text        	                    as "Total claim amount",
		max(td.encounter_date_time::date::text) filter ( where visit_number = 1 )									as "Work completion date",
		tcd.confirmed_date::text												as "Date of registration"
        
from sakhiapp.individual i 
inner join tuberculosis_data td on td.ind_id = i.id
inner join tb_confirmed_data tcd on tcd.ind_id = i.id
inner join client_user on (client_user.id = td.created_by_id or client_user.id = td.last_modified_by_id)

where td."Type of TB" = 'Drug resistant'
and AGE(DATE 'now', (tcd.confirmed_date)::date) >= INTERVAL '1 month'
and AGE(DATE 'now', (tcd.confirmed_date)::date) < INTERVAL '6 month'

group by 6

union all

select 'I8.15'                                                       			as "Code",
        3000                                                          			as "Rate",
        count(distinct td.ind_id)                                         	    as "Beneficiary count",
        (count(distinct td.ind_id) * 3000)::text        	                    as "Total claim amount",
		completed.treatment_completed_date::date::text,
		confirmed_date::date::text
from sakhiapp.individual i
inner join tuberculosis_data td on td.ind_id = i.id
inner join tb_confirmed_data tcd on tcd.ind_id = i.id
inner join tb_completed_data completed on completed.ind_id = i.id
inner join client_user on (client_user.id = td.created_by_id or client_user.id = td.last_modified_by_id)

where completed.treatment_completed_date::date between :from_date::date and :to_date::date
and td."Type of TB" = 'Drug resistant'

group by 5,6

union all

select 'I8.17'                                                       			as "Code",
        1000                                                          			as "Rate",
        count(distinct td.ind_id)                                         	    as "Beneficiary count",
        (count(distinct td.ind_id) * 1000)::text        	                    as "Total claim amount",
		completed.treatment_completed_date::date::text,
		confirmed_date::date::text
from sakhiapp.individual i
inner join tuberculosis_data td on td.ind_id = i.id
inner join tb_confirmed_data tcd on tcd.ind_id = i.id
inner join tb_completed_data completed on completed.ind_id = i.id
inner join client_user on (client_user.id = td.created_by_id or client_user.id = td.last_modified_by_id)

where completed.treatment_completed_date::date between :from_date::date and :to_date::date
and td."Type of TB" = 'Drug sensitive'

group by 5,6

;
