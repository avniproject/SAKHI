
INSERT INTO public.custom_query (uuid, name, query, organisation_id, is_voided, version, created_by_id,
                                 last_modified_by_id, created_date_time, last_modified_date_time)
VALUES ('2403c079-b728-4f70-8a69-165c8f7f0f40', 'dawa prapatra', 'with client_user as (select * from users where users.username = :username),
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

                                                                       enc_data as (select individual.id                                                          as indID,
                                                                                           enc.encounter_date_time::date,
                                                                                           enc."Whether ANC done under PMSMA",
                                                                                           enc."180 Iron Folic Acid Tablet issued or not?",
                                                                                           enc."TD 1 Dose"::date,
                                                                                           enc."TD 2 Dose"::date,
                                                                                           enc."TD Booster Dose"::date,
                                                                                           row_number()
                                                                                           over (partition by enc.individual_id order by encounter_date_time asc) as visit_number

                                                                                    from sakhiapp.individual
                                                                                             left join sakhiapp.individual_pregnancy pe on individual.id = pe.individual_id
                                                                                             left join sakhiapp.individual_pregnancy_anc enc on enc.program_enrolment_id = pe.id
                                                                                             left join client_user u1 on u1.id = enc.last_modified_by_id
                                                                                             left join client_user u2 on u2.id = enc.created_by_id
                                                                                    where encounter_date_time is not null
                                                                                      and individual.is_voided = false
                                                                                      and pe.is_voided = false
                                                                                      and enc.is_voided = false
                                                                                    and ( u1.id=enc.last_modified_by_id or u2.id=enc.created_by_id)
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
                                                                                             and  encounter_date_time between :from_date::date and :to_date::date)

                                                                  select 'C1.1'                                                   "Code",
                                                                         100                                                   as "Rate",
                                                                         count(distinct fourth_visit_data.indID)               as "Beneficiary count",
                                                                         (count(distinct fourth_visit_data.indID) * 100)::text as "Total claim amount",
                                                                         fourth_visit_data.date_of_4th_ANC::text                     as "Work completion date",
                                                                         first_visit_data.date_of_1st_ANC::text                      as "Date of registration"
                                                                  from fourth_visit_data
                                                                           left join first_visit_data on fourth_visit_data.indID = first_visit_data.indID
                                                                  group by fourth_visit_data.date_of_4th_ANC,
                                                                           first_visit_data.date_of_1st_ANC

                                                                  union all

                                                                  select 'C1.3'                                                   "Code",
                                                                         100                                                   as "Rate",
                                                                         count(distinct fourth_visit_data.indID)               as "Beneficiary count",
                                                                         (count(distinct fourth_visit_data.indID) * 100)::text as "Total claim amount",
                                                                         fourth_visit_data.date_of_4th_ANC::text                     as "Work completion date",
                                                                         second_visit_data.date_of_2nd_ANC::text                     as "Date of registration"
                                                                  from fourth_visit_data
                                                                           left join second_visit_data on second_visit_data.indID = fourth_visit_data.indID
                                                                  where "180 Iron Folic Acid Tablet issued or not?" = 'Yes'
                                                                  group by fourth_visit_data.date_of_4th_ANC,
                                                                           second_visit_data.date_of_2nd_ANC

                                                                  union all

                                                                  select 'C1.5'                                              as "Code",
                                                                         100                                                 as "Rate",
                                                                         count(distinct second_TD_data.indID)                as "Beneficiary count",
                                                                         (count(distinct second_TD_data.indID) * 100) ::text as "Total claim amount",
                                                                         second_TD_data.date_of_2nd_TD::text                       as "Work completion date",
                                                                         first_TD_data.date_of_1st_TD::text                        as "Date of registration"


                                                                  from second_TD_data
                                                                           left join first_TD_data on second_TD_data.indID = first_TD_data.indID
                                                                  group by second_TD_data.date_of_2nd_TD,
                                                                           first_TD_data.date_of_1st_TD

                                                                  union all

                                                                  select 'C1.5'                                              as "Code",
                                                                         100                                                 as "Rate",
                                                                         count(distinct td_booster_data.indID)               as "Beneficiary count",
                                                                         (count(distinct td_booster_data.indID) * 100)::text as "Total claim amount",
                                                                         td_booster_data.date_of_TD_booster::text                  as "Work completion date",
                                                                         td_booster_data.date_of_TD_booster::text                  as "Date of registration"


                                                                  from td_booster_data
                                                                  group by td_booster_data.date_of_TD_booster

                                                                  union all

                                                                  select 'C1.8'                                            as "Code",
                                                                         200                                               as "Rate",
                                                                         count(distinct anc_done_data.indID)               as "Beneficiary count",
                                                                         (count(distinct anc_done_data.indID) * 200)::text as "Total claim amount",
                                                                         anc_done_data.date_of_ANC::text                         as "Work completion date",
                                                                         anc_done_data.date_of_ANC::text                         as "Date of registration"


                                                                  from anc_done_data
                                                                  group by anc_done_data.date_of_ANC

                                                                  union all

                                                                  select 'C1.9'                                                 as "Code",
                                                                         100                                                    as "Rate",
                                                                         count(distinct seventh_visit_data.indID)               as "Beneficiary count",
                                                                         (count(distinct seventh_visit_data.indID) * 100)::text as "Total claim amount",
                                                                         seventh_visit_data.date_of_7th_ANC::text                     as "Work completion date",
                                                                         first_visit_data.date_of_1st_ANC::text                      as "Date of registration"


                                                                  from seventh_visit_data
                                                                           left join first_visit_data on seventh_visit_data.indID = first_visit_data.indID
                                                                  group by seventh_visit_data.date_of_7th_ANC,
                                                                           first_visit_data.date_of_1st_ANC', 206, false, 0, 4330, 4330, now(), now());
