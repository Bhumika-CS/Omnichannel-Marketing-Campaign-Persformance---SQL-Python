-- 1️⃣ Create a backup of the existing table (if it exists)
DROP TABLE IF EXISTS "Consolidated_Campaign_Data_Backup";
CREATE TABLE IF NOT EXISTS "Consolidated_Campaign_Data_Backup" AS
SELECT * FROM "Consolidated_Campaign_Data";

-- 2️⃣ Truncate old data (preserve structure)
DROP TABLE IF EXISTS "Consolidated_Campaign_Data";

-- 3️⃣ Recreate and load fresh data
CREATE TABLE "Consolidated_Campaign_Data" AS
WITH display_union AS (
    SELECT * FROM display_vendor1
    UNION ALL
    SELECT * FROM display_vendor2
),
display AS (
    SELECT 
        'Third party' AS source,
         "Source" AS vendor,
        'Display' AS channel_name,
        "Campaign_name " AS campaign_name,
        "HCP_ID" AS npi,
        TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS') AS delivered_datetime,
        COUNT("DateTime")::int AS no_of_sends
    FROM display_union
    WHERE UPPER(TRIM("Activity ")) = 'VIEW'
    GROUP BY 1,2,3,4,5,6
),
display_clicks AS (
    SELECT 
        "HCP_ID" AS npi,
        MIN(TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS')) AS primary_eng_datetime,
        COUNT("DateTime")::int AS no_of_prim_eng
    FROM display_union
    WHERE UPPER(TRIM("Activity ")) = 'CLICK'
    GROUP BY 1
),
thirdparty_email AS (
    SELECT 
        'Third party' AS source,
         "Source" AS vendor,
        'Third party email' AS channel_name,
        "Campaign_name " AS campaign_name,
        "HCP_ID" AS npi,
        TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS') AS delivered_datetime,
        COUNT("DateTime")::int AS no_of_sends
    FROM third_party_email
    WHERE UPPER(TRIM("Activity ")) = 'DELIVERED'
    GROUP BY 1,2,3,4,5,6
),
thirdparty_open AS (
    SELECT 
        "HCP_ID" AS npi,
        MIN(TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS')) AS primary_eng_datetime,
        COUNT("DateTime")::int AS no_of_prim_eng
    FROM third_party_email
    WHERE UPPER(TRIM("Activity ")) = 'OPEN'
    GROUP BY 1
),
thirdparty_click AS (
    SELECT 
        "HCP_ID" AS npi,
        MIN(TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS')) AS sencondary_eng_datetime,
        COUNT("DateTime")::int AS no_of_sec_eng
    FROM third_party_email
    WHERE UPPER(TRIM("Activity ")) = 'CLICK'
    GROUP BY 1
),
internal_sent AS (
    SELECT 
        'Internal' AS source,
         "Source" AS vendor,
        'Email' AS channel_name,
        "Campaign_name " AS campaign_name,
        "HCP_ID" AS npi,
        TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS') AS delivered_datetime,
        COUNT("DateTime")::int AS no_of_sends
    FROM internal_email_sent
    WHERE UPPER("Status") = 'DELIVERED'
    GROUP BY 1,2,3,4,5,6
),
internal_open AS (
    SELECT 
        "HCP_ID" AS npi,
        MIN(TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS')) AS primary_eng_datetime,
        COUNT("DateTime")::int AS no_of_prim_eng
    FROM internal_email_opens
    WHERE UPPER("Status") = 'OPEN'
    GROUP BY 1
),
internal_click AS (
    SELECT 
        "HCP_ID" AS npi,
        MIN(TO_TIMESTAMP("DateTime", 'DD/MM/YY HH24:MI:SS')) AS sencondary_eng_datetime,
        COUNT("DateTime")::int AS no_of_sec_eng
    FROM internal_email_clicks
    WHERE UPPER("Status") = 'CLICK'
    GROUP BY 1
),
combined AS (
    SELECT 
        d.source,d.vendor,d.channel_name,d.campaign_name,d.npi,d.delivered_datetime,d.no_of_sends,
        c.primary_eng_datetime,c.no_of_prim_eng,NULL::timestamp AS sencondary_eng_datetime,NULL::int AS no_of_sec_eng
    FROM display d
    LEFT JOIN display_clicks c ON d.npi=c.npi AND d.delivered_datetime<=c.primary_eng_datetime
    UNION ALL
    SELECT 
        t.source,t.vendor,t.channel_name,t.campaign_name,t.npi,t.delivered_datetime,t.no_of_sends,
        o.primary_eng_datetime,o.no_of_prim_eng,c.sencondary_eng_datetime,c.no_of_sec_eng
    FROM thirdparty_email t
    LEFT JOIN thirdparty_open o ON t.npi=o.npi AND t.delivered_datetime<=o.primary_eng_datetime
    LEFT JOIN thirdparty_click c ON t.npi=c.npi AND o.primary_eng_datetime<=c.sencondary_eng_datetime
    UNION ALL
    SELECT 
        i.source,i.vendor,i.channel_name,i.campaign_name,i.npi,i.delivered_datetime,i.no_of_sends,
        o.primary_eng_datetime,o.no_of_prim_eng,c.sencondary_eng_datetime,c.no_of_sec_eng
    FROM internal_sent i
    LEFT JOIN internal_open o ON i.npi=o.npi AND i.delivered_datetime<=o.primary_eng_datetime
    LEFT JOIN internal_click c ON i.npi=c.npi AND o.primary_eng_datetime<=c.sencondary_eng_datetime
)
SELECT 
    z.source::varchar(50) AS source,
    z.vendor::varchar(50) AS vendor,
    z.channel_name::varchar(100) AS channel_name,
    z.campaign_name::varchar(255) AS campaign_name,
    x."Subject line"::varchar(255) AS subject_line,
    z.npi::bigint AS npi,
    y."MDM_ID"::bigint AS mdm_id,
    z.delivered_datetime AS delivered_datetime,
    z.no_of_sends,
    z.primary_eng_datetime,
    z.no_of_prim_eng, 
    z.sencondary_eng_datetime,
    z.no_of_sec_eng,
    y."Acad_Comm "::varchar(255) AS acad_comm,
    y."Teaching hospital "::varchar(255) AS teaching_hospital,
    y."Region "::varchar(255) AS region,
    y."Prescriber"::varchar(255) AS prescriber
FROM combined z
LEFT JOIN hcp_info y ON z.npi=y."HCP_ID"
LEFT JOIN campaigns x ON z.campaign_name=x."Campaingn";
