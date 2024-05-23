---======== APPROACH 2 ===============
-- STEP 1: Create a staging table 
-- https://clickhouse.com/docs/en/sql-reference/functions/uuid-functions
-- Date -2 bytes of stroage
-- DateTime - 4 bytes of stroage 
CREATE TABLE staging_threats_severity (
    uuid UUID,
    threat_type String,
    raw_severity String,
    status String,
    detected_at DateTime('Australia/Sydney'),
    description String
) ENGINE = MergeTree()
ORDER BY uuid;

DROP TABLE staging_threats_severity;

DESCRIBE TABLE staging_threats_severity;
-- STEP 2 : Insert raw data into staging table
-- https://attack.mitre.org/matrices/enterprise/

INSERT INTO su_datatypes.staging_threats_severity (uuid, threat_type, raw_severity, status, detected_at, description) 
VALUES
    (generateUUIDv4(), 'Phishing', 'HIGH', 'Detected', now('Australia/Sydney'), 'Spear phishing link detected targeting financial department.'),
    (generateUUIDv4(), 'Credential Dumping', 'CRITICAL', 'In_Progress', now('Australia/Sydney'), 'Attempted credential dumping using LSASS process.'),
    (generateUUIDv4(), 'Command and Control', 'MEDIUM', 'Mitigated', now('Australia/Sydney'), 'C2 traffic identified and blocked on external firewall.'),
    (generateUUIDv4(), 'Privilege Escalation', 'MED', 'Resolved', now('Australia/Sydney'), 'Privilege escalation attempt using local exploit.'),
    (generateUUIDv4(), 'Data Exfiltration', 'INFO', 'Detected', now('Australia/Sydney'), 'Unusual data transfer to external IP address detected.');

DROP TABLE threats_severity;

-- STEP - 3 threats_severity with ENUM
CREATE TABLE threats_severity (
    uuid UUID,
    threat_type Enum8(
        'Phishing' = 1,
        'Credential Dumping' = 2,
        'Command and Control' = 3,
        'Privilege Escalation' = 4,
        'Data Exfiltration' = 5
    ),
    severity Enum8(
        'CRITICAL' = 1,
        'HIGH'  = 2,
        'MEDIUM' = 3,
        'LOW' = 4,
        'INFO'  = 5
    ),
    status Enum8(
        'Detected' = 1,
        'In_Progress' = 2,
        'Mitigated' = 3,
        'Resolved' = 4
    ),
    detected_at DateTime('Australia/Sydney'),
    description String
) ENGINE = MergeTree()
ORDER BY uuid;

-- DROP TABLE threats_severity; 
--- STEP - 4 - Transform and Insert Data from Staging to Main Table
-- https://clickhouse.com/docs/en/sql-reference/operators#operator_case
--=CONDITIONAL EXPRESSION
-- CASE [x]
--     WHEN a THEN b
--     [WHEN ... THEN ...]
--     [ELSE c]
-- END
INSERT INTO threats_severity(uuid, threat_type, severity, status, detected_at, description)
SELECT
    uuid,
    threat_type,
    CASE 
        WHEN raw_severity = 'CRITICAL' THEN 'CRITICAL'
        WHEN raw_severity = 'HIGH' THEN 'HIGH'
        WHEN raw_severity IN ('MEDIUM', 'MED') THEN 'MEDIUM'
        WHEN raw_severity =  'LOW' THEN 'LOW'
        WHEN raw_severity =  'INFO' THEN 'INFO'
        ELSE 'UNKNOWN'
    END AS severity,
    status,
    detected_at,
    description
FROM staging_threats_severity;


TRUNCATE TABLE threats_severity;

SELECT * FROM threats_severity;

--- INSTEAD OF CASE END, USE multiIf - multiIf
-- Allows to write the CASE operator more compactly in the query.
INSERT INTO threats_severity(uuid, threat_type, severity, status, detected_at, description)
SELECT 
    uuid,
    threat_type,
    multiIf(
        raw_severity = 'CRITICAL', 'CRITICAL',
        raw_severity = 'HIGH', 'HIGH',
        raw_severity IN ('MEDIUM', 'MED'),'MEDIUM',
        raw_severity = 'LOW' ,'LOW' ,
        raw_severity = 'INFO', 'INFO',
        'UNKNOWN'
    ) AS severity,
    status, 
    detected_at,
    description
FROM staging_threats_severity;




-- Enum value must be unique, so duplicates such as 'MEDIUM' = 3 , 'MED' = 3 will not work
-- Below is an example 
-- { "meta": [ ], "data": [ ], "rows": 0, "exception": "Code: 62. DB::Exception: Duplicate values in enum: 'MED' = 3 and '3'. (SYNTAX_ERROR) (version 24.2.2.16100 (official build))" }
CREATE TABLE cyber_threats (
    uuid UUID,
    threat_type Enum8(
        'Malware' = 1,
        'Phishing' = 2,
        'DDoS' = 3,
        'Ransomware' = 4,
        'Insider Threat' = 5
    ),
    severity Enum8(
        'CRITICAL' = 1,
        'HIGH' = 2,
        'MEDIUM' = 3,
        'MED' = 3,
        'LOW' = 4,
        'INFO' = 5   
    ),
    status Enum8(
        'Detected' = 1,
        'In Progress' = 2,
        'Mitigated' = 3,
        'Resolved'= 4
    ),
    detected_at DateTime,
    description String 
)
ENGINE = MergeTree()
ORDER BY uuid;

DESCRIBE TABLE su_datatypes.cyber_threats;
