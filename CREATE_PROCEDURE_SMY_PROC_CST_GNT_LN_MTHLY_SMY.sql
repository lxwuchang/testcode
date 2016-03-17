CREATE PROCEDURE SMY.PROC_CST_GNT_LN_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_GNT_LN_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_GNT_LN_MTHLY_SMY
-- Source Table:				SOR.SCR_AR_RLTNP,SOR.CTR_AR,SMY.LOAN_AR_SMY,SMY.DEP_AR_SMY,SOR.CST_CLT_INF
-- Target Table: 				SMY.CST_GNT_LN_MTHLY_SMY
-- Project:             ZJ RCCB EDW
-- Note                 Delete and Insert and Update
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.09
-- Origin Author:       Peng Jie
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-09   Peng Jie     Create SP File		
-- 2009-11-19   Xu Yan       Updated some logic
-- 2011-03-16   Wang You Bing       Add DEL_F And Updated some logic
-- 2012-02-28   Chen XiaoWen        1�����ݻָ����ݲ��裬�޸�CST_GNT_LN_MTHLY_SMYɾ����������Ϊʹ��ACG_DT������
--                                  2��ΪTMP_LOAN_AR_SMY��ʱ����������
-- 2012-03-16   Chen XiaoWen        1��ȥ��SELECT MAX(ACG_DT) ... SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT�������߼�
--                                  2��������ʱ��TMP_SCR_AR_RLTNP��TMP����м����������������
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*�����쳣����ʹ�ñ���*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
DECLARE SMY_DATE DATE;                                 --��ʱ���ڱ���
DECLARE SMY_RCOUNT INT;                                --DML������ü�¼��
DECLARE SMY_PROCNM VARCHAR(100);    --
DECLARE CUR_YEAR SMALLINT;--
DECLARE CUR_MONTH SMALLINT;--
DECLARE CUR_DAY INTEGER;--
--DECLARE MAX_ACG_DT DATE;--
DECLARE MTH_FIRST_DAY DATE; 
DECLARE MTH_LAST_DAY DATE;

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
  ROLLBACK;--
  SET SMY_STEPNUM = SMY_STEPNUM + 1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  RESIGNAL;--
END;--
/*
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;--
  SET SMY_STEPNUM = SMY_STEPNUM + 1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
END;--
*/

/*������ֵ*/
SET SMY_PROCNM = 'PROC_CST_GNT_LN_MTHLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --ȡ����
SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,iso),1,7)||'-01'); --ȡ���³���
VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ; --ȡ�������һ��
--SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT;	--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

---------------------------Start of modification on 20091119----------------------------------------
/*�û���ʱ��ռ�*/
DECLARE GLOBAL TEMPORARY TABLE TMP_LOAN_AR_SMY ( CTR_AR_ID CHAR(20), LN_BAL DECIMAL(17,2))
        ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID);--

CREATE INDEX SESSION.TMP_LOAN_AR_SMY_CTR_AR_ID ON SESSION.TMP_LOAN_AR_SMY(CTR_AR_ID);

insert into SESSION.TMP_LOAN_AR_SMY (CTR_AR_ID, LN_BAL) 
 select CTR_AR_ID
 			  ,SUM(LN_BAL) 
 from SMY.LOAN_AR_SMY
 group by CTR_AR_ID;--
 
 SET SMY_STEPNUM = SMY_STEPNUM+1;--
 SET SMY_STEPDESC = 'TMP_LOAN_AR_SMY��ʱ��������';--
 GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
---------------------------End of modification on 20091119----------------------------------------

/*���ݻָ��뱸��*/

   --DELETE FROM SMY.CST_GNT_LN_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   DELETE FROM SMY.CST_GNT_LN_MTHLY_SMY WHERE ACG_DT>=MTH_FIRST_DAY AND ACG_DT<=MTH_LAST_DAY;

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '������ͨ��������';--


--������ͨ��������
DECLARE GLOBAL TEMPORARY TABLE TMP_SCR_AR_RLTNP AS (
    SELECT
      SCRT_NO,
      SCRD_NO,
      PRIM_CST_ID,
      SCR_TP_ID,
      SCR_AMT
    FROM SOR.SCR_AR_RLTNP
    WHERE SCR_TP_ID=15940001 AND DEL_F=0 AND substr(SCRD_NO,4,2)<>'13'
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(SCRD_NO);

CREATE INDEX SESSION.IDX_TMP_SCR_AR_RLTNP ON SESSION.TMP_SCR_AR_RLTNP(SCRD_NO);

INSERT INTO SESSION.TMP_SCR_AR_RLTNP
    SELECT
      SCRT_NO,
      SCRD_NO,
      PRIM_CST_ID,
      SCR_TP_ID,
      SCR_AMT
    FROM SOR.SCR_AR_RLTNP
    WHERE SCR_TP_ID=15940001 AND DEL_F=0 AND substr(SCRD_NO,4,2)<>'13'
;

DECLARE GLOBAL TEMPORARY TABLE TMP AS (
    SELECT 
      a.SCRT_NO AS SCRT_NO,
      a.SCRD_NO AS SCRD_NO,
      COALESCE(b.DNMN_CCY_ID,'') AS DNMN_CCY_ID,
      2012 AS CYEAR,
      3 AS CMONTH,
      COALESCE(a.PRIM_CST_ID, '') AS PRIM_CST_ID,
      COALESCE(a.SCR_TP_ID, -1) AS SCR_TP_ID,
      '2012-03-12' AS SMYDATE,
      COALESCE(c.LN_BAL,0.00) AS LN_BAL,
      COALESCE(b.LMT_AMT, 0.00) AS LMT_AMT,
      a.SCR_AMT AS SCR_AMT
    FROM SESSION.TMP_SCR_AR_RLTNP a LEFT JOIN SOR.CTR_AR b ON a.SCRD_NO=b.CTR_AR_ID AND b.DEL_F=0
    LEFT JOIN SESSION.TMP_LOAN_AR_SMY c ON c.CTR_AR_ID = a.SCRD_NO
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(SCRD_NO);

CREATE INDEX SESSION.IDX_TMP ON SESSION.TMP(SCRT_NO,SCRD_NO,DNMN_CCY_ID,CYEAR,CMONTH,PRIM_CST_ID,SCR_TP_ID,SMYDATE,SCR_AMT);

INSERT INTO SESSION.TMP
    SELECT 
      a.SCRT_NO AS SCRT_NO,
      a.SCRD_NO AS SCRD_NO,
      COALESCE(b.DNMN_CCY_ID,'') AS DNMN_CCY_ID,
      CUR_YEAR,
      CUR_MONTH,
      COALESCE(a.PRIM_CST_ID, '') AS PRIM_CST_ID,
      COALESCE(a.SCR_TP_ID, -1) AS SCR_TP_ID,
      SMY_DATE,
      COALESCE(c.LN_BAL,0.00) AS LN_BAL,
      COALESCE(b.LMT_AMT, 0.00) AS LMT_AMT,
      a.SCR_AMT AS SCR_AMT
    FROM SESSION.TMP_SCR_AR_RLTNP a LEFT JOIN SOR.CTR_AR b ON a.SCRD_NO=b.CTR_AR_ID AND b.DEL_F=0
    LEFT JOIN SESSION.TMP_LOAN_AR_SMY c ON c.CTR_AR_ID = a.SCRD_NO
;

INSERT INTO SMY.CST_GNT_LN_MTHLY_SMY
   (GNTR_CST_ID              ,--�����˿ͻ�����
    CTR_AR_Id                ,--��ͬ��
    CCY                      ,--����
    CDR_YR                   ,--���YYYY
    CDR_MTH                  ,--�·�MM
    GNT_CST_ID               ,--�������˿ͻ�����
    CLT_TP_Id                ,--���ⵣ������
    ACG_DT                   ,--����YYYY-MM-DD
    BAL_AMT                  ,--�������
    CTR_AMT                  ,--��ͬ���
    GNTD_AMT                 )--�������
SELECT 
    SCRT_NO,
    SCRD_NO,
    DNMN_CCY_ID,
    CYEAR,
    CMONTH,
    PRIM_CST_ID,
    SCR_TP_ID,
    SMYDATE,
    SUM(LN_BAL),
    LMT_AMT,
    SUM(SCR_AMT)
FROM SESSION.TMP
GROUP BY 
    SCRT_NO,
    SCRD_NO,
    DNMN_CCY_ID,
    CYEAR,
    CMONTH,
    PRIM_CST_ID,
    SCR_TP_ID,
    SMYDATE,
    LMT_AMT
;

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '������������';--

--������������
INSERT INTO SMY.CST_GNT_LN_MTHLY_SMY
  (GNTR_CST_ID              ,--�����˿ͻ�����
   CTR_AR_Id                ,--��ͬ��
   CCY                      ,--����
   CDR_YR                   ,--���YYYY
   CDR_MTH                  ,--�·�MM
   GNT_CST_ID               ,--�������˿ͻ�����
   CLT_TP_Id                ,--���ⵣ������
   ACG_DT                   ,--����YYYY-MM-DD
   BAL_AMT                  ,--�������
   CTR_AMT                  ,--��ͬ���
   GNTD_AMT                 )--�������
WITH TMP_SCR_AR_RLTNP AS (SELECT 
                            SCRT_NO           ,
                            SCRD_NO           ,
                            PRIM_CST_ID       ,
                            SCR_TP_ID         ,
                            SCR_AMT           
                          FROM SOR.SCR_AR_RLTNP 
                          WHERE SCR_TP_ID=15940002)
SELECT 
  COALESCE(c.CST_ID ,'')                ,
  a.SCRD_NO                ,
  COALESCE(d.DNMN_CCY_ID ,'')           ,
  CUR_YEAR               ,
  CUR_MONTH              ,  
  COALESCE(a.PRIM_CST_ID, '')            ,
  COALESCE(a.SCR_TP_ID , -1)              ,
  SMY_DATE               ,
  SUM(COALESCE(e.LN_BAL,0.00))   ,
  d.LMT_AMT                ,
  SUM(a.SCR_AMT)
FROM TMP_SCR_AR_RLTNP a LEFT JOIN SOR.GNT_TEAM_CTR b on a.SCRT_NO=b.CTR_SEQ_NO and b.DEL_F=0  ---Add DEL_F By WangYoubing On 20110316---
                        LEFT JOIN SOR.GNT_TEAM_MEMBER c on c.GNT_ID = b.CTR_ID and c.DEL_F=0  ---Add DEL_F By WangYoubing On 20110316---
                        LEFT JOIN SOR.CTR_AR d ON a.SCRD_NO=d.CTR_AR_ID and d.DEL_F=0         ---Add DEL_F By WangYoubing On 20110316---
                        LEFT JOIN SESSION.TMP_LOAN_AR_SMY e on e.CTR_AR_ID = a.SCRD_NO
GROUP BY
  COALESCE(c.CST_ID ,'')                ,
  a.SCRD_NO                ,
  COALESCE(d.DNMN_CCY_ID ,'')           ,
  CUR_YEAR               ,
  CUR_MONTH              ,  
  COALESCE(a.PRIM_CST_ID, '')            ,
  COALESCE(a.SCR_TP_ID , -1)              ,
  SMY_DATE               ,         
  d.LMT_AMT                      --,
 -- a.SCR_AMT;--
;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

-----------------Modified ��ͬ����ȡ���߼� By WangYoubing On 20110316 Start-----
SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '�����ͬ��������';--

--�����ͬ��������
INSERT INTO SMY.CST_GNT_LN_MTHLY_SMY
  (GNTR_CST_ID              ,--�����˿ͻ�����
   CTR_AR_Id                ,--��ͬ��
   CCY                      ,--����
   CDR_YR                   ,--���YYYY
   CDR_MTH                  ,--�·�MM
   GNT_CST_ID               ,--�������˿ͻ�����
   CLT_TP_Id                ,--���ⵣ������
   ACG_DT                   ,--����YYYY-MM-DD
   BAL_AMT                  ,--�������
   CTR_AMT                  ,--��ͬ���
   GNTD_AMT                 )--�������
WITH TMP_SCR_AR_RLTNP AS (SELECT 
                            A.SCRT_NO           ,
                            A.SCRD_NO           ,
                            A.PRIM_CST_ID       ,
                            A.SCR_TP_ID         ,
                            A.SCR_AMT           ,
                            B.SCRT_NO AS SCRT_NO_B
                          FROM SOR.SCR_AR_RLTNP A
                          INNER JOIN (SELECT SCRT_NO,SCRD_NO FROM SOR.SCR_AR_RLTNP WHERE DEL_F=0 AND SCR_TP_ID=15940001 AND substr(SCRD_NO,4,2)='13') B
                          ON A.SCRT_NO=B.SCRD_NO
                          WHERE SCR_TP_ID=15940003
                          AND A.DEL_F=0),
TMP_SCR_AR_RLTNP1 AS (SELECT 
                            A.SCRT_NO           ,
                            A.SCRD_NO           ,
                            A.PRIM_CST_ID       ,
                            A.SCR_TP_ID         ,
                            A.SCR_AMT           ,
                            B.SCRT_NO AS SCRT_NO_B
                          FROM SOR.SCR_AR_RLTNP A
                          INNER JOIN (SELECT SCRT_NO,SCRD_NO FROM SOR.SCR_AR_RLTNP WHERE DEL_F=0 AND SCR_TP_ID=15940004 AND substr(SCRD_NO,4,2)='13') B
                          ON A.SCRT_NO=B.SCRD_NO
                          WHERE SCR_TP_ID=15940003
                          AND A.DEL_F=0),
    TMP_DEP_AR as (
       SELECT DMD_DEP_AR_ID as DEP_AR_ID
         			, PRIM_CST_ID as CST_ID
       FROM SOR.DMD_DEP_MN_AR
       WHERE DEL_F=0
       UNION ALL
       SELECT FT_DEP_AR_ID as DEP_AR_ID
       				,PRIM_CST_ID as CST_ID
       FROM SOR.FT_DEP_AR
       WHERE DEL_F=0
     ),
TMP_SCR_AR_RLTNP2 AS (SELECT 
                            A.SCRT_NO           ,
                            A.SCRD_NO           ,
                            A.PRIM_CST_ID       ,
                            A.SCR_TP_ID         ,
                            A.SCR_AMT           ,
                            B.SCRT_NO AS SCRT_NO_B
                          FROM SOR.SCR_AR_RLTNP A
                          INNER JOIN (SELECT SCRT_NO,SCRD_NO FROM SOR.SCR_AR_RLTNP WHERE DEL_F=0 AND SCR_TP_ID IN (15940005,15940006) AND substr(SCRD_NO,4,2)='13') B
                          ON A.SCRT_NO=B.SCRD_NO
                          WHERE SCR_TP_ID=15940003
                          AND A.DEL_F=0)
SELECT 
   GNTR_CST_ID              ,--�����˿ͻ�����
   CTR_AR_Id                ,--��ͬ��
   CCY                      ,--����
   CDR_YR                   ,--���YYYY
   CDR_MTH                  ,--�·�MM
   GNT_CST_ID               ,--�������˿ͻ�����
   CLT_TP_Id                ,--���ⵣ������
   ACG_DT                   ,--����YYYY-MM-DD
   SUM(BAL_AMT)                  ,--�������
   SUM(CTR_AMT)                  ,--��ͬ���
   SUM(GNTD_AMT)                 --�������  
FROM (
SELECT 
  --COALESCE(d.PRIM_CST_ID,'')                         ,
  COALESCE(a.SCRT_NO_B,'')                         GNTR_CST_ID,
  a.SCRD_NO                        CTR_AR_Id,
  COALESCE(b.DNMN_CCY_ID, '')                    CCY,
  CUR_YEAR                       CDR_YR,
  CUR_MONTH                      CDR_MTH,   
  COALESCE(a.PRIM_CST_ID , '')                   GNT_CST_ID,
  COALESCE(a.SCR_TP_ID , -1)                     CLT_TP_Id,
  SMY_DATE                       ACG_DT,
  SUM(COALESCE(c.LN_BAL,0.00))    BAL_AMT,
  COALESCE( b.LMT_AMT  , 0.00)                      CTR_AMT,
  SUM(a.SCR_AMT)    GNTD_AMT                    
FROM TMP_SCR_AR_RLTNP a INNER join SOR.CTR_AR b on a.SCRD_NO=b.CTR_AR_ID and b.DEL_F=0
                        INNER JOIN SESSION.TMP_LOAN_AR_SMY c ON c.CTR_AR_ID = a.SCRD_NO
                        --INNER JOIN SOR.CTR_AR d ON a.SCRT_NO=d.CTR_AR_ID
GROUP BY
  COALESCE(a.SCRT_NO_B,'')                         ,
  a.SCRD_NO                        ,
  COALESCE(b.DNMN_CCY_ID, '')                    ,
  CUR_YEAR                       ,
  CUR_MONTH                      ,   
  COALESCE(a.PRIM_CST_ID , '')                   ,
  COALESCE(a.SCR_TP_ID , -1)                     ,
  SMY_DATE                       ,
  COALESCE( b.LMT_AMT  , 0.00)
UNION ALL
(SELECT 
  COALESCE(d.CST_ID,'')                  GNTR_CST_ID,
  a.SCRD_NO                 CTR_AR_Id,
  COALESCE(b.DNMN_CCY_ID ,'')            CCY,
  CUR_YEAR                CDR_YR,
  CUR_MONTH               CDR_MTH,  
  COALESCE(a.PRIM_CST_ID, '')             GNT_CST_ID,
  COALESCE(a.SCR_TP_ID, -1)               CLT_TP_Id,
  SMY_DATE                ACG_DT,
  SUM(COALESCE(c.LN_BAL,0.00))   BAL_AMT,
  COALESCE(b.LMT_AMT, 0.00)                 CTR_AMT,
  SUM(a.SCR_AMT) GNTD_AMT
FROM TMP_SCR_AR_RLTNP1 a INNER JOIN SOR.CTR_AR b ON a.SCRD_NO=b.CTR_AR_ID AND b.DEL_F=0
                        INNER JOIN SESSION.TMP_LOAN_AR_SMY c on c.CTR_AR_ID = a.SCRD_NO
                        INNER JOIN TMP_DEP_AR d ON a.SCRT_NO_B=d.DEP_AR_ID
GROUP BY 
  COALESCE(d.CST_ID,'')                  ,
  a.SCRD_NO                 ,
  COALESCE(b.DNMN_CCY_ID ,'')            ,
  CUR_YEAR                ,
  CUR_MONTH               ,  
  COALESCE(a.PRIM_CST_ID, '')             ,
  COALESCE(a.SCR_TP_ID, -1)               ,
  SMY_DATE                ,
  COALESCE(b.LMT_AMT, 0.00) 
)
UNION ALL
(SELECT 
  COALESCE(d.PRIM_CST_ID, '')           GNTR_CST_ID,
  a.SCRD_NO               CTR_AR_Id,
  COALESCE(b.DNMN_CCY_ID, '')           CCY,
  CUR_YEAR                CDR_YR,
  CUR_MONTH               CDR_MTH,   
  COALESCE(a.PRIM_CST_ID , '')          GNT_CST_ID,
  COALESCE(a.SCR_TP_ID  , -1)           CLT_TP_Id,
  SMY_DATE                ACG_DT,
  SUM(COALESCE(c.LN_BAL,0.00))   BAL_AMT,
  COALESCE(b.LMT_AMT, 0.00)               CTR_AMT,
  SUM(a.SCR_AMT) GNTD_AMT
FROM TMP_SCR_AR_RLTNP2 a INNER JOIN SOR.CTR_AR b on a.SCRD_NO=b.CTR_AR_ID and b.DEL_F=0
                        INNER JOIN SESSION.TMP_LOAN_AR_SMY c ON c.CTR_AR_ID = a.SCRD_NO 
                        INNER JOIN (SELECT CLT_SRL_NBR,PRIM_CST_ID 
                                   FROM (SELECT RANK() OVER(PARTITION BY CLT_SRL_NBR,PRIM_CST_ID ORDER BY CLT_ORDR_NBR ) NUM,CLT_SRL_NBR,PRIM_CST_ID 
                                         FROM SOR.CST_CLT_INF
                                         WHERE DEL_F=0)M 
                                   WHERE NUM=1) d
                                   ON d.CLT_SRL_NBR=a.SCRT_NO_B   
GROUP BY
  COALESCE(d.PRIM_CST_ID, '')           ,
  a.SCRD_NO               ,
  COALESCE(b.DNMN_CCY_ID, '')           ,
  CUR_YEAR                ,
  CUR_MONTH               ,   
  COALESCE(a.PRIM_CST_ID , '')          ,
  COALESCE(a.SCR_TP_ID  , -1)           ,
  SMY_DATE                ,
  COALESCE(b.LMT_AMT, 0.00)
)
) M
GROUP BY GNTR_CST_ID              ,--�����˿ͻ�����
   CTR_AR_Id                ,--��ͬ��
   CCY                      ,--����
   CDR_YR                   ,--���YYYY
   CDR_MTH                  ,--�·�MM
   GNT_CST_ID               ,--�������˿ͻ�����
   CLT_TP_Id                ,--���ⵣ������
   ACG_DT                   --����YYYY-MM-DD
;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

-----------------Modified ��ͬ����ȡ���߼� By WangYoubing On 20110316 End-----

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '���뱣֤�𵣱�����';--

--���뱣֤�𵣱�����
INSERT INTO SMY.CST_GNT_LN_MTHLY_SMY
  (GNTR_CST_ID              ,--�����˿ͻ�����
   CTR_AR_Id                ,--��ͬ��
   CCY                      ,--����
   CDR_YR                   ,--���YYYY
   CDR_MTH                  ,--�·�MM
   GNT_CST_ID               ,--�������˿ͻ�����
   CLT_TP_Id                ,--���ⵣ������
   ACG_DT                   ,--����YYYY-MM-DD
   BAL_AMT                  ,--�������
   CTR_AMT                  ,--��ͬ���
   GNTD_AMT                 )--�������
WITH TMP_SCR_AR_RLTNP AS (SELECT 
                            SCRT_NO           ,
                            SCRD_NO           ,
                            PRIM_CST_ID       ,
                            SCR_TP_ID         ,
                            SCR_AMT           
                          FROM SOR.SCR_AR_RLTNP 
                          WHERE SCR_TP_ID=15940004
                          AND DEL_F=0                           ---Add DEL_F By WangYoubing On 20110316---
                          AND substr(SCRD_NO,4,2)<>'13'),       ---Add DEL_F By WangYoubing On 20110316---
    TMP_DEP_AR as (
       SELECT DMD_DEP_AR_ID as DEP_AR_ID
         			, PRIM_CST_ID as CST_ID
       FROM SOR.DMD_DEP_MN_AR
       WHERE DEL_F=0                     ---Add DEL_F By WangYoubing On 20110316---
       UNION ALL
       SELECT FT_DEP_AR_ID as DEP_AR_ID
       				,PRIM_CST_ID as CST_ID
       FROM SOR.FT_DEP_AR
       WHERE DEL_F=0                    ---Add DEL_F By WangYoubing On 20110316---
     )
SELECT 
  COALESCE(d.CST_ID,'')                  ,
  a.SCRD_NO                 ,
  COALESCE(b.DNMN_CCY_ID ,'')            ,
  CUR_YEAR                ,
  CUR_MONTH               ,  
  COALESCE(a.PRIM_CST_ID, '')             ,
  COALESCE(a.SCR_TP_ID, -1)               ,
  SMY_DATE                ,
  SUM(COALESCE(c.LN_BAL,0.00))   ,
  COALESCE(b.LMT_AMT, 0.00)                 ,
  SUM(a.SCR_AMT)
FROM TMP_SCR_AR_RLTNP a INNER JOIN SOR.CTR_AR b ON a.SCRD_NO=b.CTR_AR_ID AND b.DEL_F=0   ---Add DEL_F By WangYoubing On 20110316---
                        INNER JOIN SESSION.TMP_LOAN_AR_SMY c on c.CTR_AR_ID = a.SCRD_NO
                        INNER JOIN TMP_DEP_AR d ON a.SCRT_NO=d.DEP_AR_ID
GROUP BY 
  COALESCE(d.CST_ID,'')                  ,
  a.SCRD_NO                 ,
  COALESCE(b.DNMN_CCY_ID ,'')            ,
  CUR_YEAR                ,
  CUR_MONTH               ,  
  COALESCE(a.PRIM_CST_ID, '')             ,
  COALESCE(a.SCR_TP_ID, -1)               ,
  SMY_DATE                ,
  COALESCE(b.LMT_AMT, 0.00) 
  --a.SCR_AMT;--
;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '�����Ѻ,��Ѻ����';--

--�����Ѻ,��Ѻ����

INSERT INTO SMY.CST_GNT_LN_MTHLY_SMY
  (GNTR_CST_ID              ,--�����˿ͻ�����
   CTR_AR_Id                ,--��ͬ��
   CCY                      ,--����
   CDR_YR                   ,--���YYYY
   CDR_MTH                  ,--�·�MM
   GNT_CST_ID               ,--�������˿ͻ�����
   CLT_TP_Id                ,--���ⵣ������
   ACG_DT                   ,--����YYYY-MM-DD
   BAL_AMT                  ,--�������
   CTR_AMT                  ,--��ͬ���
   GNTD_AMT                 )--�������
WITH TMP_SCR_AR_RLTNP AS (SELECT 
                            SCRT_NO           ,
                            SCRD_NO           ,
                            PRIM_CST_ID       ,
                            SCR_TP_ID         ,
                            SCR_AMT           
                          FROM SOR.SCR_AR_RLTNP 
                          WHERE SCR_TP_ID IN (15940005, 15940006)
                          AND DEL_F=0                                       ---Add DEL_F By WangYoubing On 20110316---
                          AND substr(SCRD_NO,4,2)<>'13')                    ---Add DEL_F By WangYoubing On 20110316---
SELECT 
  COALESCE(d.PRIM_CST_ID, '')           ,
  a.SCRD_NO               ,
  COALESCE(b.DNMN_CCY_ID, '')           ,
  CUR_YEAR                ,
  CUR_MONTH               ,   
  COALESCE(a.PRIM_CST_ID , '')          ,
  COALESCE(a.SCR_TP_ID  , -1)           ,
  SMY_DATE                ,
  SUM(COALESCE(c.LN_BAL,0.00))   ,
  COALESCE(b.LMT_AMT, 0.00)               ,
  SUM(a.SCR_AMT)
FROM TMP_SCR_AR_RLTNP a INNER JOIN SOR.CTR_AR b on a.SCRD_NO=b.CTR_AR_ID and b.DEL_F=0         ---Add DEL_F By WangYoubing On 20110316---
                        INNER JOIN SESSION.TMP_LOAN_AR_SMY c ON c.CTR_AR_ID = a.SCRD_NO 
                        INNER JOIN (SELECT CLT_SRL_NBR,PRIM_CST_ID 
                                   FROM (SELECT RANK() OVER(PARTITION BY CLT_SRL_NBR,PRIM_CST_ID ORDER BY CLT_ORDR_NBR ) NUM,CLT_SRL_NBR,PRIM_CST_ID 
                                         FROM SOR.CST_CLT_INF
                                         WHERE DEL_F=0)M 
                                   WHERE NUM=1) d
                                   ON d.CLT_SRL_NBR=a.SCRT_NO                
GROUP BY
  COALESCE(d.PRIM_CST_ID, '')           ,
  a.SCRD_NO               ,
  COALESCE(b.DNMN_CCY_ID, '')           ,
  CUR_YEAR                ,
  CUR_MONTH               ,   
  COALESCE(a.PRIM_CST_ID , '')          ,
  COALESCE(a.SCR_TP_ID  , -1)           ,
  SMY_DATE                ,
  COALESCE(b.LMT_AMT, 0.00)
  --a.SCR_AMT
;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--


END@