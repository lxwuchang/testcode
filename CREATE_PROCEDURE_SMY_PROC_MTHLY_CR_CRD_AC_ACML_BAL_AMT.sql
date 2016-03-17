CREATE PROCEDURE SMY.PROC_MTHLY_CR_CRD_AC_ACML_BAL_AMT(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_MTHLY_CR_CRD_AC_ACML_BAL_AMT.sql
-- Procedure name: 			SMY.PROC_MTHLY_CR_CRD_AC_ACML_BAL_AMT
-- Source Table:				SOR.CC_AC_TXN_DTL,SOR.CC_AC_AR
-- Target Table: 				SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT
-- Project:             ZJ RCCB EDW
--
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.10.28
-- Origin Author:       Wang Youbing
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-10-28   Wang Youbing     Create SP File	
-- 2009-12-04   Xu Yan           Rename the history table	
-- 2010-01-19   Xu Yan           Added columns AMT_RCVD_For_LST_TM_OD, AMT_RCVD, OD_BAL_AMT ,DEP_BAL_CRD and related columns for accumlated amount
-- 2010-07-23   Fang Yihua       Update column RVL_LMT_AC_F
-- 2010-08-17   Fang Yihua       Modify the method of calendar days Calculating 
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*�����쳣����ʹ�ñ���*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
DECLARE SMY_DATE DATE;                                 --��ʱ���ڱ���
DECLARE SMY_RCOUNT INT;                                --DML������ü�¼��
DECLARE SMY_PROCNM VARCHAR(100);                        --�洢��������
DECLARE at_end SMALLINT DEFAULT 0;--
/*�����洢����ʹ�ñ���*/
DECLARE CUR_YEAR SMALLINT;                             --��
DECLARE CUR_MONTH SMALLINT;                            --��
DECLARE CUR_DAY INTEGER;                               --��
DECLARE YR_FIRST_DAY DATE;                             --�����1��1��
DECLARE QTR_FIRST_DAY DATE;                            --�����ȵ�1��
DECLARE MONTH_FIRST_DAY DATE;                          --���µ�1��
DECLARE NEXT_YR_FIRST_DAY DATE;                        --����1��1��
DECLARE NEXT_QTR_FIRST_DAY DATE;                       --�¼��ȵ�1��
DECLARE NEXT_MONTH_FIRST_DAY DATE;                     --���µ�1��
DECLARE MONTH_DAY SMALLINT;                            --��������
DECLARE YR_DAY SMALLINT;                               --��������
DECLARE QTR_DAY SMALLINT;                              --����������
DECLARE MAX_ACG_DT DATE;                               --���������
DECLARE DELETE_SQL VARCHAR(200);                       --ɾ����ʷ��̬SQL

/*1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC)��SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.*/
DECLARE CONTINUE HANDLER FOR NOT FOUND
SET at_end=1;  --
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
  ROLLBACK;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  RESIGNAL;--
END;--
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
END;--
/*������ֵ*/
SET SMY_PROCNM = 'PROC_MTHLY_CR_CRD_AC_ACML_BAL_AMT';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --ȡ����
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
SET NEXT_YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
SET YR_DAY=DAYS(SMY_DATE)-DAYS(YR_FIRST_DAY) + 1;--
IF CUR_MONTH IN (1,2,3) THEN 
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
   SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
       SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
           SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
            SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
END IF;--
SET QTR_DAY=DAYS(SMY_DATE)-DAYS(QTR_FIRST_DAY) + 1;--
SET MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01')));--
IF CUR_MONTH=12 THEN
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
ELSE
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH+1)),2)||'-01')));--
END IF;--
--------------------------------------------start on 20100817-------------------------------------------------------------
--SET MONTH_DAY=DAYS(NEXT_MONTH_FIRST_DAY)-DAYS(MONTH_FIRST_DAY);--
SET MONTH_DAY=DAYS(ACCOUNTING_DATE)-DAYS(MONTH_FIRST_DAY)+1;--
--------------------------------------------end on 20100817-------------------------------------------------------------
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT;--
SET DELETE_SQL='ALTER TABLE HIS.MTHLY_CR_CRD_AC_ACML_BAL_AMT ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*���ݻָ��뱸��*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT SELECT * FROM HIS.MTHLY_CR_CRD_AC_ACML_BAL_AMT;--
      COMMIT;--
   END IF;--
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;--
   INSERT INTO HIS.MTHLY_CR_CRD_AC_ACML_BAL_AMT SELECT * FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = 'DEFINE SESSION.TMP.';--

DECLARE GLOBAL TEMPORARY TABLE TMP(CC_AC_AR_ID CHAR(20),
                                   DNMN_CCY_ID CHAR(3),
                                   CDR_YR SMALLINT,
                                   CDR_MTH SMALLINT,
                                   ACG_DT DATE,
                                   NOD_In_MTH SMALLINT,
                                   NOCLD_In_MTH SMALLINT,
                                   NOCLD_IN_QTR SMALLINT,
                                   NOCLD_IN_YEAR SMALLINT,
                                   BAL_AMT DECIMAL(17,2),
                                   CUR_Day_CR_AMT DECIMAL(17,2),
                                   CUR_Day_DB_AMT DECIMAL(17,2),
                                   NBR_CUR_CR_TXN INTEGER,
                                   NBR_CUR_DB_TXN INTEGER,
                                   RVL_LMT_AC_F SMALLINT,
                                   BYND_LMT_F SMALLINT
                                  ,AMT_RCVD_For_LST_TM_OD DECIMAL(17,2)
                                  ,AMT_RCVD           DECIMAL(17,2)
                                  ,OD_BAL_AMT  NUMERIC(17,2)  
																	,DEP_BAL_CRD  NUMERIC(17,2)                                                        
                                  ) 
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(CC_AC_AR_ID);--

INSERT INTO SESSION.TMP
WITH TMP0 AS (
SELECT CC_AC_AR_ID,
       DNMN_CCY_ID,
       SUM(CASE WHEN DB_CR_IND=14280002 THEN TXN_AMT ELSE 0 END) CUR_Day_CR_AMT,            --���մ���������
       SUM(CASE WHEN DB_CR_IND=14280001 THEN TXN_AMT ELSE 0 END) CUR_Day_DB_AMT,            --���ս跽������
       SUM(CASE WHEN DB_CR_IND=14280002 THEN 1 ELSE 0 END) NBR_CUR_CR_TXN,                --���մ�����������
       SUM(CASE WHEN DB_CR_IND=14280001 THEN 1 ELSE 0 END) NBR_CUR_DB_TXN                 --���ս跽��������      
FROM SOR.CC_AC_TXN_DTL 
WHERE DEL_F=0                        --δɾ��
AND TXN_DT=ACCOUNTING_DATE 
AND DB_CR_IND in (14280001,14280002) --14280001,��  14280002,��  -1,Դ���д���Ϊ�ջ�ո�
AND TXN_TP_ID = 20460007 --��������
GROUP BY CC_AC_AR_ID,
         DNMN_CCY_ID
), T_REPYMT_AMT as (
   		SELECT 
   				 CC_AC_AR_ID
   				,DNMN_CCY_ID
    		  ,SUM(CASE WHEN TXN_DT < MONTH_FIRST_DAY  THEN REPYMT_AMT ELSE 0 END) AS AMT_RCVD_For_LST_TM_OD   			  
   			 	,SUM(REPYMT_AMT) AS AMT_RCVD   			 	
   		FROM SOR.CC_REPYMT_TXN_DTL    		
   		WHERE REPYMT_DT=ACCOUNTING_DATE 
   		      and DEL_F = 0   		
   		GROUP BY CC_AC_AR_ID , DNMN_CCY_ID	
)   		
SELECT A.CC_AC_AR_ID,
       A.DNMN_CCY_ID,
       CUR_YEAR CDR_YR,
       CUR_MONTH CDR_MTH,
       ACCOUNTING_DATE ACG_DT,
       (case when A.AR_LCS_TP_ID=20370007     --����
              then 1 
              else 0 end )NOD_In_MTH,                             --������Ч
       MONTH_DAY NOCLD_In_MTH,
       QTR_DAY NOCLD_IN_QTR,
       YR_DAY NOCLD_IN_YEAR,
       BAL_AMT,
       COALESCE(B.CUR_Day_CR_AMT,0) CUR_Day_CR_AMT,          --���մ���������
       COALESCE(B.CUR_Day_DB_AMT,0) CUR_Day_DB_AMT,          --���ս跽������
       COALESCE(B.NBR_CUR_CR_TXN,0) NBR_CUR_CR_TXN,          --���մ�����������
       COALESCE(B.NBR_CUR_DB_TXN,0) NBR_CUR_DB_TXN,           --���ս跽��������
----------------------------------------------- Start on 2010-07-23 ----------------------------------------
       --(CASE WHEN REPY_AMT_GRC_PRD-PREV_STA_BAL_AMT>0 THEN 0 ELSE 1 END)  RVL_LMT_AC_F,  --ѭ�������˻���־
       (CASE WHEN REPY_AMT_GRC_PRD+PREV_STA_BAL_AMT<0 and REPY_AMT_GRC_PRD-MN_REPYMT_AMT>=0 THEN 1 ELSE 0 END)  RVL_LMT_AC_F,  --ѭ�������˻���־
----------------------------------------------- End on 2010-07-23 ------------------------------------------
       (CASE WHEN BAL_AMT-FEE_AMT_DUE-ODUE_INT_AMT<=0 AND BAL_AMT+CR_LMT+TMP_CRED_LMT_AMT-RES_CRED_AMT-FEE_AMT_DUE-ODUE_INT_AMT<0 THEN 1 ELSE 0 END) BYND_LMT_F
       ,VALUE(T.AMT_RCVD_For_LST_TM_OD,0)                  --���ڻ����ϸ���֮ǰ�Ľ�� 
       ,VALUE(T.AMT_RCVD, 0)                               --�ѻ�����   
       ,(CASE WHEN BAL_AMT > 0 THEN 0 ELSE ABS(BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE) END)  AS OD_BAL_AMT --͸֧���
       ,(CASE WHEN BAL_AMT <=0 THEN 0 ELSE BAL_AMT END)  AS DEP_BAL_CRD  --���п�������            
FROM SOR.CC_AC_AR A
LEFT JOIN TMP0 B
ON A.CC_AC_AR_ID=B.CC_AC_AR_ID AND A.DNMN_CCY_ID=B.DNMN_CCY_ID
left join T_REPYMT_AMT T
on A.CC_AC_AR_ID = T.CC_AC_AR_ID AND A.DNMN_CCY_ID = T.DNMN_CCY_ID
WHERE 
--A.AR_LCS_TP_ID=20370007     --����
  (
    A.END_DT >= YR_FIRST_DAY    --ȥ������֮ǰ���ڵļ�¼
    OR
    A.END_DT = '1899-12-31'
  )
  AND 
     A.DEL_F=0                     --δɾ��
/*
GROUP BY A.CC_AC_AR_ID,
         A.DNMN_CCY_ID,
         (CASE WHEN REPY_AMT_GRC_PRD-PREV_STA_BAL_AMT>0 THEN 0 ELSE 1 END),
         (CASE WHEN BAL_AMT-FEE_AMT_DUE-ODUE_INT_AMT<=0 AND BAL_AMT+CR_LMT+TMP_CRED_LMT_AMT-RES_CRED_AMT-FEE_AMT_DUE-ODUE_INT_AMT<0 THEN 1 ELSE 0 END)
*/
;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = 'UPDATE SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT.';--

IF CUR_DAY=1 THEN                                                                     --�³�
   IF CUR_MONTH IN (4,7,10) THEN                                                      --���������
      INSERT INTO SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT(AC_AR_ID,
                                                   CCY,
                                                   CDR_YR,
                                                   CDR_MTH,
                                                   ACG_DT,
                                                   NOD_In_MTH,
                                                   NOCLD_In_MTH,
                                                   NOD_IN_QTR,
                                                   NOCLD_IN_QTR,
                                                   NOD_IN_YEAR,
                                                   NOCLD_IN_YEAR,
                                                   BAL_AMT,
                                                   MTD_ACML_BAL_AMT,
                                                   QTD_ACML_BAL_AMT,
                                                   YTD_ACML_BAL_AMT,
                                                   CUR_Day_CR_AMT,
                                                   CUR_Day_DB_AMT,
                                                   NBR_CUR_CR_TXN,
                                                   NBR_CUR_DB_TXN,
                                                   TOT_MTD_CR_AMT,
                                                   TOT_MTD_DB_AMT,
                                                   TOT_MTD_NBR_CR_TXN,
                                                   TOT_MTD_NBR_DB_TXN,
                                                   TOT_QTD_CR_AMT,
                                                   TOT_QTD_DB_AMT,
                                                   TOT_QTD_NBR_CR_TXN,
                                                   TOT_QTD_NBR_DB_TXN,
                                                   TOT_YTD_CR_AMT,
                                                   TOT_YTD_DB_AMT,
                                                   TOT_YTD_NBR_CR_TXN,
                                                   TOT_YTD_NBR_DB_TXN,
                                                   RVL_LMT_AC_F,
                                                   BYND_LMT_F
                                                  ,AMT_RCVD_For_LST_TM_OD  
																									,AMT_RCVD  
																									,TOT_MTD_AMT_RCVD_For_LST_TM_OD  
																									,TOT_MTD_AMT_RCVD  
																									,TOT_QTD_AMT_RCVD_For_LST_TM_OD  
																									,TOT_QTD_AMT_RCVD  
																									,TOT_YTD_AMT_RCVD_For_LST_TM_OD  
																									,TOT_YTD_AMT_RCVD
																									,OD_BAL_AMT
																									,DEP_BAL_CRD
																									,MTD_ACML_OD_BAL_AMT  
																									,MTD_ACML_DEP_BAL_AMT 
																									,QTD_ACML_OD_BAL_AMT  
																									,QTD_ACML_DEP_BAL_AMT 
																									,YTD_ACML_OD_BAL_AMT  
																									,YTD_ACML_DEP_BAL_AMT    
                                                   )
      SELECT S.CC_AC_AR_ID,
             S.DNMN_CCY_ID,
             S.CDR_YR,
             S.CDR_MTH,
             S.ACG_DT,
             S.NOD_In_MTH,
             S.NOCLD_In_MTH,
             S.NOD_In_MTH,
             S.NOCLD_IN_QTR,
             COALESCE(T.NOD_IN_YEAR+S.NOD_In_MTH,S.NOD_In_MTH),
             S.NOCLD_IN_YEAR,
             S.BAL_AMT,
             S.BAL_AMT,
             S.BAL_AMT,
             COALESCE(T.YTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT),
             S.CUR_Day_CR_AMT,
             S.CUR_Day_DB_AMT,
             S.NBR_CUR_CR_TXN,
             S.NBR_CUR_DB_TXN,
             S.CUR_Day_CR_AMT,
             S.CUR_Day_DB_AMT,
             S.NBR_CUR_CR_TXN,
             S.NBR_CUR_DB_TXN,
             S.CUR_Day_CR_AMT,
             S.CUR_Day_DB_AMT,
             S.NBR_CUR_CR_TXN,
             S.NBR_CUR_DB_TXN,
             COALESCE(T.TOT_YTD_CR_AMT+S.CUR_Day_CR_AMT,S.CUR_Day_CR_AMT),
             COALESCE(T.TOT_YTD_DB_AMT+S.CUR_Day_DB_AMT,S.CUR_Day_DB_AMT),
             COALESCE(T.TOT_YTD_NBR_CR_TXN+S.NBR_CUR_CR_TXN,S.NBR_CUR_CR_TXN),
             COALESCE(T.TOT_YTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN),
             COALESCE(T.RVL_LMT_AC_F,0),
             S.BYND_LMT_F
            ,S.AMT_RCVD_For_LST_TM_OD  
						,S.AMT_RCVD  
            ,S.AMT_RCVD_For_LST_TM_OD  
						,S.AMT_RCVD   
            ,S.AMT_RCVD_For_LST_TM_OD  
						,S.AMT_RCVD 
						,COALESCE(S.AMT_RCVD_For_LST_TM_OD + T.TOT_YTD_AMT_RCVD_For_LST_TM_OD , S.AMT_RCVD_For_LST_TM_OD )
						,COALESCE(S.AMT_RCVD  + T.TOT_YTD_AMT_RCVD ,S.AMT_RCVD)
            ,S.OD_BAL_AMT
						,S.DEP_BAL_CRD
            ,S.OD_BAL_AMT
						,S.DEP_BAL_CRD
            ,S.OD_BAL_AMT
						,S.DEP_BAL_CRD
						,COALESCE(S.OD_BAL_AMT + T.YTD_ACML_OD_BAL_AMT , S.OD_BAL_AMT)
						,COALESCE(S.DEP_BAL_CRD + T.YTD_ACML_DEP_BAL_AMT, S.DEP_BAL_CRD)  																			
      FROM SESSION.TMP S
      LEFT JOIN SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT T
      ON S.CC_AC_AR_ID=T.AC_AR_ID
      AND S.DNMN_CCY_ID=T.CCY
      AND S.CDR_YR=T.CDR_YR
      AND S.CDR_MTH-1 = T.CDR_MTH;--
            
      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
      COMMIT;--
   ELSEIF CUR_MONTH=1 THEN                                                              --���
          INSERT INTO SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT(AC_AR_ID,
                                                       CCY,
                                                       CDR_YR,
                                                       CDR_MTH,
                                                       ACG_DT,
                                                       NOD_In_MTH,
                                                       NOCLD_In_MTH,
                                                       NOD_IN_QTR,
                                                       NOCLD_IN_QTR,
                                                       NOD_IN_YEAR,
                                                       NOCLD_IN_YEAR,
                                                       BAL_AMT,
                                                       MTD_ACML_BAL_AMT,
                                                       QTD_ACML_BAL_AMT,
                                                       YTD_ACML_BAL_AMT,
                                                       CUR_Day_CR_AMT,
                                                       CUR_Day_DB_AMT,
                                                       NBR_CUR_CR_TXN,
                                                       NBR_CUR_DB_TXN,
                                                       TOT_MTD_CR_AMT,
                                                       TOT_MTD_DB_AMT,
                                                       TOT_MTD_NBR_CR_TXN,
                                                       TOT_MTD_NBR_DB_TXN,
                                                       TOT_QTD_CR_AMT,
                                                       TOT_QTD_DB_AMT,
                                                       TOT_QTD_NBR_CR_TXN,
                                                       TOT_QTD_NBR_DB_TXN,
                                                       TOT_YTD_CR_AMT,
                                                       TOT_YTD_DB_AMT,
                                                       TOT_YTD_NBR_CR_TXN,
                                                       TOT_YTD_NBR_DB_TXN,
                                                       RVL_LMT_AC_F,
                                                       BYND_LMT_F
                                                      ,AMT_RCVD_For_LST_TM_OD  
																											,AMT_RCVD  
																											,TOT_MTD_AMT_RCVD_For_LST_TM_OD  
																											,TOT_MTD_AMT_RCVD  
																											,TOT_QTD_AMT_RCVD_For_LST_TM_OD  
																											,TOT_QTD_AMT_RCVD  
																											,TOT_YTD_AMT_RCVD_For_LST_TM_OD  
																											,TOT_YTD_AMT_RCVD
																				              ,OD_BAL_AMT
																											,DEP_BAL_CRD
																											,MTD_ACML_OD_BAL_AMT  
																											,MTD_ACML_DEP_BAL_AMT 
																											,QTD_ACML_OD_BAL_AMT  
																											,QTD_ACML_DEP_BAL_AMT 
																											,YTD_ACML_OD_BAL_AMT  
																											,YTD_ACML_DEP_BAL_AMT    							
																											
                                                       )
          SELECT S.CC_AC_AR_ID,
                 S.DNMN_CCY_ID,
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.NOD_In_MTH,
                 S.NOCLD_In_MTH,
                 S.NOD_In_MTH,
                 S.NOCLD_IN_QTR,
                 S.NOD_In_MTH,
                 S.NOCLD_IN_YEAR,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 COALESCE(T.RVL_LMT_AC_F,0),
                 S.BYND_LMT_F
                ,S.AMT_RCVD_For_LST_TM_OD  
								,S.AMT_RCVD  
                ,S.AMT_RCVD_For_LST_TM_OD  
								,S.AMT_RCVD 
                ,S.AMT_RCVD_For_LST_TM_OD  
								,S.AMT_RCVD
                ,S.AMT_RCVD_For_LST_TM_OD  
								,S.AMT_RCVD
		            ,S.OD_BAL_AMT
								,S.DEP_BAL_CRD
		            ,S.OD_BAL_AMT
								,S.DEP_BAL_CRD 
		            ,S.OD_BAL_AMT
								,S.DEP_BAL_CRD
		            ,S.OD_BAL_AMT
								,S.DEP_BAL_CRD 																                
          FROM SESSION.TMP S
          LEFT JOIN SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT T
          ON S.CC_AC_AR_ID=T.AC_AR_ID
          AND S.DNMN_CCY_ID=T.CCY
          AND S.CDR_YR=T.CDR_YR
          AND S.CDR_MTH-1 = T.CDR_MTH;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
        ELSE                                                                             --�³��Ǽ��������
          INSERT INTO SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT(AC_AR_ID,
                                                       CCY,
                                                       CDR_YR,
                                                       CDR_MTH,
                                                       ACG_DT,
                                                       NOD_In_MTH,
                                                       NOCLD_In_MTH,
                                                       NOD_IN_QTR,
                                                       NOCLD_IN_QTR,
                                                       NOD_IN_YEAR,
                                                       NOCLD_IN_YEAR,
                                                       BAL_AMT,
                                                       MTD_ACML_BAL_AMT,
                                                       QTD_ACML_BAL_AMT,
                                                       YTD_ACML_BAL_AMT,
                                                       CUR_Day_CR_AMT,
                                                       CUR_Day_DB_AMT,
                                                       NBR_CUR_CR_TXN,
                                                       NBR_CUR_DB_TXN,
                                                       TOT_MTD_CR_AMT,
                                                       TOT_MTD_DB_AMT,
                                                       TOT_MTD_NBR_CR_TXN,
                                                       TOT_MTD_NBR_DB_TXN,
                                                       TOT_QTD_CR_AMT,
                                                       TOT_QTD_DB_AMT,
                                                       TOT_QTD_NBR_CR_TXN,
                                                       TOT_QTD_NBR_DB_TXN,
                                                       TOT_YTD_CR_AMT,
                                                       TOT_YTD_DB_AMT,
                                                       TOT_YTD_NBR_CR_TXN,
                                                       TOT_YTD_NBR_DB_TXN,
                                                       RVL_LMT_AC_F,
                                                       BYND_LMT_F
                                                      ,AMT_RCVD_For_LST_TM_OD  
																											,AMT_RCVD  
																											,TOT_MTD_AMT_RCVD_For_LST_TM_OD  
																											,TOT_MTD_AMT_RCVD  
																											,TOT_QTD_AMT_RCVD_For_LST_TM_OD  
																											,TOT_QTD_AMT_RCVD  
																											,TOT_YTD_AMT_RCVD_For_LST_TM_OD  
																											,TOT_YTD_AMT_RCVD
																				              ,OD_BAL_AMT
																											,DEP_BAL_CRD
																											,MTD_ACML_OD_BAL_AMT  
																											,MTD_ACML_DEP_BAL_AMT 
																											,QTD_ACML_OD_BAL_AMT  
																											,QTD_ACML_DEP_BAL_AMT 
																											,YTD_ACML_OD_BAL_AMT  
																											,YTD_ACML_DEP_BAL_AMT    							
																											
                                                       )
          SELECT S.CC_AC_AR_ID,
                 S.DNMN_CCY_ID,
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.NOD_In_MTH,
                 S.NOCLD_In_MTH,
                 COALESCE(T.NOD_IN_QTR+S.NOD_In_MTH,S.NOD_In_MTH),
                 S.NOCLD_IN_QTR,
                 COALESCE(T.NOD_IN_YEAR+S.NOD_In_MTH,S.NOD_In_MTH),
                 S.NOCLD_IN_YEAR,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 COALESCE(T.QTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT),
                 COALESCE(T.YTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT),
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 COALESCE(T.TOT_QTD_CR_AMT+S.CUR_Day_CR_AMT,S.CUR_Day_CR_AMT),
                 COALESCE(T.TOT_QTD_DB_AMT+S.CUR_Day_DB_AMT,S.CUR_Day_DB_AMT),
                 COALESCE(T.TOT_QTD_NBR_CR_TXN+S.NBR_CUR_CR_TXN,S.NBR_CUR_CR_TXN),
                 COALESCE(T.TOT_QTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN),
                 COALESCE(T.TOT_YTD_CR_AMT+S.CUR_Day_CR_AMT,S.CUR_Day_CR_AMT),
                 COALESCE(T.TOT_YTD_DB_AMT+S.CUR_Day_DB_AMT,S.CUR_Day_DB_AMT),
                 COALESCE(T.TOT_YTD_NBR_CR_TXN+S.NBR_CUR_CR_TXN,S.NBR_CUR_CR_TXN),
                 COALESCE(T.TOT_YTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN),
                 COALESCE(T.RVL_LMT_AC_F,0),
                 S.BYND_LMT_F
								,S.AMT_RCVD_For_LST_TM_OD  
								,S.AMT_RCVD  
								,S.AMT_RCVD_For_LST_TM_OD  
								,S.AMT_RCVD  
								,COALESCE(S.AMT_RCVD_For_LST_TM_OD + T.TOT_QTD_AMT_RCVD_For_LST_TM_OD, S.AMT_RCVD_For_LST_TM_OD)
								,COALESCE(S.AMT_RCVD + T.TOT_QTD_AMT_RCVD, S.AMT_RCVD)  
								,COALESCE(S.AMT_RCVD_For_LST_TM_OD + T.TOT_YTD_AMT_RCVD_For_LST_TM_OD , S.AMT_RCVD_For_LST_TM_OD )
								,COALESCE(S.AMT_RCVD  + T.TOT_YTD_AMT_RCVD ,S.AMT_RCVD)
		            ,S.OD_BAL_AMT
								,S.DEP_BAL_CRD
		            ,S.OD_BAL_AMT
								,S.DEP_BAL_CRD
								,COALESCE(S.OD_BAL_AMT  + T.QTD_ACML_OD_BAL_AMT, S.OD_BAL_AMT)
								,COALESCE(S.DEP_BAL_CRD + T.QTD_ACML_DEP_BAL_AMT, S.DEP_BAL_CRD)
								,COALESCE(S.OD_BAL_AMT + T.YTD_ACML_OD_BAL_AMT , S.OD_BAL_AMT)
								,COALESCE(S.DEP_BAL_CRD + T.YTD_ACML_DEP_BAL_AMT, S.DEP_BAL_CRD)  								
          FROM SESSION.TMP S
          LEFT JOIN SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT T
          ON S.CC_AC_AR_ID=T.AC_AR_ID
          AND S.DNMN_CCY_ID=T.CCY
          AND S.CDR_YR=T.CDR_YR
          AND S.CDR_MTH-1 = T.CDR_MTH;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;                      --
   END IF;--
ELSE                                                                                          ---���³�
  MERGE INTO SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT S
  USING SESSION.TMP T
  ON S.AC_AR_ID=T.CC_AC_AR_ID 
  AND S.CCY=T.DNMN_CCY_ID
  AND S.CDR_YR=T.CDR_YR
  AND S.CDR_MTH=T.CDR_MTH
  WHEN MATCHED 
  THEN UPDATE SET(AC_AR_ID,
                  CCY,
                  CDR_YR,
                  CDR_MTH,
                  ACG_DT,
                  NOD_In_MTH,
                  NOCLD_In_MTH,
                  NOD_IN_QTR,
                  NOCLD_IN_QTR,
                  NOD_IN_YEAR,
                  NOCLD_IN_YEAR,
                  BAL_AMT,
                  MTD_ACML_BAL_AMT,
                  QTD_ACML_BAL_AMT,
                  YTD_ACML_BAL_AMT,
                  CUR_Day_CR_AMT,
                  CUR_Day_DB_AMT,
                  NBR_CUR_CR_TXN,
                  NBR_CUR_DB_TXN,
                  TOT_MTD_CR_AMT,
                  TOT_MTD_DB_AMT,
                  TOT_MTD_NBR_CR_TXN,
                  TOT_MTD_NBR_DB_TXN,
                  TOT_QTD_CR_AMT,
                  TOT_QTD_DB_AMT,
                  TOT_QTD_NBR_CR_TXN,
                  TOT_QTD_NBR_DB_TXN,
                  TOT_YTD_CR_AMT,
                  TOT_YTD_DB_AMT,
                  TOT_YTD_NBR_CR_TXN,
                  TOT_YTD_NBR_DB_TXN,
                  RVL_LMT_AC_F,
                  BYND_LMT_F
		              ,AMT_RCVD_For_LST_TM_OD  
									,AMT_RCVD  
									,TOT_MTD_AMT_RCVD_For_LST_TM_OD  
									,TOT_MTD_AMT_RCVD  
									,TOT_QTD_AMT_RCVD_For_LST_TM_OD  
									,TOT_QTD_AMT_RCVD  
									,TOT_YTD_AMT_RCVD_For_LST_TM_OD  
									,TOT_YTD_AMT_RCVD
		              ,OD_BAL_AMT
									,DEP_BAL_CRD
									,MTD_ACML_OD_BAL_AMT  
									,MTD_ACML_DEP_BAL_AMT 
									,QTD_ACML_OD_BAL_AMT  
									,QTD_ACML_DEP_BAL_AMT 
									,YTD_ACML_OD_BAL_AMT  
									,YTD_ACML_DEP_BAL_AMT    																                  
                  )
                =(T.CC_AC_AR_ID,
                  T.DNMN_CCY_ID,
                  T.CDR_YR,
                  T.CDR_MTH,
                  T.ACG_DT,
                  S.NOD_In_MTH+T.NOD_In_MTH,
                  T.NOCLD_In_MTH,
                  S.NOD_IN_QTR+T.NOD_In_MTH,
                  T.NOCLD_IN_QTR,
                  S.NOD_IN_YEAR+T.NOD_In_MTH,
                  T.NOCLD_IN_YEAR,                        
                  T.BAL_AMT,
                  S.MTD_ACML_BAL_AMT+T.BAL_AMT,
                  S.QTD_ACML_BAL_AMT+T.BAL_AMT,
                  S.YTD_ACML_BAL_AMT+T.BAL_AMT,
                  T.CUR_Day_CR_AMT,
                  T.CUR_Day_DB_AMT,
                  T.NBR_CUR_CR_TXN,
                  T.NBR_CUR_DB_TXN,
                  S.TOT_MTD_CR_AMT+T.CUR_Day_CR_AMT,
                  S.TOT_MTD_DB_AMT+T.CUR_Day_DB_AMT,
                  S.TOT_MTD_NBR_CR_TXN+T.NBR_CUR_CR_TXN,
                  S.TOT_MTD_NBR_DB_TXN+T.NBR_CUR_DB_TXN,
                  S.TOT_QTD_CR_AMT+T.CUR_Day_CR_AMT,
                  S.TOT_QTD_DB_AMT+T.CUR_Day_DB_AMT,
                  S.TOT_QTD_NBR_CR_TXN+T.NBR_CUR_CR_TXN,
                  S.TOT_QTD_NBR_DB_TXN+T.NBR_CUR_DB_TXN,
                  S.TOT_YTD_CR_AMT+T.CUR_Day_CR_AMT,
                  S.TOT_YTD_DB_AMT+T.CUR_Day_DB_AMT,
                  S.TOT_YTD_NBR_CR_TXN+T.NBR_CUR_CR_TXN,
                  S.TOT_YTD_NBR_DB_TXN+T.NBR_CUR_DB_TXN,
                  (CASE WHEN CUR_DAY=25 THEN T.RVL_LMT_AC_F ELSE S.RVL_LMT_AC_F END),
                  T.BYND_LMT_F
									,T.AMT_RCVD_For_LST_TM_OD  
									,T.AMT_RCVD  
									,S.TOT_MTD_AMT_RCVD_For_LST_TM_OD + T.AMT_RCVD_For_LST_TM_OD
									,S.TOT_MTD_AMT_RCVD  + T.AMT_RCVD
									,S.TOT_QTD_AMT_RCVD_For_LST_TM_OD + T.AMT_RCVD_For_LST_TM_OD
									,S.TOT_QTD_AMT_RCVD + T.AMT_RCVD
									,S.TOT_YTD_AMT_RCVD_For_LST_TM_OD + T.AMT_RCVD_For_LST_TM_OD
									,S.TOT_YTD_AMT_RCVD  + T.AMT_RCVD
			            ,T.OD_BAL_AMT
									,T.DEP_BAL_CRD
									,S.MTD_ACML_OD_BAL_AMT + T.OD_BAL_AMT
									,S.MTD_ACML_DEP_BAL_AMT + T.DEP_BAL_CRD
									,S.QTD_ACML_OD_BAL_AMT  + T.OD_BAL_AMT
									,S.QTD_ACML_DEP_BAL_AMT + T.DEP_BAL_CRD
									,S.YTD_ACML_OD_BAL_AMT + T.OD_BAL_AMT 
									,S.YTD_ACML_DEP_BAL_AMT + T.DEP_BAL_CRD
                  )
  WHEN NOT MATCHED
  THEN INSERT(AC_AR_ID,
              CCY,
              CDR_YR,
              CDR_MTH,
              ACG_DT,
              NOD_In_MTH,
              NOCLD_In_MTH,
              NOD_IN_QTR,
              NOCLD_IN_QTR,
              NOD_IN_YEAR,
              NOCLD_IN_YEAR,
              BAL_AMT,
              MTD_ACML_BAL_AMT,
              QTD_ACML_BAL_AMT,
              YTD_ACML_BAL_AMT,
              CUR_Day_CR_AMT,
              CUR_Day_DB_AMT,
              NBR_CUR_CR_TXN,
              NBR_CUR_DB_TXN,
              TOT_MTD_CR_AMT,
              TOT_MTD_DB_AMT,
              TOT_MTD_NBR_CR_TXN,
              TOT_MTD_NBR_DB_TXN,
              TOT_QTD_CR_AMT,
              TOT_QTD_DB_AMT,
              TOT_QTD_NBR_CR_TXN,
              TOT_QTD_NBR_DB_TXN,
              TOT_YTD_CR_AMT,
              TOT_YTD_DB_AMT,
              TOT_YTD_NBR_CR_TXN,
              TOT_YTD_NBR_DB_TXN,
              RVL_LMT_AC_F,
              BYND_LMT_F
              ,AMT_RCVD_For_LST_TM_OD  
							,AMT_RCVD  
							,TOT_MTD_AMT_RCVD_For_LST_TM_OD  
							,TOT_MTD_AMT_RCVD  
							,TOT_QTD_AMT_RCVD_For_LST_TM_OD  
							,TOT_QTD_AMT_RCVD  
							,TOT_YTD_AMT_RCVD_For_LST_TM_OD  
							,TOT_YTD_AMT_RCVD  
              ,OD_BAL_AMT
							,DEP_BAL_CRD
							,MTD_ACML_OD_BAL_AMT  
							,MTD_ACML_DEP_BAL_AMT 
							,QTD_ACML_OD_BAL_AMT  
							,QTD_ACML_DEP_BAL_AMT 
							,YTD_ACML_OD_BAL_AMT  
							,YTD_ACML_DEP_BAL_AMT    							
              )
       VALUES(T.CC_AC_AR_ID,
              T.DNMN_CCY_ID,
              T.CDR_YR,
              T.CDR_MTH,
              T.ACG_DT,
              T.NOD_In_MTH,
              T.NOCLD_In_MTH,
              T.NOD_In_MTH,
              T.NOCLD_IN_QTR,
              T.NOD_In_MTH,
              T.NOCLD_IN_YEAR,
              T.BAL_AMT,
              T.BAL_AMT,
              T.BAL_AMT,
              T.BAL_AMT,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN,
              0,
              T.BYND_LMT_F
	            ,T.AMT_RCVD_For_LST_TM_OD  
							,T.AMT_RCVD  
	            ,T.AMT_RCVD_For_LST_TM_OD  
							,T.AMT_RCVD
	            ,T.AMT_RCVD_For_LST_TM_OD  
							,T.AMT_RCVD
	            ,T.AMT_RCVD_For_LST_TM_OD  
							,T.AMT_RCVD
              ,T.OD_BAL_AMT
							,T.DEP_BAL_CRD
              ,T.OD_BAL_AMT
							,T.DEP_BAL_CRD
              ,T.OD_BAL_AMT
							,T.DEP_BAL_CRD
              ,T.OD_BAL_AMT
							,T.DEP_BAL_CRD   							
              );--
  
  GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);  --
  COMMIT; --
END IF;--

SET SMY_STEPNUM=6 ;--
SET SMY_STEPDESC = '�洢���̽���!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
END
@