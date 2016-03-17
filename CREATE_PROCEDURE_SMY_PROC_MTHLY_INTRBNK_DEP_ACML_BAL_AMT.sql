CREATE PROCEDURE SMY.PROC_MTHLY_INTRBNK_DEP_ACML_BAL_AMT(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_MTHLY_INTRBNK_DEP_ACML_BAL_AMT.sql
-- Procedure name: 			SMY.PROC_MTHLY_INTRBNK_DEP_ACML_BAL_AMT
-- Source Table:				SOR.INTRBNK_DEP_SUB_AR,SOR.INTRBNK_DEP_AR_TXN_DTL
-- Target Table: 				SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT
-- Project:             ZJ RCCB EDW
--
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.11
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
-- 2010-01-19   Xu Yan           Updated the conditional statement and NOCLD value
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
DECLARE at_end SMALLINT DEFAULT 0;
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
SET at_end=1;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;
  ROLLBACK;
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
  COMMIT;
  RESIGNAL;
END;
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
  COMMIT;
END;
/*������ֵ*/
SET SMY_PROCNM = 'PROC_MTHLY_INTRBNK_DEP_ACML_BAL_AMT';
SET SMY_DATE=ACCOUNTING_DATE;
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --ȡ����
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');
SET NEXT_YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');
SET YR_DAY=Dayofyear(SMY_DATE);
IF CUR_MONTH IN (1,2,3) THEN 
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');
   SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');
       SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');
           SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');
            SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');
END IF;
SET QTR_DAY=DAYS(SMY_DATE)-DAYS(QTR_FIRST_DAY) + 1;
SET MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01')));
IF CUR_MONTH=12 THEN
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');
ELSE
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH+1)),2)||'-01')));
END IF;
SET MONTH_DAY=CUR_DAY;
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT;
SET DELETE_SQL='ALTER TABLE HIS.MTHLY_INTRBNK_DEP_ACML_BAL_AMT ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;
COMMIT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
COMMIT;

/*���ݻָ��뱸��*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT SELECT * FROM HIS.MTHLY_INTRBNK_DEP_ACML_BAL_AMT;
      COMMIT;
   END IF;
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;
   INSERT INTO HIS.MTHLY_INTRBNK_DEP_ACML_BAL_AMT SELECT * FROM SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
END IF;

SET SMY_STEPNUM = SMY_STEPNUM+1;
SET SMY_STEPDESC = 'DEFINE SESSION.TMP.';

DECLARE GLOBAL TEMPORARY TABLE TMP(INTRBNK_DEP_AR_ID CHAR(20),
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
                                   NBR_CUR_DB_TXN INTEGER) 
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(INTRBNK_DEP_AR_ID,DNMN_CCY_ID,CDR_YR,CDR_MTH);
INSERT INTO SESSION.TMP
WITH TMP0 AS (
Select INTRBNK_DEP_AR_ID,
       DNMN_CCY_ID,
       SUM(CASE WHEN DB_CR_IND=14280002 THEN TXN_AMT ELSE 0 END) CUR_Day_CR_AMT,            --���մ���������
       SUM(CASE WHEN DB_CR_IND=14280001 THEN TXN_AMT ELSE 0 END) CUR_Day_DB_AMT,            --���ս跽������
       SUM(CASE WHEN DB_CR_IND=14280002 THEN 1 ELSE 0 END) NBR_CUR_CR_TXN,                --���մ�����������
       SUM(CASE WHEN DB_CR_IND=14280001 THEN 1 ELSE 0 END) NBR_CUR_DB_TXN                 --���ս跽��������
from SOR.INTRBNK_DEP_AR_TXN_DTL 
WHERE DEL_F=0                        --δɾ��
AND TXN_DT=ACCOUNTING_DATE 
AND TXN_TP_ID = 20460007 --��������
AND DB_CR_IND in (14280001,14280002)  --14280001,��  14280002,��  -1Դ���д���Ϊ�ջ�ո�
GROUP BY INTRBNK_DEP_AR_ID,
         DNMN_CCY_ID
), TMP_INR AS (
     SELECT  A.INTRBNK_DEP_AR_ID
            ,A.DNMN_CCY_ID
            ,max(case when A.AR_LCS_TP_ID= 20370007     --���� 
                           then 1 else 0 end )NOD_In_MTH
            ,SUM(BAL_AMT) BAL_AMT
     FROM SOR.INTRBNK_DEP_SUB_AR A
     WHERE 
			     (A.STL_DT >= YR_FIRST_DAY
			      OR     
			      A.STL_DT = '1899-12-31'
			     )
						AND A.DEL_F=0   
		GROUP BY A.INTRBNK_DEP_AR_ID,A.DNMN_CCY_ID
)
SELECT A.INTRBNK_DEP_AR_ID,
       A.DNMN_CCY_ID,
       CUR_YEAR CDR_YR,
       CUR_MONTH CDR_MTH,
       ACCOUNTING_DATE ACG_DT,
       NOD_In_MTH,                             --������Ч
       MONTH_DAY NOCLD_In_MTH,
       QTR_DAY NOCLD_IN_QTR,
       YR_DAY NOCLD_IN_YEAR,
       BAL_AMT,
       COALESCE(B.CUR_Day_CR_AMT,0) CUR_Day_CR_AMT,          --���մ���������
       COALESCE(B.CUR_Day_DB_AMT,0) CUR_Day_DB_AMT,          --���ս跽������
       COALESCE(B.NBR_CUR_CR_TXN,0) NBR_CUR_CR_TXN,          --���մ�����������
       COALESCE(B.NBR_CUR_DB_TXN,0) NBR_CUR_DB_TXN           --���ս跽��������
FROM TMP_INR A
LEFT JOIN TMP0 B
ON A.INTRBNK_DEP_AR_ID=B.INTRBNK_DEP_AR_ID AND A.DNMN_CCY_ID=B.DNMN_CCY_ID
---------------Start on 20100119--------------------------------
--WHERE A.AR_LCS_TP_ID=20370007     --����
---------------End on 20100119--------------------------------

;

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
COMMIT;

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;
SET SMY_STEPDESC = 'UPDATE SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT.';

IF CUR_DAY=1 THEN                                                                     --�³�
   IF CUR_MONTH IN (4,7,10) THEN                                                      --���������
      INSERT INTO SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT(AC_AR_ID,
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
                                                 TOT_YTD_NBR_DB_TXN)
      SELECT S.INTRBNK_DEP_AR_ID,
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
             COALESCE(T.TOT_YTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN)
      FROM SESSION.TMP S
      LEFT JOIN SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT T
      ON S.INTRBNK_DEP_AR_ID=T.AC_AR_ID
      AND S.DNMN_CCY_ID=T.CCY
      AND S.CDR_YR=T.CDR_YR
      AND S.CDR_MTH-1=T.CDR_MTH;
            
      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
      COMMIT;
   ELSEIF CUR_MONTH=1 THEN                                                              --���
          INSERT INTO SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT(AC_AR_ID,
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
                                                     TOT_YTD_NBR_DB_TXN)
          SELECT S.INTRBNK_DEP_AR_ID,
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
                 S.NBR_CUR_DB_TXN
          FROM SESSION.TMP S;
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
          COMMIT;
        ELSE                                                                             --�³��Ǽ��������
          INSERT INTO SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT(AC_AR_ID,
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
                                                     TOT_YTD_NBR_DB_TXN)
          SELECT S.INTRBNK_DEP_AR_ID,
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
                 COALESCE(T.TOT_YTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN)
          FROM SESSION.TMP S
          LEFT JOIN SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT T
          ON S.INTRBNK_DEP_AR_ID=T.AC_AR_ID
          AND S.DNMN_CCY_ID=T.CCY
          AND S.CDR_YR=T.CDR_YR
          AND S.CDR_MTH-1=T.CDR_MTH;
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP); 
          COMMIT;                     
   END IF;
ELSE                                                                                          ---���³�
  MERGE INTO SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT S
  USING SESSION.TMP T
  ON S.AC_AR_ID=T.INTRBNK_DEP_AR_ID 
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
                  TOT_YTD_NBR_DB_TXN)
                =(T.INTRBNK_DEP_AR_ID,
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
                  S.TOT_YTD_NBR_DB_TXN+T.NBR_CUR_DB_TXN)
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
              TOT_YTD_NBR_DB_TXN)
       VALUES(T.INTRBNK_DEP_AR_ID,
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
              T.NBR_CUR_DB_TXN);
                   
  GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
  COMMIT;
END IF;

SET SMY_STEPNUM=6 ;
SET SMY_STEPDESC = '�洢���̽���!';

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);

END@