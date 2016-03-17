CREATE PROCEDURE SMY.PROC_ATM_TXN_VOL_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_ATM_TXN_VOL_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_ATM_TXN_VOL_MTHLY_SMY
-- Source Table:				SOR.STMT_DEP_AC_RGST
-- Target Table: 				SMY.ATM_TXN_VOL_MTHLY_SMY
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
-- 2009-10-28   Wang Youbing    Create SP File
-- 2009-12-04   Xu Yan       		Rename the history table		
-- 2009-12-25   Xu Yan       		Updated OU_ID definition
-- 2010-01-14   Xu Yan       		Filtered in DEL_F=0 only for SOR.STMT_DEP_AC_RGST
-- 2010-06-03   Fang Yihua      Add a field ZJRCU_OWN_ATM_F��    		
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
SET at_end=1;--
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
SET SMY_PROCNM = 'PROC_ATM_TXN_VOL_MTHLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --ȡ����
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
SET NEXT_YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
SET YR_DAY=DAYS(NEXT_YR_FIRST_DAY)-DAYS(YR_FIRST_DAY);--
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
SET QTR_DAY=DAYS(NEXT_QTR_FIRST_DAY)-DAYS(QTR_FIRST_DAY);--
SET MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01')));--
IF CUR_MONTH=12 THEN
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
ELSE
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH+1)),2)||'-01')));--
END IF;--
SET MONTH_DAY=DAYS(NEXT_MONTH_FIRST_DAY)-DAYS(MONTH_FIRST_DAY);--
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.ATM_TXN_VOL_MTHLY_SMY;--
SET DELETE_SQL='ALTER TABLE HIS.ATM_TXN_VOL_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*���ݻָ��뱸��*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.ATM_TXN_VOL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.ATM_TXN_VOL_MTHLY_SMY SELECT * FROM HIS.ATM_TXN_VOL_MTHLY_SMY;--
      COMMIT;--
   END IF;--
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;--
   INSERT INTO HIS.ATM_TXN_VOL_MTHLY_SMY SELECT * FROM SMY.ATM_TXN_VOL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '����ϵͳ��ʱ��,����ά�Ȼ���,��ʱ��ŵ��ս�������.';--

DECLARE GLOBAL TEMPORARY TABLE TMP(ATM_SEQ_NO CHAR(20),
                                   CCY CHAR(3),
                                   CASH_TFR_IND INTEGER,
                                   DB_CR_IND INTEGER,
                                   AC_OU_ID CHARACTER(18),
                                   RCU_CRD_F SMALLINT,
--------------------------------------- Start on 2010-06-03 ----------------------------------
                                   ZJRCU_OWN_ATM_F SMALLINT,
--------------------------------------- End on 2010-06-03 ------------------------------------
                                   CDR_YR SMALLINT,
                                   CDR_MTH SMALLINT,
                                   ACG_DT DATE,
                                   OU_ID CHARACTER(18),
                                   CUR_Day_NBR_TXN INTEGER,
                                   CUR_Day_AMT DECIMAL(17,2))
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K  
 PARTITIONING KEY(ATM_SEQ_NO);--
INSERT INTO SESSION.TMP
SELECT A.TML_SEQ_NO AS ATM_SEQ_NO,
       A.DNMN_CCY_ID AS CCY,
       A.CASH_TFR_IND,
       A.DB_CR_IND,
       A.AC_RPRG_OU_IP_ID AS AC_OU_ID,
      (CASE WHEN A.AC_RPRG_OU_IP_ID<>'999000' THEN 1 ELSE 0 END) AS RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
       ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
       CUR_YEAR CDR_YR,
       CUR_MONTH CDR_MTH,
       ACCOUNTING_DATE ACG_DT,
       --COALESCE(B.OU_IP_ID,' ') AS OU_ID,
       COALESCE(substr(B.OU_IP_ID,3,6),' ') AS OU_ID,
       COUNT(1) AS CUR_Day_NBR_TXN,
       SUM(TXN_AMT) AS CUR_Day_AMT
--------------------------------------- Start on 2010-06-03----------------------------------
FROM SOR.STMT_DEP_AC_RGST A
LEFT JOIN SOR.ATM_TM_INFO B
ON A.TML_SEQ_NO=B.ATM_TM_SEQ_NO
--WHERE A.ZJRCU_OWN_ATM_F=1            --����ATM
where A.CNL_TP=21690003                --��������ΪATM
AND (ZJRCU_OWN_ATM_F=1 or (ZJRCU_OWN_ATM_F=0 and substr(MCHNT_AC_AR_ID,2,1)<>'1'))   --���е�ATM�������е�ATM�����Ҳ��������Ѳ���
--------------------------------------- End on 2010-06-03------------------------------------
AND TXN_DT=ACCOUNTING_DATE
-----------------Start on 20100114-----------------------
And A.DEL_F <> 1                     --δɾ��
-----------------End on 20100114-----------------------
GROUP BY A.TML_SEQ_NO,
         A.DNMN_CCY_ID,
         A.CASH_TFR_IND, 
         A.DB_CR_IND,
         A.AC_RPRG_OU_IP_ID,
         (CASE WHEN A.AC_RPRG_OU_IP_ID<>'999000' THEN 1 ELSE 0 END),
--------------------------------------- Start on 2010-06-03----------------------------------
         ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
         COALESCE(substr(B.OU_IP_ID,3,6),' ')
         --COALESCE(B.OU_IP_ID,' ');--
;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = 'ʹ�õ��ս������ݸ��»��ܱ�.';--

IF CUR_DAY=1 THEN                                                                     --�³�
   IF CUR_MONTH=1 THEN                                                              --���
         INSERT INTO SMY.ATM_TXN_VOL_MTHLY_SMY(ATM_SEQ_NO,
                                                CCY,
                                                CASH_TFR_IND,
                                                DB_CR_IND,
                                                AC_OU_ID,
                                                RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                                                ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
                                                CDR_YR,
                                                CDR_MTH,
                                                ACG_DT,
                                                OU_ID,
                                                TOT_MTD_NBR_TXN,
                                                TOT_MTD_AMT,
                                                TOT_QTD_NBR_TXN,
                                                TOT_QTD_AMT,
                                                TOT_YTD_NBR_TXN,
                                                TOT_YTD_AMT)
          SELECT S.ATM_SEQ_NO,
                 S.CCY,
                 S.CASH_TFR_IND,
                 S.DB_CR_IND,
                 S.AC_OU_ID,
                 S.RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                 S.ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.OU_ID,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT
          FROM SESSION.TMP S;--

      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
      COMMIT;--
   ELSE  -- CUR_MONTH<>1 AND CUR_DAY=1
   	---------------------------------------Start on 20100114-------------------------------------------------------
   	INSERT INTO SMY.ATM_TXN_VOL_MTHLY_SMY(
   					ATM_SEQ_NO,
            CCY,
            CASH_TFR_IND,
            DB_CR_IND,
            AC_OU_ID,
            RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
            ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
            CDR_YR,
            CDR_MTH,
            ACG_DT,
            OU_ID,
            TOT_MTD_NBR_TXN,
            TOT_MTD_AMT,
            TOT_QTD_NBR_TXN,
            TOT_QTD_AMT,
            TOT_YTD_NBR_TXN,
            TOT_YTD_AMT
     )SELECT 
      			 S.ATM_SEQ_NO
	          ,S.CCY
	          ,S.CASH_TFR_IND
	          ,S.DB_CR_IND
	          ,S.AC_OU_ID
	          ,S.RCU_CRD_F
--------------------------------------- Start on 2010-06-03----------------------------------
            ,S.ZJRCU_OWN_ATM_F
--------------------------------------- End on 2010-06-03------------------------------------
	          ,CUR_YEAR
	          ,CUR_MONTH
	          ,SMY_DATE
	          ,S.OU_ID
	          ,0                --TOT_MTD_NBR_TXN
	          ,0                --TOT_MTD_AMT
	          ,case when CUR_MONTH in (4,7,10) then 0 else S.TOT_QTD_NBR_TXN end  --TOT_QTD_NBR_TXN
	          ,case when CUR_MONTH in (4,7,10) then 0 else S.TOT_QTD_AMT end		  --TOT_QTD_AMT        
	          ,S.TOT_YTD_NBR_TXN
	          ,S.TOT_YTD_AMT
		      FROM SMY.ATM_TXN_VOL_MTHLY_SMY S
		      WHERE 
		         S.CDR_MTH = CUR_MONTH -1
		         and
		         S.CDR_YR = CUR_YEAR
		         and
		         not exists(
		            select 1 from SESSION.TMP T
		            where T.ATM_SEQ_NO = S.ATM_SEQ_NO		                  
											AND T.CASH_TFR_IND = S.CASH_TFR_IND
											AND T.DB_CR_IND    = S.DB_CR_IND   
											AND T.CCY          = S.CCY         
											AND T.AC_OU_ID     = S.AC_OU_ID    
											AND T.RCU_CRD_F    = S.RCU_CRD_F
--------------------------------------- Start on 2010-06-03----------------------------------
											AND T.ZJRCU_OWN_ATM_F=S.ZJRCU_OWN_ATM_F
--------------------------------------- End on 2010-06-03------------------------------------
											AND T.CDR_YR       = S.CDR_YR      
											AND T.CDR_MTH -1   = S.CDR_MTH        
		                  
		         )		        
		      ;--
    ---------------------------------------End on 20100114-------------------------------------------------------		   
   	IF CUR_MONTH IN (4,7,10) THEN                                                      --���������
		      INSERT INTO SMY.ATM_TXN_VOL_MTHLY_SMY(ATM_SEQ_NO,
		                                            CCY,
		                                            CASH_TFR_IND,
		                                            DB_CR_IND,
		                                            AC_OU_ID,
		                                            RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                                                ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
		                                            CDR_YR,
		                                            CDR_MTH,
		                                            ACG_DT,
		                                            OU_ID,
		                                            TOT_MTD_NBR_TXN,
		                                            TOT_MTD_AMT,
		                                            TOT_QTD_NBR_TXN,
		                                            TOT_QTD_AMT,
		                                            TOT_YTD_NBR_TXN,
		                                            TOT_YTD_AMT)
		      SELECT S.ATM_SEQ_NO,
		             S.CCY,
		             S.CASH_TFR_IND,
		             S.DB_CR_IND,
		             S.AC_OU_ID,
		             S.RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                 S.ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
		             S.CDR_YR,
		             S.CDR_MTH,
		             S.ACG_DT,
		             S.OU_ID,
		             S.CUR_Day_NBR_TXN,
		             S.CUR_Day_AMT,
		             S.CUR_Day_NBR_TXN,
		             S.CUR_Day_AMT,
		             COALESCE(T.TOT_YTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
		             COALESCE(T.TOT_YTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT)
		      FROM SESSION.TMP S
		      LEFT JOIN SMY.ATM_TXN_VOL_MTHLY_SMY T
		      ON S.ATM_SEQ_NO=T.ATM_SEQ_NO
		      AND S.CCY=T.CCY
		      AND S.CASH_TFR_IND=T.CASH_TFR_IND
		      AND S.DB_CR_IND=T.DB_CR_IND
		      AND S.AC_OU_ID=T.AC_OU_ID
		      AND S.RCU_CRD_F=T.RCU_CRD_F
--------------------------------------- Start on 2010-06-03----------------------------------
          AND S.ZJRCU_OWN_ATM_F=T.ZJRCU_OWN_ATM_F
--------------------------------------- End on 2010-06-03------------------------------------
		      AND S.CDR_YR=T.CDR_YR
		      AND S.CDR_MTH-1=T.CDR_MTH;--
		   

          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
        ELSE                                                                             --�³��Ǽ��������
          INSERT INTO SMY.ATM_TXN_VOL_MTHLY_SMY(ATM_SEQ_NO,
                                                CCY,
                                                CASH_TFR_IND,
                                                DB_CR_IND,
                                                AC_OU_ID,
                                                RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                                                ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
                                                CDR_YR,
                                                CDR_MTH,
                                                ACG_DT,
                                                OU_ID,
                                                TOT_MTD_NBR_TXN,
                                                TOT_MTD_AMT,
                                                TOT_QTD_NBR_TXN,
                                                TOT_QTD_AMT,
                                                TOT_YTD_NBR_TXN,
                                                TOT_YTD_AMT)
          SELECT S.ATM_SEQ_NO,
                 S.CCY,
                 S.CASH_TFR_IND,
                 S.DB_CR_IND,
                 S.AC_OU_ID,
                 S.RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                 S.ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.OU_ID,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 COALESCE(T.TOT_QTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
                 COALESCE(T.TOT_QTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT),
                 COALESCE(T.TOT_YTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
                 COALESCE(T.TOT_YTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT)
          FROM SESSION.TMP S
          LEFT JOIN SMY.ATM_TXN_VOL_MTHLY_SMY T
          ON S.ATM_SEQ_NO=T.ATM_SEQ_NO
          AND S.CCY=T.CCY
          AND S.CASH_TFR_IND=T.CASH_TFR_IND
          AND S.DB_CR_IND=T.DB_CR_IND
          AND S.AC_OU_ID=T.AC_OU_ID
          AND S.RCU_CRD_F=T.RCU_CRD_F
--------------------------------------- Start on 2010-06-03----------------------------------
          AND S.ZJRCU_OWN_ATM_F=T.ZJRCU_OWN_ATM_F
--------------------------------------- End on 2010-06-03------------------------------------
          AND S.CDR_YR=T.CDR_YR
          AND S.CDR_MTH-1=T.CDR_MTH;--

          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
    END IF;    --CUR_MONTH IN (4,7,10)
   END IF;     --CUR_MONTH=1          
ELSE    --CUR_DAY<>1                                                                                      ---���³�
  MERGE INTO SMY.ATM_TXN_VOL_MTHLY_SMY S
  USING SESSION.TMP T
  ON S.ATM_SEQ_NO=T.ATM_SEQ_NO
  AND S.CCY=T.CCY
  AND S.CASH_TFR_IND=T.CASH_TFR_IND
  AND S.DB_CR_IND=T.DB_CR_IND
  AND S.AC_OU_ID=T.AC_OU_ID
  AND S.RCU_CRD_F=T.RCU_CRD_F
--------------------------------------- Start on 2010-06-03----------------------------------
  AND S.ZJRCU_OWN_ATM_F=T.ZJRCU_OWN_ATM_F
--------------------------------------- End on 2010-06-03------------------------------------
  AND S.CDR_YR=T.CDR_YR
  AND S.CDR_MTH=T.CDR_MTH
  WHEN MATCHED
  THEN UPDATE SET(ATM_SEQ_NO,
                  CCY,
                  CASH_TFR_IND,
                  DB_CR_IND,
                  AC_OU_ID,
                  RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                  ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
                  CDR_YR,
                  CDR_MTH,
                  ACG_DT,
                  OU_ID,
                  TOT_MTD_NBR_TXN,
                  TOT_MTD_AMT,
                  TOT_QTD_NBR_TXN,
                  TOT_QTD_AMT,
                  TOT_YTD_NBR_TXN,
                  TOT_YTD_AMT)
                =(T.ATM_SEQ_NO,
                  T.CCY,
                  T.CASH_TFR_IND,
                  T.DB_CR_IND,
                  T.AC_OU_ID,
                  T.RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
                  T.ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
                  T.CDR_YR,
                  T.CDR_MTH,
                  T.ACG_DT,
                  T.OU_ID,
                  S.TOT_MTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_MTD_AMT+T.CUR_Day_AMT,
                  S.TOT_QTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_QTD_AMT+T.CUR_Day_AMT,
                  S.TOT_YTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_YTD_AMT+T.CUR_Day_AMT)
  WHEN NOT MATCHED
  THEN INSERT(ATM_SEQ_NO,
              CCY,
              CASH_TFR_IND,
              DB_CR_IND,
              AC_OU_ID,
              RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
              ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
              CDR_YR,
              CDR_MTH,
              ACG_DT,
              OU_ID,
              TOT_MTD_NBR_TXN,
              TOT_MTD_AMT,
              TOT_QTD_NBR_TXN,
              TOT_QTD_AMT,
              TOT_YTD_NBR_TXN,
              TOT_YTD_AMT)
       VALUES(T.ATM_SEQ_NO,
              T.CCY,
              T.CASH_TFR_IND,
              T.DB_CR_IND,
              T.AC_OU_ID,
              T.RCU_CRD_F,
--------------------------------------- Start on 2010-06-03----------------------------------
              T.ZJRCU_OWN_ATM_F,
--------------------------------------- End on 2010-06-03------------------------------------
              T.CDR_YR,
              T.CDR_MTH,
              T.ACG_DT,
              T.OU_ID,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT);--

  GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
  COMMIT;--
END IF;   --CUR_DAY=1

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '���»������.';--
UPDATE SMY.ATM_TXN_VOL_MTHLY_SMY SET ACG_DT=ACCOUNTING_DATE WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

SET SMY_STEPNUM=6 ;--
SET SMY_STEPDESC = '�洢���̽���!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END
@