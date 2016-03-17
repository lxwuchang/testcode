CREATE PROCEDURE SMY.PROC_OU_INR_AC_BAL_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_OU_INR_AC_BAL_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_OU_INR_AC_BAL_DLY_SMY
-- Source Table:				SMY.MTHLY_INR_AC_ACML_BAL_AMT
--                      SOR.ACG_SBJ_CODE_MAPPING
-- Target Table: 				SMY.OU_INR_AC_BAL_DLY_SMY
--                      SMY.OU_INR_AC_BAL_MTHLY_SMY 
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  INSERT ONLY EACH DAY
--=============================================================================
-- Creation Date:       2009.11.11
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-11   JAMES SHANG     Create SP File
-- 2010-08-10   Fang Yihua      Added three new columns 'NOCLD_IN_MTH','NOCLD_IN_QTR','NOCLD_IN_YEAR'
-- 2010-08-24		Feng Jia Qiang	Modify the condition which insert data into SMY.OU_INR_AC_BAL_MTHLY_SMY
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN

/*�����쳣����ʹ�ñ���*/
		DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
		DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
		DECLARE SMY_DATE DATE;        --��ʱ���ڱ���
		DECLARE SMY_RCOUNT INT;       --DML������ü�¼��
		DECLARE SMY_PROCNM VARCHAR(100);                        --�洢��������

/*�����洢����ʹ�ñ���*/
		DECLARE CUR_YEAR SMALLINT;--
		DECLARE CUR_MONTH SMALLINT;--
		DECLARE CUR_DAY INTEGER;--
		DECLARE YR_FIRST_DAY DATE;--
		DECLARE QTR_FIRST_DAY DATE;--
		DECLARE YR_DAY SMALLINT;--
		DECLARE QTR_DAY SMALLINT;--
		DECLARE MAX_ACG_DT DATE;--
		DECLARE LAST_SMY_DATE DATE;--
		DECLARE MTH_FIRST_DAY DATE;--
		DECLARE V_T SMALLINT;--
    DECLARE C_YR_DAY SMALLINT;--
		DECLARE C_QTR_DAY SMALLINT;--
		DECLARE QTR_LAST_DAY DATE;--
		DECLARE C_MON_DAY SMALLINT;--
		DECLARE CUR_QTR SMALLINT;    --
		-- ���������µ����һ��
		DECLARE MTH_LAST_DAY DATE; 	--
/*
	1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.
*/

		DECLARE CONTINUE HANDLER FOR NOT FOUND
		  SET V_T=0 ; --
		    
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
    SET SMY_PROCNM  ='PROC_OU_INR_AC_BAL_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ���
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;     --
    --��������������
    SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
  
    --���㼾����������
    IF CUR_QTR = 1  
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;--
    ELSEIF CUR_QTR = 2
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;       	--
    ELSEIF CUR_QTR = 3
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;       	--
    ELSE
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');       --
    END IF;--

------------------------------------Start on 2010-08-10 -------------------------------------------------
    SET YR_DAY      =DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;--
    SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;-- 
------------------------------------End on 2010-08-10 ---------------------------------------------------

  /*ȡ������������*/ 
  	SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_INR_AC_BAL_DLY_SMY;--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
       COMMIT;--
    END IF;--

/*�±�Ļָ�*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_INR_AC_BAL_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
   		COMMIT;--
   	END IF;--

  SET SMY_STEPNUM = 2;
  SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';
  
DECLARE GLOBAL TEMPORARY TABLE PRE AS (
  SELECT 
      ACG_OU_IP_ID,                                          
      ACG_SBJ_ID,
      BAL_ACG_EFF_TP_ID,
      CCY,
      NOD_IN_MTH,
      NOD_IN_QTR,
      NOD_IN_YEAR,
      BAL_AMT,
      NBR_AC,
      CUR_CR_AMT,
      CUR_DB_AMT,
      MTD_ACML_BAL_AMT,
      QTD_ACML_BAL_AMT,
      YTD_ACML_BAL_AMT,
      TOT_MTD_DB_AMT,
      TOT_MTD_CR_AMT,
      TOT_QTD_CR_AMT,
      TOT_QTD_DB_AMT,
      TOT_YTD_DB_AMT,
      TOT_YTD_CR_AMT
  FROM SMY.OU_INR_AC_BAL_DLY_SMY
)DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID);
  
CREATE INDEX SESSION.PRE_IDX ON SESSION.PRE(ACG_OU_IP_ID,ACG_SBJ_ID,BAL_ACG_EFF_TP_ID,CCY);
  
/*��������һ�ղ���Ҫ����*/
IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
  IF ACCOUNTING_DATE = QTR_FIRST_DAY THEN   --����
      INSERT INTO SESSION.PRE
      (
			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,       
          NOD_IN_QTR,       
          NOD_IN_YEAR,      
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT, 
          QTD_ACML_BAL_AMT, 
          YTD_ACML_BAL_AMT, 
          TOT_MTD_DB_AMT,   
          TOT_MTD_CR_AMT,   
          TOT_QTD_CR_AMT,   
          TOT_QTD_DB_AMT,   
          TOT_YTD_DB_AMT,   
          TOT_YTD_CR_AMT    
      )
      SELECT 
 			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          0,                  --������Ч����
          0,                  --������Ч����
          NOD_IN_YEAR,        --������Ч����
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          0,                  --���ۼ����
          0,                  --���ۼ����
          YTD_ACML_BAL_AMT,   --���ۼ����
          0,                  --���ۼƽ跽������
          0,                  --���ۼƴ���������
          0,                  --���ۼƴ���������
          0,                  --���ۼƽ跽������
          TOT_YTD_DB_AMT,     --���ۼƽ跽������
          TOT_YTD_CR_AMT      --���ۼƴ���������
      FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
  ELSEIF ACCOUNTING_DATE=MTH_FIRST_DAY THEN   --�³�
      INSERT INTO SESSION.PRE 
      (
			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,       
          NOD_IN_QTR,       
          NOD_IN_YEAR,      
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT, 
          QTD_ACML_BAL_AMT, 
          YTD_ACML_BAL_AMT, 
          TOT_MTD_DB_AMT,   
          TOT_MTD_CR_AMT,   
          TOT_QTD_CR_AMT,   
          TOT_QTD_DB_AMT,   
          TOT_YTD_DB_AMT,   
          TOT_YTD_CR_AMT    
      )
      SELECT 
 			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          0,                  --������Ч����
          NOD_IN_QTR,         --������Ч����
          NOD_IN_YEAR,        --������Ч����
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          0,                  --���ۼ����
          QTD_ACML_BAL_AMT,   --���ۼ����
          YTD_ACML_BAL_AMT,   --���ۼ����
          0,                  --���ۼƽ跽������
          0,                  --���ۼƴ���������
          TOT_QTD_CR_AMT,     --���ۼƴ���������
          TOT_QTD_DB_AMT,     --���ۼƽ跽������
          TOT_YTD_DB_AMT,     --���ۼƽ跽������
          TOT_YTD_CR_AMT      --���ۼƴ���������
      FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
  ELSE
      INSERT INTO SESSION.PRE 
      (
			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,       
          NOD_IN_QTR,       
          NOD_IN_YEAR,      
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT, 
          QTD_ACML_BAL_AMT, 
          YTD_ACML_BAL_AMT, 
          TOT_MTD_DB_AMT,   
          TOT_MTD_CR_AMT,   
          TOT_QTD_CR_AMT,   
          TOT_QTD_DB_AMT,   
          TOT_YTD_DB_AMT,   
          TOT_YTD_CR_AMT    
      ) 
      SELECT 
 			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,         --������Ч����
          NOD_IN_QTR,         --������Ч����
          NOD_IN_YEAR,        --������Ч����
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT,   --���ۼ����
          QTD_ACML_BAL_AMT,   --���ۼ����
          YTD_ACML_BAL_AMT,   --���ۼ����
          TOT_MTD_DB_AMT,     --���ۼƽ跽������
          TOT_MTD_CR_AMT,     --���ۼƴ���������
          TOT_QTD_CR_AMT,     --���ۼƴ���������
          TOT_QTD_DB_AMT,     --���ۼƽ跽������
          TOT_YTD_DB_AMT,     --���ۼƽ跽������
          TOT_YTD_CR_AMT      --���ۼƴ���������
      FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
  END IF;
END IF;

  /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
	
  SET SMY_STEPNUM = 3;
  SET SMY_STEPDESC = '�����û���ʱ��,��ŵ�������';
  
DECLARE GLOBAL TEMPORARY TABLE CUR AS (
  SELECT
      RPRG_OU_IP_ID AS ACG_OU_IP_ID,                         --�������        
      INR.ACG_SBJ_ID AS ACG_SBJ_ID,                          --��Ŀ������      
      BAL_ACG_EFF_TP_ID AS BAL_ACG_EFF_TP_ID,                --����        
      CCY AS CCY,                                            --����            
      '1899-12-31' AS ACG_DT,                                --����            
      12 AS CDR_YR,                                          --���            
      31 AS CDR_MTH,                                         --�·�            
      COALESCE(MAP.NEW_ACG_SBJ_ID,'-1') AS NEW_ACG_SBJ_ID,   --�¿�Ŀ          
      1 AS NOD_IN_MTH,                                       --������Ч����    
      1 AS NOD_IN_QTR,                                       --������Ч����    
      1 AS NOD_IN_YEAR,                                      --������Ч����         
      SUM(BAL_AMT) AS BAL_AMT,                               --���            
      COUNT(DISTINCT AC_AR_ID) AS NBR_AC,                    --�˻���          
      SUM(CUR_DAY_CR_AMT) AS CUR_CR_AMT,                     --����������      
      SUM(CUR_DAY_DB_AMT) AS CUR_DB_AMT                      --�跽������      
  FROM SMY.MTHLY_INR_AC_ACML_BAL_AMT INR
  LEFT JOIN SOR.ACG_SBJ_CODE_MAPPING MAP ON INR.ACG_SBJ_ID = MAP.ACG_SBJ_ID AND MAP.END_DT = '9999-12-31'
  GROUP BY 
      RPRG_OU_IP_ID,                      --�������
      INR.ACG_SBJ_ID,                     --��Ŀ������
      BAL_ACG_EFF_TP_ID,                  --����
      CCY,                                --����
      COALESCE(MAP.NEW_ACG_SBJ_ID,'-1')   --�¿�Ŀ
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID);
  
CREATE INDEX SESSION.CUR_IDX ON SESSION.CUR(ACG_OU_IP_ID,ACG_SBJ_ID,BAL_ACG_EFF_TP_ID,CCY);

INSERT INTO SESSION.CUR
SELECT
    RPRG_OU_IP_ID AS ACG_OU_IP_ID,                         --�������        
    INR.ACG_SBJ_ID AS ACG_SBJ_ID,                          --��Ŀ������      
    BAL_ACG_EFF_TP_ID AS BAL_ACG_EFF_TP_ID,                --����        
    CCY AS CCY,                                            --����            
    ACCOUNTING_DATE AS ACG_DT,                             --����            
    CUR_YEAR AS CDR_YR,                                    --���            
    CUR_MONTH AS CDR_MTH,                                  --�·�            
    COALESCE(MAP.NEW_ACG_SBJ_ID,'-1') AS NEW_ACG_SBJ_ID,   --�¿�Ŀ          
    1 AS NOD_IN_MTH,                                       --������Ч����    
    1 AS NOD_IN_QTR,                                       --������Ч����    
    1 AS NOD_IN_YEAR,                                      --������Ч����         
    SUM(BAL_AMT) AS BAL_AMT,                               --���            
    COUNT(DISTINCT AC_AR_ID) AS NBR_AC,                    --�˻���          
    SUM(CUR_DAY_CR_AMT) AS CUR_CR_AMT,                     --����������      
    SUM(CUR_DAY_DB_AMT) AS CUR_DB_AMT                      --�跽������        
FROM SMY.MTHLY_INR_AC_ACML_BAL_AMT INR
LEFT JOIN SOR.ACG_SBJ_CODE_MAPPING MAP ON INR.ACG_SBJ_ID = MAP.ACG_SBJ_ID AND MAP.END_DT = '9999-12-31'
WHERE ACG_DT = ACCOUNTING_DATE
GROUP BY 
    RPRG_OU_IP_ID,                      --�������
    INR.ACG_SBJ_ID,                     --��Ŀ������
    BAL_ACG_EFF_TP_ID,                  --����
    CCY,                                --����
    COALESCE(MAP.NEW_ACG_SBJ_ID,'-1')   --�¿�Ŀ
;

  /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
	
  SET SMY_STEPNUM = 4;
  SET SMY_STEPDESC = '���������ݺ��������ݹ��������SMY.OU_INR_AC_BAL_DLY_SMY';
  
  INSERT INTO SMY.OU_INR_AC_BAL_DLY_SMY
  (
       ACG_OU_IP_ID        --�������
      ,ACG_SBJ_ID          --��Ŀ������
      ,BAL_ACG_EFF_TP_ID   --����
      ,CCY                 --����
      ,ACG_DT              --����
      ,CDR_YR              --���
      ,CDR_MTH             --�·�
      ,NEW_ACG_SBJ_ID      --�¿�Ŀ
      ,NOD_IN_MTH          --������Ч����
      ,NOD_IN_QTR          --������Ч����
      ,NOD_IN_YEAR         --������Ч����
      ,BAL_AMT             --���
      ,NBR_AC              --�˻���
      ,CUR_CR_AMT          --����������
      ,CUR_DB_AMT          --�跽������
      ,MTD_ACML_BAL_AMT    --���ۼ����
      ,QTD_ACML_BAL_AMT    --���ۼ����
      ,YTD_ACML_BAL_AMT    --���ۼ����
      ,TOT_MTD_DB_AMT      --���ۼƽ跽������
      ,TOT_MTD_CR_AMT      --���ۼƴ���������
      ,TOT_QTD_CR_AMT      --���ۼƴ���������
      ,TOT_QTD_DB_AMT      --���ۼƽ跽������
      ,TOT_YTD_DB_AMT      --���ۼƽ跽������
      ,TOT_YTD_CR_AMT      --���ۼƴ���������
      ,NOCLD_IN_MTH        --������������
      ,NOCLD_IN_QTR        --������������
      ,NOCLD_IN_YEAR       --������������
  )
  SELECT 
       CUR.ACG_OU_IP_ID                                   --�������
      ,CUR.ACG_SBJ_ID                                     --��Ŀ������
      ,CUR.BAL_ACG_EFF_TP_ID                              --����
      ,CUR.CCY                                            --����
      ,CUR.ACG_DT                                         --����
      ,CUR.CDR_YR                                         --���
      ,CUR.CDR_MTH                                        --�·�
      ,CUR.NEW_ACG_SBJ_ID                                 --�¿�Ŀ
      ,COALESCE(PRE.NOD_IN_MTH ,0) + CUR.NOD_IN_MTH       --������Ч����
      ,COALESCE(PRE.NOD_IN_QTR ,0) + CUR.NOD_IN_QTR       --������Ч����
      ,COALESCE(PRE.NOD_IN_YEAR,0) + CUR.NOD_IN_YEAR      --������Ч����
      ,CUR.BAL_AMT                                        --���
      ,CUR.NBR_AC                                         --�˻���
      ,CUR.CUR_CR_AMT                                     --����������
      ,CUR.CUR_DB_AMT                                     --�跽������
      ,COALESCE(PRE.MTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT    --���ۼ����
      ,COALESCE(PRE.QTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT    --���ۼ����
      ,COALESCE(PRE.YTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT    --���ۼ����
      ,COALESCE(PRE.TOT_MTD_DB_AMT ,0) + CUR.CUR_DB_AMT   --���ۼƽ跽������
      ,COALESCE(PRE.TOT_MTD_CR_AMT ,0) + CUR.CUR_CR_AMT   --���ۼƴ���������
      ,COALESCE(PRE.TOT_QTD_CR_AMT ,0) + CUR.CUR_CR_AMT   --���ۼƴ���������
      ,COALESCE(PRE.TOT_QTD_DB_AMT ,0) + CUR.CUR_DB_AMT   --���ۼƽ跽������
      ,COALESCE(PRE.TOT_YTD_DB_AMT ,0) + CUR.CUR_DB_AMT   --���ۼƽ跽������
      ,COALESCE(PRE.TOT_YTD_CR_AMT ,0) + CUR.CUR_CR_AMT   --���ۼƴ���������
      ,CUR_DAY                                            --������������
      ,QTR_DAY                                            --������������
      ,YR_DAY                                             --������������
  FROM SESSION.CUR AS CUR LEFT OUTER JOIN SESSION.PRE AS PRE
      ON CUR.ACG_OU_IP_ID         = PRE.ACG_OU_IP_ID   
		  AND CUR.ACG_SBJ_ID        = PRE.ACG_SBJ_ID     
		  AND CUR.BAL_ACG_EFF_TP_ID = PRE.BAL_ACG_EFF_TP_ID
		  AND CUR.CCY               = PRE.CCY
;

  /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
	
	/*��������һ�ղ���Ҫ�ز�*/
	IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
    SET SMY_STEPNUM = 5;
    SET SMY_STEPDESC = '�ز�����𲢵�����';
    
    INSERT INTO SMY.OU_INR_AC_BAL_DLY_SMY
    (
         ACG_OU_IP_ID        --�������
        ,ACG_SBJ_ID          --��Ŀ������
        ,BAL_ACG_EFF_TP_ID   --����
        ,CCY                 --����
        ,ACG_DT              --����
        ,CDR_YR              --���
        ,CDR_MTH             --�·�
        ,NEW_ACG_SBJ_ID      --�¿�Ŀ
        ,NOD_IN_MTH          --������Ч����
        ,NOD_IN_QTR          --������Ч����
        ,NOD_IN_YEAR         --������Ч����
        ,BAL_AMT             --���
        ,NBR_AC              --�˻���
        ,CUR_CR_AMT          --����������
        ,CUR_DB_AMT          --�跽������
        ,MTD_ACML_BAL_AMT    --���ۼ����
        ,QTD_ACML_BAL_AMT    --���ۼ����
        ,YTD_ACML_BAL_AMT    --���ۼ����
        ,TOT_MTD_DB_AMT      --���ۼƽ跽������
        ,TOT_MTD_CR_AMT      --���ۼƴ���������
        ,TOT_QTD_CR_AMT      --���ۼƴ���������
        ,TOT_QTD_DB_AMT      --���ۼƽ跽������
        ,TOT_YTD_DB_AMT      --���ۼƽ跽������
        ,TOT_YTD_CR_AMT      --���ۼƴ���������
        ,NOCLD_IN_MTH        --������������
        ,NOCLD_IN_QTR        --������������
        ,NOCLD_IN_YEAR       --������������
    )
    SELECT 
         ACG_OU_IP_ID        --�������
        ,ACG_SBJ_ID          --��Ŀ������
        ,BAL_ACG_EFF_TP_ID   --����
        ,CCY                 --����
        ,ACCOUNTING_DATE     --����
        ,CUR_YEAR            --���
        ,CUR_MONTH           --�·�
        ,NEW_ACG_SBJ_ID      --�¿�Ŀ
        ,NOD_IN_MTH          --������Ч����
        ,NOD_IN_QTR          --������Ч����
        ,NOD_IN_YEAR         --������Ч����
        ,0                   --���
        ,0                   --�˻���
        ,0                   --����������
        ,0                   --�跽������
        ,case when CUR_DAY=1 then 0 else MTD_ACML_BAL_AMT end   --���ۼ����
        ,case when CUR_DAY=1 and CUR_MONTH in (4,7,10) then 0 else QTD_ACML_BAL_AMT end   --���ۼ����
        ,YTD_ACML_BAL_AMT    --���ۼ����
        ,case when CUR_DAY=1 then 0 else TOT_MTD_DB_AMT end     --���ۼƽ跽������
        ,case when CUR_DAY=1 then 0 else TOT_MTD_CR_AMT end     --���ۼƴ���������
        ,case when CUR_DAY=1 and CUR_MONTH in (4,7,10) then 0 else TOT_QTD_CR_AMT end     --���ۼƴ���������
        ,case when CUR_DAY=1 and CUR_MONTH in (4,7,10) then 0 else TOT_QTD_DB_AMT end     --���ۼƽ跽������
        ,TOT_YTD_DB_AMT      --���ۼƽ跽������
        ,TOT_YTD_CR_AMT      --���ۼƴ���������
        ,NOCLD_IN_MTH        --������������
        ,NOCLD_IN_QTR        --������������
        ,NOCLD_IN_YEAR       --������������
    FROM SMY.OU_INR_AC_BAL_DLY_SMY PRE
    WHERE ACG_DT = LAST_SMY_DATE
      AND NOT EXISTS(
        SELECT 1 FROM SMY.OU_INR_AC_BAL_DLY_SMY CUR
        WHERE CUR.ACG_OU_IP_ID=PRE.ACG_OU_IP_ID
          AND CUR.ACG_SBJ_ID=PRE.ACG_SBJ_ID
          AND CUR.BAL_ACG_EFF_TP_ID=PRE.BAL_ACG_EFF_TP_ID
          AND CUR.CCY=PRE.CCY
          AND CUR.ACG_DT=ACCOUNTING_DATE)
    ;
    
    /** �ռ�������Ϣ */
    GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
    INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) 
      VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --
	END IF;
	

/*�±�Ĳ���*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
  		SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
		  SET SMY_STEPDESC = '����������Ϊ�������һ��,���±�SMY.OU_INR_AC_BAL_MTHLY_SMY �в�������';   	--
	INSERT INTO SMY.OU_INR_AC_BAL_MTHLY_SMY
 (
          ACG_OU_IP_ID        --�������
         ,ACG_SBJ_ID          --��Ŀ������
         ,BAL_ACG_EFF_TP_ID   --����
         ,CCY                 --����
         ,ACG_DT              --����
         ,CDR_YR              --���
         ,CDR_MTH             --�·�
         ,NEW_ACG_SBJ_ID      --�¿�Ŀ
         ,NOD_IN_MTH          --������Ч����
         ,NOD_IN_QTR          --������Ч����
         ,NOD_IN_YEAR         --������Ч����
         ,BAL_AMT             --���
         ,NBR_AC              --�˻���
         ,CUR_CR_AMT          --����������
         ,CUR_DB_AMT          --�跽������
         ,MTD_ACML_BAL_AMT    --���ۼ����
         ,QTD_ACML_BAL_AMT    --���ۼ����
         ,YTD_ACML_BAL_AMT    --���ۼ����
         ,TOT_MTD_DB_AMT      --���ۼƽ跽������
         ,TOT_MTD_CR_AMT      --���ۼƴ���������
         ,TOT_QTD_CR_AMT      --���ۼƴ���������
         ,TOT_QTD_DB_AMT      --���ۼƽ跽������
         ,TOT_YTD_DB_AMT      --���ۼƽ跽������
         ,TOT_YTD_CR_AMT      --���ۼƴ���������
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------

            )  
            
SELECT
          ACG_OU_IP_ID        --�������
         ,ACG_SBJ_ID          --��Ŀ������
         ,BAL_ACG_EFF_TP_ID   --����
         ,CCY                 --����
         ,ACG_DT              --����
         ,CDR_YR              --���
         ,CDR_MTH             --�·�
         ,NEW_ACG_SBJ_ID      --�¿�Ŀ
         ,NOD_IN_MTH          --������Ч����
         ,NOD_IN_QTR          --������Ч����
         ,NOD_IN_YEAR         --������Ч����
         ,BAL_AMT             --���
         ,NBR_AC              --�˻���
         ,CUR_CR_AMT          --����������
         ,CUR_DB_AMT          --�跽������
         ,MTD_ACML_BAL_AMT    --���ۼ����
         ,QTD_ACML_BAL_AMT    --���ۼ����
         ,YTD_ACML_BAL_AMT    --���ۼ����
         ,TOT_MTD_DB_AMT      --���ۼƽ跽������
         ,TOT_MTD_CR_AMT      --���ۼƴ���������
         ,TOT_QTD_CR_AMT      --���ۼƴ���������
         ,TOT_QTD_DB_AMT      --���ۼƽ跽������
         ,TOT_YTD_DB_AMT      --���ۼƽ跽������
         ,TOT_YTD_CR_AMT      --���ۼƴ���������
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------

    -- FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE  ACG_DT = LAST_SMY_DATE ; --deleted by Feng Jia Qiang 2010-08-24
  FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE  ACG_DT = MTH_LAST_DAY ; --added by Feng Jia Qiang 2010-08-24
   
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	        		  --
		  
	 END IF;--


	 COMMIT;--
END@