CREATE PROCEDURE SMY.PROC_DB_CRD_DEP_BAL_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_DB_CRD_DEP_BAL_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_DB_CRD_DEP_BAL_DLY_SMY
-- Source Table:				SOR.DB_CRD , SOR.DMD_DEP_SUB_AR,SOR.CRD
-- Target Table: 				SMY.DB_CRD_DEP_BAL_DLY_SMY
--                      SMY.DB_CRD_DEP_BAL_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  INSERT ONLY EACH DAY
--=============================================================================
-- Creation Date:       2009.11.10
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-10   JAMES SHANG     Create SP File
-- 2009-11-24   JAMES SHANG     ������±�������ݵĴ���
-- 2009-12-03   Xu Yan          Used the SMY table improve the performance
-- 2009-12-17   Xu Yan          Updated the WHERE condition to filter in only normal account
-- 2010-01-19   Xu Yan          Updated the accumualted value getting logic
-- 2010-01-21   Xu Yan          Dealed with the cards which have duplicated accounts.
-- 2010-02-01   Xu Yan          Removed the inactive cards and accumulated amount
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
		DECLARE CUR_YEAR SMALLINT;
		DECLARE CUR_MONTH SMALLINT;
		DECLARE CUR_DAY INTEGER;
		DECLARE YR_FIRST_DAY DATE;
		DECLARE QTR_FIRST_DAY DATE;
		DECLARE YR_DAY SMALLINT;
		DECLARE QTR_DAY SMALLINT;
		DECLARE MAX_ACG_DT DATE;
		DECLARE LAST_SMY_DATE DATE;
		DECLARE MTH_FIRST_DAY DATE;
		DECLARE V_T SMALLINT;
    DECLARE C_YR_DAY SMALLINT;
		DECLARE C_QTR_DAY SMALLINT;
		DECLARE QTR_LAST_DAY DATE;
		DECLARE C_MON_DAY SMALLINT;
		DECLARE CUR_QTR SMALLINT;
		-- ���������µ����һ��
		DECLARE MTH_LAST_DAY DATE; 

/*
	1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.
*/

		DECLARE CONTINUE HANDLER FOR NOT FOUND
		  SET V_T=0 ; 
		    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    	SET SMY_SQLCODE = SQLCODE;
      ROLLBACK;
      set SMY_STEPNUM = SMY_STEPNUM +1;
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
      COMMIT;
      RESIGNAL;
    END;
    
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
    	SET SMY_STEPNUM = SMY_STEPNUM + 1;
      SET SMY_SQLCODE = SQLCODE;
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'SQL ����Warning ��Ϣ', SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
      COMMIT;
    END;

   /*������ֵ*/
    SET SMY_PROCNM  ='PROC_DB_CRD_DEP_BAL_DLY_SMY';
    SET SMY_DATE    =ACCOUNTING_DATE;    
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --ȡ����ڼ���
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    --��������������
    SET C_MON_DAY = CUR_DAY;    
    
    --���㼾����������
    IF CUR_QTR = 1  
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;
    ELSEIF CUR_QTR = 2
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;       	
    ELSEIF CUR_QTR = 3
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;       	
    ELSE
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');       
    END IF;

  /*ȡ������������*/ 
  	SET C_QTR_DAY = DAYS(SMY_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.DB_CRD_DEP_BAL_DLY_SMY;

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;
			COMMIT;
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.DB_CRD_DEP_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;
       COMMIT;
    END IF;

/*�±�Ļָ�*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.DB_CRD_DEP_BAL_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;
   		COMMIT;
   	END IF;

SET SMY_STEPDESC = '�����û���ʱ��,���DB_CRD_SMY����';

	/*�����û���ʱ��*/
	------------------------Start on 20100121--------------------------------------------------------------
	DECLARE GLOBAL TEMPORARY TABLE TMP_DB_CRD
		LIKE SMY.DB_CRD_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CRD_NO);
 
 create index session.IDX_AC_AR on session.TMP_DB_CRD(AC_AR_ID);
 
 insert into SESSION.TMP_DB_CRD
    select * from SMY.DB_CRD_SMY where CRD_LCS_TP_ID = 11920001   --����	
    ;
 --------------Start on 20100201--------------------
 /*
 insert into SESSION.TMP_DB_CRD 
    select * from SMY.DB_CRD_SMY S
			 where not exists (
			     select 1 
			     from SESSION.TMP_DB_CRD T
			     where  
			       T.AC_AR_ID = S.AC_AR_ID
			 );
	*/		 
 --------------End on 20100201--------------------
 ------------------------End on 20100121--------------------------------------------------------------
      
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 	set SMY_STEPNUM = SMY_STEPNUM +1;
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           


		SET SMY_STEPDESC = '����SMY.DB_CRD_DEP_BAL_DLY_SMY �в���������Ϊ���������';
		

	INSERT INTO SMY.DB_CRD_DEP_BAL_DLY_SMY
 (
     OU_ID                  --���������
    ,CRD_TP_ID              --������
    ,PSBK_RLTD_F            --������ر�ʶ
    ,IS_NONGXIN_CRD_F       --���տ�/ũ�ſ���ʶ
    ,ENT_IDV_IND            --������
    ,CCY                    --����
    ,ACG_DT                 --����YYYY-MM-DD
    ,CDR_YR                 --���YYYY
    ,CDR_MTH                --�·�MM
    ,NOD_In_MTH           --��������
    ,NOD_In_QTR           --��������������
    ,NOD_In_Year          --��������������
    ,DEP_BAL_CRD            --���п�������
    ,MTD_ACML_BAL_AMT       --�ۼ����
    ,QTD_ACML_DEP_BAL_AMT   --�ۼƴ�����
    ,YTD_ACML_DEP_BAL_AMT   --�ۼƴ�����	
            )
	WITH TMP_CUR AS 
	( 
		SELECT 
		     OU_ID     AS OU_ID       
        ,DB_CRD_TP_ID            AS CRD_TP_ID  
        ,PSBK_RLTD_F       AS PSBK_RLTD_F
        ,IS_NONGXIN_CRD_F
        ,ENT_IDV_IND     AS ENT_IDV_IND     
        ,DB_CRD.CCY      AS CCY             
        ,ACCOUNTING_DATE         AS ACG_DT          
        ,CUR_YEAR                AS CDR_YR          
        ,CUR_MONTH               AS CDR_MTH         
        ,C_MON_DAY               AS NOD_In_MTH    
        ,C_QTR_DAY               AS NOD_In_QTR    
        ,C_YR_DAY               AS NOD_In_Year   
        ,SUM(COALESCE(AC.BAL_AMT,0))  AS DEP_BAL_CRD  
        ,SUM(COALESCE(AC.MTD_ACML_BAL_AMT, 0)) AS MTD_ACML_BAL_AMT
    		,SUM(COALESCE(AC.QTD_ACML_BAL_AMT,0))  AS QTD_ACML_DEP_BAL_AMT --�ۼƴ�����
    		,SUM(COALESCE(AC.YTD_ACML_BAL_AMT,0))  AS YTD_ACML_DEP_BAL_AMT --�ۼƴ�����	                  
		FROM SESSION.TMP_DB_CRD  AS DB_CRD 	
		     LEFT JOIN SMY.MTHLY_DMD_DEP_ACML_BAL_AMT AS AC
		     ON DB_CRD.AC_AR_ID = AC.AC_AR_ID
		        AND
		        DB_CRD.CCY = AC.CCY
		        AND
		        AC.CDR_YR = CUR_YEAR
		        AND
		        AC.CDR_MTH = CUR_MONTH
		--where CRD_LCS_TP_ID = 11920001   --����	
	where DB_CRD.END_DT >= YR_FIRST_DAY
	      OR
	      DB_CRD.END_DT = '1899-12-31'
		GROUP BY 
		     OU_ID       
        ,DB_CRD_TP_ID
        ,PSBK_RLTD_F
        ,IS_NONGXIN_CRD_F
        ,ENT_IDV_IND
        ,DB_CRD.CCY	
       )            
SELECT
     CUR.OU_ID                
    ,CUR.CRD_TP_ID            
    ,CUR.PSBK_RLTD_F          
    ,CUR.IS_NONGXIN_CRD_F     
    ,CUR.ENT_IDV_IND          
    ,CUR.CCY                  
    ,CUR.ACG_DT               
    ,CUR.CDR_YR               
    ,CUR.CDR_MTH              
    ,CUR.NOD_In_MTH 
    ,CUR.NOD_In_QTR 
    ,CUR.NOD_In_Year
    ,CUR.DEP_BAL_CRD
    ,MTD_ACML_BAL_AMT    
    ,QTD_ACML_DEP_BAL_AMT
    ,YTD_ACML_DEP_BAL_AMT
           
FROM  TMP_CUR AS CUR  
;
 /** �ռ�������Ϣ */	                          
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 	set SMY_STEPNUM = SMY_STEPNUM +1;
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 


  IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
  	  		
		  SET SMY_STEPDESC = '����������Ϊ�������һ��,���±�SMY.DB_CRD_DEP_BAL_MTHLY_SMY �в�������';

	INSERT INTO SMY.DB_CRD_DEP_BAL_MTHLY_SMY
  (
     OU_ID                  --���������
    ,CRD_TP_ID              --������
    ,PSBK_RLTD_F            --������ر�ʶ
    ,IS_NONGXIN_CRD_F       --���տ�/ũ�ſ���ʶ
    ,ENT_IDV_IND            --������
    ,CCY                    --����
    ,ACG_DT                 --����YYYY-MM-DD
    ,CDR_YR                 --���YYYY
    ,CDR_MTH                --�·�MM
    ,NOD_In_MTH           --��������
    ,NOD_In_QTR           --��������������
    ,NOD_In_Year          --��������������
    ,DEP_BAL_CRD            --���п�������
    ,MTD_ACML_BAL_AMT       --�ۼ����
    ,QTD_ACML_DEP_BAL_AMT   --�ۼƴ�����
    ,YTD_ACML_DEP_BAL_AMT   --�ۼƴ�����	
            )
  SELECT 
     OU_ID                  --���������
    ,CRD_TP_ID              --������
    ,PSBK_RLTD_F            --������ر�ʶ
    ,IS_NONGXIN_CRD_F       --���տ�/ũ�ſ���ʶ
    ,ENT_IDV_IND            --������
    ,CCY                    --����
    ,ACG_DT                 --����YYYY-MM-DD
    ,CDR_YR                 --���YYYY
    ,CDR_MTH                --�·�MM
    ,NOD_In_MTH           --��������
    ,NOD_In_QTR           --��������������
    ,NOD_In_Year          --��������������
    ,DEP_BAL_CRD            --���п�������
    ,MTD_ACML_BAL_AMT       --�ۼ����
    ,QTD_ACML_DEP_BAL_AMT   --�ۼƴ�����
    ,YTD_ACML_DEP_BAL_AMT   --�ۼƴ�����
  FROM   SMY.DB_CRD_DEP_BAL_DLY_SMY  WHERE ACG_DT=	ACCOUNTING_DATE;            
   
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 	set SMY_STEPNUM = SMY_STEPNUM +1;
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  		   
  END IF;

	 COMMIT;
END@