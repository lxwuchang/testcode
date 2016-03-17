CREATE PROCEDURE SMY.PROC_OU_DEP_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_OU_DEP_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_OU_DEP_DLY_SMY
-- Source Table:				SMY.DEP_AR_SMY ,SOR.CST
--                      SMY.MTHLY_FT_DEP_ACML_BAL_AMT
--                      SMY.MTHLY_DMD_DEP_ACML_BAL_AMT
--                      SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT
-- Target Table: 				SMY.OU_DEP_DLY_SMY
--                      SMY.OU_DEP_MTHLY_SMY 
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
-- 2009-11-24   JAMES SHANG     �����±���		
-- 2009-11-26   Xu Yan          Updated 'NEW_ACG_SBJ_ID' column
-- 2009-12-01   Xu Yan          Updated the joint table for 'NEW_ACG_SBJ_ID'
-- 2009-12-02   Xu Yan          Updated 'NEW_ACG_SBJ_ID' due to SOR change
-- 2009-12-18   Xu Yan          Updated to filter in only normal accounts
-- 2010-01-06   Xu Yan          Included the transactions on the account closing day.
-- 2010-01-19   Xu Yan          Updated the accumualted value getting logic,which is same as the LOAN
-- 2010-02-25   Xu Yan          Fixed a bug about the accumulated amount.
-- 2010-08-10   Fang Yihua      Added three new columns 'NOCLD_IN_MTH','NOCLD_IN_QTR','NOCLD_IN_YEAR'
-- 2010-09-02   Sheng Qibin     Added two TEMPORARY TABLE ,two INDEX, TMP_MTHLY_DMD_DEP_ACML_BAL_AMT,TMP_MTHLY_FT_DEP_ACML_BAL_AMT,IDX_TMP1,IDX_TMP_MTHLY_FT_DEP_ACML_BAL_AMT
-- 2012-02-06   Zheng Bin       Updated SESSION.TMP_CUR'data filter by the current year
-- 2012-02-27   Chen XiaoWen    1.����������ʱ�������
--                              2.�������ݵ�TMP_MTHLY_DMD_DEP_ACML_BAL_AMT��TMP_MTHLY_FT_DEP_ACML_BAL_AMT��ʱ��ʱ���޸�ԭ��ѯ������ʹ��ACG_DT��������ѯ
--                              3.�������ݵ�TMP_CUR_AMTʱ��������UNION ALL��װ�������Ĳ��������޸�Ϊ�ĸ�������insert��䡣
-- 2012-03-16   Chen XiaoWen    1.������ʱ��TMP_TMP�������м������ٽ���group by
-- 2012-06-07   Chen XiaoWen    �޸�TM_MAT_SEG_ID�ֶε�ȡ������
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
		DECLARE CUR_QTR SMALLINT; --
		-- ���������µ����һ��
		DECLARE MTH_LAST_DAY DATE; 		   --
    DECLARE MAT_SEG_ID_1 INT DEFAULT 0;
    DECLARE MAT_SEG_ID_2 INT DEFAULT 0;
    DECLARE CNT INT DEFAULT 0;
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
      set SMY_STEPNUM = SMY_STEPNUM + 1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
--    DECLARE CONTINUE HANDLER FOR SQLWARNING
--    BEGIN
--      SET SMY_SQLCODE = SQLCODE;--
--      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
--      COMMIT;--
--    END;--
--

   /*������ֵ*/

    SET SMY_PROCNM  ='PROC_OU_DEP_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --ȡ����ڼ���
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���

    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;  --

    --��������������

    SET C_MON_DAY = CUR_DAY;    --

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

  	SET C_QTR_DAY = DAYS(SMY_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_DEP_DLY_SMY;--
		
    SELECT MAT_SEG_ID INTO MAT_SEG_ID_1 FROM SMY.MAT_SEG WHERE LOW_VAL=-99999;
    SELECT MAT_SEG_ID INTO MAT_SEG_ID_2 FROM SMY.MAT_SEG WHERE MAX_VAL=99999;
    SELECT MAX(LOW_VAL)-1 INTO CNT FROM SMY.MAT_SEG;

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/

		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--

			COMMIT;--

		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;--

		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --
				
/*���ݻָ��뱸��*/

    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_DEP_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
       COMMIT;--
    END IF;--

/*�±�Ļָ�*/

   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_DEP_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
   		COMMIT;--
   	END IF;--
   	
 /** �ռ�������Ϣ */		                             

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --

   /** ͳ�ƽ衢����������

     ��Դ��

     SMY.MTHLY_FT_DEP_ACML_BAL_AMT

     SMY.MTHLY_DMD_DEP_ACML_BAL_AMT

     SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT

    **/

		--SET SMY_STEPNUM = 3 ;--

		SET SMY_STEPDESC = '�����û���ʱ��,ͳ�ƽ衢����������';--
		
------add by sheng qibin start 20100902----------------                                                                                                        
	                                                                                                                           
	DECLARE GLOBAL TEMPORARY TABLE TMP_MTHLY_DMD_DEP_ACML_BAL_AMT                                                                                         
		LIKE SMY.MTHLY_DMD_DEP_ACML_BAL_AMT                                                                                               
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(AC_AR_ID);--
	                                                                                                                                                                                                                               
	CREATE UNIQUE INDEX SESSION.IDX_TMP1 ON SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT (AC_AR_ID,CCY);
   
   DECLARE GLOBAL TEMPORARY TABLE TMP_MTHLY_FT_DEP_ACML_BAL_AMT                                                                                         
		LIKE SMY.MTHLY_FT_DEP_ACML_BAL_AMT
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(AC_AR_ID); --
	
	CREATE UNIQUE INDEX SESSION.IDX_TMP_MTHLY_FT_DEP_ACML_BAL_AMT ON SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT (AC_AR_ID,CCY);

	insert into SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT
	select * from SMY.MTHLY_DMD_DEP_ACML_BAL_AMT where ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;
	
	insert into SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT
	select * FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT where ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;

/*	
	insert into SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT
	select * from SMY.MTHLY_DMD_DEP_ACML_BAL_AMT 
	WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH ;--
 
 insert into SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT
 select * FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT  --
			WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH ;--
*/
	
------add by sheng qibin end 20100902----------------    

	DECLARE GLOBAL TEMPORARY TABLE TMP_CUR_AMT  AS 	
	(
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT) AS CUR_DAY_CR_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) AS CUR_DAY_DB_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as MTD_ACML_BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as QTD_ACML_BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as YTD_ACML_BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as TOT_MTD_CR_AMT
         ,SUM(CUR_DAY_DB_AMT) as TOT_MTD_DB_AMT
         ,SUM(CUR_DAY_DB_AMT) as TOT_QTD_CR_AMT
				 ,SUM(CUR_DAY_DB_AMT) as TOT_QTD_DB_AMT
				 ,SUM(CUR_DAY_DB_AMT) as TOT_YTD_CR_AMT
				 ,SUM(CUR_DAY_DB_AMT) as TOT_YTD_DB_AMT				 	  		 
			FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT 
			GROUP BY 	
					AC_AR_ID	
				  ,CCY
	  )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(AC_AR_ID)
;		--

  CREATE INDEX SESSION.TMP_CUR_AMT_IDX ON SESSION.TMP_CUR_AMT (AC_AR_ID,CCY);
  
  INSERT INTO SESSION.TMP_CUR_AMT 
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(COALESCE(CUR_DAY_CR_AMT,0))
	  		 ,SUM(COALESCE(CUR_DAY_DB_AMT,0))
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))  
		--- delete by sheng qibin start 20100902------- 	
		--	FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT  --
		--	WHERE CDR_YR = CUR_YEAR
		--	      AND
		--	      CDR_MTH = CUR_MONTH
		---delete by sheng qibin start 20100902-------
			--add by   sheng qibin start 20100902-------	 
			FROM  SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT
			--add by   sheng qibin end 20100902-------	 
			GROUP BY 	
					AC_AR_ID	
				 ,CCY;
	--UNION ALL
	INSERT INTO SESSION.TMP_CUR_AMT
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT)
	  		 ,SUM(CUR_DAY_DB_AMT)
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))
			--delete by sheng qibin start 20100902 ---- 
		 --FROM  SMY.MTHLY_DMD_DEP_ACML_BAL_AMT --
		 --	WHERE CDR_YR = CUR_YEAR
		--	      AND
		--	      CDR_MTH = CUR_MONTH  	  
		--delete by sheng qibin end 20100902 ---- 	
		--add by sheng qibin start 20100902 ---- 	 
			FROM  SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT
		--add by sheng qibin end 20100902 ---- 	 
			GROUP BY 	
					AC_AR_ID	
				 ,CCY;
	--UNION ALL
	INSERT INTO SESSION.TMP_CUR_AMT
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT)
	  		 ,SUM(CUR_DAY_DB_AMT)
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))  	
			FROM  SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT --
			WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH			
			GROUP BY 	
					AC_AR_ID
				 ,CCY;
	--UNION ALL 
	INSERT INTO SESSION.TMP_CUR_AMT
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT)
	  		 ,SUM(CUR_DAY_DB_AMT)
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))  	
			FROM  SMY.MTHLY_EQTY_AC_ACML_BAL_AMT --
			WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH			
			GROUP BY 	
					AC_AR_ID
				 ,CCY	
  	;--
 /*ͳ����Ϣ�ռ�*/    

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	       --

		--SET SMY_STEPNUM = 4 ;--

		SET SMY_STEPDESC = '�����û������ͻ���ʱ����SOR.CST �в�����Ч����Ϊ���յ�����';--

	DECLARE GLOBAL TEMPORARY TABLE TMP_NEW_CST AS 	
	(
	  	SELECT
	  		CST_ID
	  		,EFF_CST_DT
	  	FROM SOR.CST		  
	  )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID)
	  ;				--
	
	CREATE INDEX SESSION.TMP_NEW_CST_IDX ON SESSION.TMP_NEW_CST (CST_ID);
	
  INSERT INTO SESSION.TMP_NEW_CST
   		SELECT
	  		CST_ID
	  	 ,EFF_CST_DT
	  	FROM SOR.CST
	  	WHERE YEAR(EFF_CST_DT)=CUR_YEAR
	 ;--

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  --

		--SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = '�����û���ʱ��,���뵱�ջ�����ʱ����';--

  DECLARE GLOBAL TEMPORARY TABLE TMP_CUR AS 
   (
 		SELECT
			    DEP_AR_SMY.RPRG_OU_IP_ID        AS  ACG_OU_IP_ID   
         ,DEP_AR_SMY.ACG_SBJ_ID           AS  DEP_ACG_SBJ_ID 
         ,DEP_AR_SMY.PD_GRP_CODE          AS  PD_GRP_CD      
         ,DEP_AR_SMY.PD_SUB_CODE          AS  PD_SUB_CD      
         ,DEP_AR_SMY.DEP_TP_ID            AS  DEP_TM_TP_ID   
         ,DEP_AR_SMY.TM_MAT_SEG_ID        AS  TM_MAT_SEG_ID
         ,DEP_AR_SMY.ENT_IDV_IND          AS  ENT_IDV_IND    
         ,DEP_AR_SMY.DNMN_CCY_ID          AS  CCY
         ,DATE('2009-10-16')      AS ACG_DT               --����
         ,2009      AS CDR_YR               --���
         ,9      AS CDR_MTH              --�·�
         ,1      AS NOD_IN_MTH           --����Ч����
         ,1      AS NOD_IN_QTR           --������Ч����
         ,1      AS NOD_IN_YEAR          --����Ч����	
         ,DEP_AR_SMY.ACG_SBJ_ID   AS NEW_ACG_SBJ_ID       --�¿�Ŀ
         ,SUM(DEP_AR_SMY.BAL_AMT)   AS BAL_AMT                 --���                  
         ,COUNT(DISTINCT DEP_AR_SMY.CST_ID)          AS NBR_CST            --�ͻ���
         ,COUNT(DISTINCT DEP_AR_SMY.DEP_AR_ID)        AS NBR_AC             --�˻���
         ,1  AS NBR_NEW_AC         --�����˻���        
         ,SUM(CASE WHEN TMP_NEW_CST.CST_ID  IS NULL THEN 0  ELSE 1 END)       AS NBR_NEW_CST        --�����ͻ���
         ,1     AS NBR_AC_CLS   --���������˻��� 20370008: ����
         ,SUM(COALESCE(TMP_CUR_AMT.CUR_DAY_CR_AMT ,0))     AS CUR_CR_AMT         --����������
         ,SUM(COALESCE(TMP_CUR_AMT.CUR_DAY_DB_AMT ,0))     AS CUR_DB_AMT         --�跽������
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID	AS AC_CHRCTR_TP_ID     --�˻�����           
	  		 ,SUM(VALUE(MTD_ACML_BAL_AMT,0)) AS MTD_ACML_BAL_AMT
	  		 ,SUM(VALUE(QTD_ACML_BAL_AMT,0)) AS QTD_ACML_BAL_AMT
	  		 ,SUM(VALUE(YTD_ACML_BAL_AMT,0)) AS  YTD_ACML_BAL_AMT
	  		 ,SUM(VALUE(TOT_MTD_CR_AMT  ,0)) AS  TOT_MTD_CR_AMT  
         ,SUM(VALUE(TOT_MTD_DB_AMT  ,0)) AS  TOT_MTD_DB_AMT  
         ,SUM(VALUE(TOT_QTD_CR_AMT  ,0)) AS  TOT_QTD_CR_AMT  
				 ,SUM(VALUE(TOT_QTD_DB_AMT  ,0)) AS  TOT_QTD_DB_AMT  
				 ,SUM(VALUE(TOT_YTD_CR_AMT  ,0)) AS  TOT_YTD_CR_AMT  
				 ,SUM(VALUE(TOT_YTD_DB_AMT  ,0)) AS  TOT_YTD_DB_AMT 
         ,1 TOT_MTD_NBR_NEW_AC  --�������˻���
         ,1 TOT_QTD_NBR_NEW_AC  --�������˻���
         ,1 TOT_YTD_NBR_NEW_AC  --�������˻���
         ,1 TOT_MTD_NBR_NEW_CST  --�������ͻ���
         ,1 TOT_QTD_NBR_NEW_CST  --�������ͻ���
         ,1 TOT_YTD_NBR_NEW_CST  --�������ͻ���
         ,1 TOT_MTD_NBR_AC_CLS  --���ۼ������˻���
         ,1 TOT_QTD_NBR_AC_CLS  --���ۼ������˻���
         ,1 TOT_YTD_NBR_AC_CLS  --���ۼ������˻���				     
    FROM  SMY.DEP_AR_SMY AS DEP_AR_SMY  
    LEFT OUTER JOIN SESSION.TMP_CUR_AMT  AS TMP_CUR_AMT ON DEP_AR_SMY.DEP_AR_ID = TMP_CUR_AMT.AC_AR_ID
    LEFT OUTER JOIN SESSION.TMP_NEW_CST  AS TMP_NEW_CST ON DEP_AR_SMY.CST_ID=TMP_NEW_CST.CST_ID
    GROUP BY 
			    DEP_AR_SMY.RPRG_OU_IP_ID    
         ,DEP_AR_SMY.ACG_SBJ_ID       
         ,DEP_AR_SMY.PD_GRP_CODE      
         ,DEP_AR_SMY.PD_SUB_CODE      
         ,DEP_AR_SMY.DEP_TP_ID        
         ,DEP_AR_SMY.TM_MAT_SEG_ID    
         ,DEP_AR_SMY.ENT_IDV_IND      
         ,DEP_AR_SMY.DNMN_CCY_ID 
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID             
 ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID); --

DECLARE GLOBAL TEMPORARY TABLE TMP_TMP AS 
(
    SELECT
			    DEP_AR_SMY.RPRG_OU_IP_ID        AS  ACG_OU_IP_ID   
         ,DEP_AR_SMY.ACG_SBJ_ID           AS  DEP_ACG_SBJ_ID 
         ,DEP_AR_SMY.PD_GRP_CODE          AS  PD_GRP_CD      
         ,DEP_AR_SMY.PD_SUB_CODE          AS  PD_SUB_CD      
         ,DEP_AR_SMY.DEP_TP_ID            AS  DEP_TM_TP_ID   
         ,DEP_AR_SMY.TM_MAT_SEG_ID        AS  TM_MAT_SEG_ID
         ,DEP_AR_SMY.ENT_IDV_IND          AS  ENT_IDV_IND    
         ,DEP_AR_SMY.DNMN_CCY_ID          AS  CCY
         ,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')  AS NEW_ACG_SBJ_ID
         ,DEP_AR_SMY.BAL_AMT   AS BAL_AMT
         ,DEP_AR_SMY.CST_ID          AS NBR_CST
         ,CASE WHEN AR_LCS_TP_ID <> 20370008 then 1 else 0 end        AS NBR_AC
         ,CASE WHEN DEP_AR_SMY.EFF_DT='2012-03-13' THEN 1 ELSE 0 END   AS NBR_NEW_AC
         ,CASE WHEN TMP_NEW_CST.EFF_CST_DT = '2012-03-13' THEN 1  ELSE 0 END       AS NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and DEP_AR_SMY.END_DT = '2012-03-13' then 1 else 0 end   AS NBR_AC_CLS
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_CR_AMT ,0)     AS CUR_CR_AMT
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_DB_AMT ,0)     AS CUR_DB_AMT
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID	AS AC_CHRCTR_TP_ID
	  		 ,VALUE(MTD_ACML_BAL_AMT,0) AS MTD_ACML_BAL_AMT
	  		 ,VALUE(QTD_ACML_BAL_AMT,0) AS QTD_ACML_BAL_AMT
	  		 ,VALUE(YTD_ACML_BAL_AMT,0) AS  YTD_ACML_BAL_AMT
	  		 ,VALUE(TOT_MTD_CR_AMT  ,0) AS  TOT_MTD_CR_AMT  
         ,VALUE(TOT_MTD_DB_AMT  ,0) AS  TOT_MTD_DB_AMT  
         ,VALUE(TOT_QTD_CR_AMT  ,0) AS  TOT_QTD_CR_AMT  
				 ,VALUE(TOT_QTD_DB_AMT  ,0) AS  TOT_QTD_DB_AMT  
				 ,VALUE(TOT_YTD_CR_AMT  ,0) AS  TOT_YTD_CR_AMT  
				 ,VALUE(TOT_YTD_DB_AMT  ,0) AS  TOT_YTD_DB_AMT   
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = 2012 AND MONTH(DEP_AR_SMY.EFF_DT) = 3 THEN 1 ELSE 0 END  TOT_MTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = 2012 AND QUARTER(DEP_AR_SMY.EFF_DT) = 1 THEN 1 ELSE 0 END  TOT_QTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = 2012 THEN 1 ELSE 0 END  TOT_YTD_NBR_NEW_AC
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = 2012 AND MONTH(TMP_NEW_CST.EFF_CST_DT) = 3 THEN 1  ELSE 0 END  TOT_MTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = 2012 AND QUARTER(TMP_NEW_CST.EFF_CST_DT) = 1 THEN 1  ELSE 0 END  TOT_QTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = 2012 THEN 1 ELSE 0 END  TOT_YTD_NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = 2012 and month(DEP_AR_SMY.END_DT) = 3 then 1 else 0 end TOT_MTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = 2012 and quarter(DEP_AR_SMY.END_DT) = 1 then 1 else 0 end TOT_QTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = 2012 then 1 else 0 end TOT_YTD_NBR_AC_CLS
    FROM  SMY.DEP_AR_SMY AS DEP_AR_SMY  
    LEFT OUTER JOIN SESSION.TMP_CUR_AMT  AS TMP_CUR_AMT ON DEP_AR_SMY.DEP_AR_ID = TMP_CUR_AMT.AC_AR_ID AND DEP_AR_SMY.DNMN_CCY_ID = TMP_CUR_AMT.CCY
    LEFT OUTER JOIN SESSION.TMP_NEW_CST  AS TMP_NEW_CST ON DEP_AR_SMY.CST_ID=TMP_NEW_CST.CST_ID
    LEFT OUTER JOIN SOR.ACG_SBJ_CODE_MAPPING AS ACG_MAP ON ACG_MAP.ACG_SBJ_ID = DEP_AR_SMY.ACG_SBJ_ID and ACG_MAP.END_DT = '9999-12-31'
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID);

CREATE INDEX SESSION.IDX_TMP_TMP ON SESSION.TMP_TMP(ACG_OU_IP_ID,DEP_ACG_SBJ_ID,PD_GRP_CD,PD_SUB_CD,DEP_TM_TP_ID,TM_MAT_SEG_ID,ENT_IDV_IND,CCY,AC_CHRCTR_TP_ID,NEW_ACG_SBJ_ID);

INSERT INTO SESSION.TMP_TMP
    SELECT
			    DEP_AR_SMY.RPRG_OU_IP_ID        AS  ACG_OU_IP_ID   
         ,DEP_AR_SMY.ACG_SBJ_ID           AS  DEP_ACG_SBJ_ID 
         ,DEP_AR_SMY.PD_GRP_CODE          AS  PD_GRP_CD      
         ,DEP_AR_SMY.PD_SUB_CODE          AS  PD_SUB_CD      
         ,DEP_AR_SMY.DEP_TP_ID            AS  DEP_TM_TP_ID   
         
         --,DEP_AR_SMY.TM_MAT_SEG_ID        AS  TM_MAT_SEG_ID
         ,CASE WHEN DEP_AR_SMY.DEP_TP_ID=21200012 THEN 
              CASE WHEN DEP_AR_SMY.DEP_PRD_NOM = 0 OR DEP_AR_SMY.DEP_PRD_NOM IS NULL THEN
                  CASE WHEN DEP_AR_SMY.MAT_DT<=SMY_DATE THEN MAT_SEG_ID_1
                       WHEN DEP_AR_SMY.MAT_DT>=SMY_DATE+CNT DAYS THEN MAT_SEG_ID_2
                  ELSE z.MAT_SEG_ID END
              ELSE -1 END
          WHEN DEP_AR_SMY.DEP_TP_ID=21200009 THEN
              CASE WHEN DEP_AR_SMY.MAT_DT<=SMY_DATE THEN MAT_SEG_ID_1
                   WHEN DEP_AR_SMY.MAT_DT>=SMY_DATE+CNT DAYS THEN MAT_SEG_ID_2
              ELSE z.MAT_SEG_ID END
          ELSE -1 END AS TM_MAT_SEG_ID
          
         ,DEP_AR_SMY.ENT_IDV_IND          AS  ENT_IDV_IND    
         ,DEP_AR_SMY.DNMN_CCY_ID          AS  CCY
         ,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')  AS NEW_ACG_SBJ_ID
         ,DEP_AR_SMY.BAL_AMT   AS BAL_AMT
         ,DEP_AR_SMY.CST_ID          AS NBR_CST
         ,CASE WHEN AR_LCS_TP_ID <> 20370008 then 1 else 0 end        AS NBR_AC
         ,CASE WHEN DEP_AR_SMY.EFF_DT=ACCOUNTING_DATE THEN 1 ELSE 0 END   AS NBR_NEW_AC
         ,CASE WHEN TMP_NEW_CST.EFF_CST_DT = SMY_DATE THEN 1 ELSE 0 END       AS NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and DEP_AR_SMY.END_DT = SMY_DATE then 1 else 0 end   AS NBR_AC_CLS
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_CR_AMT ,0)     AS CUR_CR_AMT
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_DB_AMT ,0)     AS CUR_DB_AMT
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID	AS AC_CHRCTR_TP_ID
	  		 ,VALUE(MTD_ACML_BAL_AMT,0) AS MTD_ACML_BAL_AMT
	  		 ,VALUE(QTD_ACML_BAL_AMT,0) AS QTD_ACML_BAL_AMT
	  		 ,VALUE(YTD_ACML_BAL_AMT,0) AS  YTD_ACML_BAL_AMT
	  		 ,VALUE(TOT_MTD_CR_AMT  ,0) AS  TOT_MTD_CR_AMT  
         ,VALUE(TOT_MTD_DB_AMT  ,0) AS  TOT_MTD_DB_AMT  
         ,VALUE(TOT_QTD_CR_AMT  ,0) AS  TOT_QTD_CR_AMT  
				 ,VALUE(TOT_QTD_DB_AMT  ,0) AS  TOT_QTD_DB_AMT  
				 ,VALUE(TOT_YTD_CR_AMT  ,0) AS  TOT_YTD_CR_AMT  
				 ,VALUE(TOT_YTD_DB_AMT  ,0) AS  TOT_YTD_DB_AMT   
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = CUR_YEAR AND MONTH(DEP_AR_SMY.EFF_DT) = CUR_MONTH THEN 1 ELSE 0 END TOT_MTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = CUR_YEAR AND QUARTER(DEP_AR_SMY.EFF_DT) = CUR_QTR THEN 1 ELSE 0 END TOT_QTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = CUR_YEAR THEN 1 ELSE 0 END TOT_YTD_NBR_NEW_AC
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = CUR_YEAR AND MONTH(TMP_NEW_CST.EFF_CST_DT) = CUR_MONTH THEN 1 ELSE 0 END TOT_MTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = CUR_YEAR AND QUARTER(TMP_NEW_CST.EFF_CST_DT) = CUR_QTR THEN 1 ELSE 0 END TOT_QTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = CUR_YEAR THEN 1 ELSE 0 END TOT_YTD_NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = cur_year and month(DEP_AR_SMY.END_DT) = cur_month then 1 else 0 end TOT_MTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = cur_year and quarter(DEP_AR_SMY.END_DT) = cur_qtr then 1 else 0 end TOT_QTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = cur_year then 1 else 0 end TOT_YTD_NBR_AC_CLS
    FROM  SMY.DEP_AR_SMY AS DEP_AR_SMY  
    LEFT OUTER JOIN SESSION.TMP_CUR_AMT  AS TMP_CUR_AMT ON DEP_AR_SMY.DEP_AR_ID = TMP_CUR_AMT.AC_AR_ID AND DEP_AR_SMY.DNMN_CCY_ID = TMP_CUR_AMT.CCY
    LEFT OUTER JOIN SESSION.TMP_NEW_CST  AS TMP_NEW_CST ON DEP_AR_SMY.CST_ID=TMP_NEW_CST.CST_ID
    LEFT OUTER JOIN SOR.ACG_SBJ_CODE_MAPPING AS ACG_MAP ON ACG_MAP.ACG_SBJ_ID = DEP_AR_SMY.ACG_SBJ_ID and ACG_MAP.END_DT = '9999-12-31'
    LEFT OUTER JOIN SMY.SMY_DT z ON DEP_AR_SMY.MAT_DT=z.SMY_DT
;

INSERT INTO SESSION.TMP_CUR 
    SELECT
			    ACG_OU_IP_ID   
         ,DEP_ACG_SBJ_ID 
         ,PD_GRP_CD      
         ,PD_SUB_CD      
         ,DEP_TM_TP_ID   
         ,TM_MAT_SEG_ID
         ,ENT_IDV_IND    
         ,CCY
         ,ACCOUNTING_DATE      AS ACG_DT                 --����
         ,CUR_YEAR      AS CDR_YR                        --���
         ,CUR_MONTH      AS CDR_MTH                      --�·�
         ,C_MON_DAY      AS NOD_IN_MTH                   --����Ч����
         ,C_QTR_DAY      AS NOD_IN_QTR                   --������Ч����
         ,C_YR_DAY      AS NOD_IN_YEAR                   --����Ч����	
         ,NEW_ACG_SBJ_ID                                 --�¿�Ŀ
         ,SUM(BAL_AMT)   AS BAL_AMT                      --���                  
         ,COUNT(DISTINCT NBR_CST)          AS NBR_CST    --�ͻ���
         ,SUM(NBR_AC)        AS NBR_AC                   --�˻���
         ,SUM(NBR_NEW_AC)   AS NBR_NEW_AC                --�����˻���        
         ,SUM(NBR_NEW_CST)       AS NBR_NEW_CST          --�����ͻ���
         ,sum(NBR_AC_CLS)   AS NBR_AC_CLS                --���������˻��� 20370008: ����
         ,SUM(CUR_CR_AMT)     AS CUR_CR_AMT              --����������
         ,SUM(CUR_DB_AMT)     AS CUR_DB_AMT              --�跽������
         ,AC_CHRCTR_TP_ID                                --�˻�����           
	  		 ,SUM(MTD_ACML_BAL_AMT) AS MTD_ACML_BAL_AMT
	  		 ,SUM(QTD_ACML_BAL_AMT) AS QTD_ACML_BAL_AMT
	  		 ,SUM(YTD_ACML_BAL_AMT) AS  YTD_ACML_BAL_AMT
	  		 ,SUM(TOT_MTD_CR_AMT) AS  TOT_MTD_CR_AMT  
         ,SUM(TOT_MTD_DB_AMT) AS  TOT_MTD_DB_AMT  
         ,SUM(TOT_QTD_CR_AMT) AS  TOT_QTD_CR_AMT  
				 ,SUM(TOT_QTD_DB_AMT) AS  TOT_QTD_DB_AMT  
				 ,SUM(TOT_YTD_CR_AMT) AS  TOT_YTD_CR_AMT  
				 ,SUM(TOT_YTD_DB_AMT) AS  TOT_YTD_DB_AMT   
         ,SUM(TOT_MTD_NBR_NEW_AC)  TOT_MTD_NBR_NEW_AC    --�������˻���
         ,SUM(TOT_QTD_NBR_NEW_AC)  TOT_QTD_NBR_NEW_AC    --�������˻���
         ,SUM(TOT_YTD_NBR_NEW_AC)  TOT_YTD_NBR_NEW_AC    --�������˻���
         ,SUM(TOT_MTD_NBR_NEW_CST)  TOT_MTD_NBR_NEW_CST  --�������ͻ���
         ,SUM(TOT_QTD_NBR_NEW_CST)  TOT_QTD_NBR_NEW_CST  --�������ͻ���
         ,SUM(TOT_YTD_NBR_NEW_CST)  TOT_YTD_NBR_NEW_CST  --�������ͻ���
         ,sum(TOT_MTD_NBR_AC_CLS)  TOT_MTD_NBR_AC_CLS    --���ۼ������˻���
         ,sum(TOT_QTD_NBR_AC_CLS)  TOT_QTD_NBR_AC_CLS    --���ۼ������˻���
         ,sum(TOT_YTD_NBR_AC_CLS)  TOT_YTD_NBR_AC_CLS    --���ۼ������˻���					  
    FROM SESSION.TMP_TMP
    GROUP BY 
			    ACG_OU_IP_ID
         ,DEP_ACG_SBJ_ID
         ,PD_GRP_CD
         ,PD_SUB_CD
         ,DEP_TM_TP_ID
         ,TM_MAT_SEG_ID
         ,ENT_IDV_IND
         ,CCY
         ,AC_CHRCTR_TP_ID
         ,NEW_ACG_SBJ_ID
;

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	
 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  		            --

		--SET SMY_STEPNUM = 6 ;--
		SET SMY_STEPDESC = '���뵱�ջ�����ʱ����';--

	INSERT INTO SMY.OU_DEP_DLY_SMY
 (
            ACG_OU_IP_ID         --�������          
           ,DEP_ACG_SBJ_ID       --����Ŀ�������룩
           ,PD_GRP_CD            --��Ʒ�����        
           ,PD_SUB_CD            --��Ʒ�ִ���        
           ,DEP_TM_TP_ID         --�����������      
           ,TM_MAT_SEG_ID        --������������      
           ,ENT_IDV_IND          --��ҵ/���˱�־     
           ,CCY                  --����
           ,ACG_DT               --����
           ,CDR_YR               --���
           ,CDR_MTH              --�·�
           ,NOD_IN_MTH           --����Ч����
           ,NOD_IN_QTR           --������Ч����
           ,NOD_IN_YEAR          --����Ч����
           ,NEW_ACG_SBJ_ID       --�¿�Ŀ
           ,BAL_AMT              --���
           ,NBR_CST              --�ͻ���
           ,NBR_AC               --�˻���
           ,NBR_NEW_AC           --�����˻���        
           ,NBR_NEW_CST          --�����ͻ���
           ,NBR_AC_CLS           --���������˻���
           ,CUR_CR_AMT           --����������
           ,CUR_DB_AMT           --�跽������
           ,TOT_MTD_NBR_NEW_AC   --�������˻���
           ,TOT_QTD_NBR_NEW_AC   --�������˻���
           ,TOT_YTD_NBR_NEW_AC   --�������˻���
           ,TOT_MTD_NBR_NEW_CST  --�������ͻ���
           ,TOT_QTD_NBR_NEW_CST  --�������ͻ���
           ,TOT_YTD_NBR_NEW_CST  --�������ͻ���
           ,TOT_MTD_NBR_AC_CLS   --���ۼ������˻���
           ,TOT_QTD_NBR_AC_CLS   --���ۼ������˻���
           ,TOT_YTD_NBR_AC_CLS   --���ۼ������˻���
           ,TOT_MTD_CR_AMT       --�´���������
           ,TOT_QTD_CR_AMT       --������������
           ,TOT_YTD_CR_AMT       --�����������
           ,TOT_MTD_DB_AMT       --�½跽������
           ,TOT_QTD_DB_AMT       --���跽������
           ,TOT_YTD_DB_AMT       --��跽������
           ,MTD_ACML_BAL_AMT     --���ۼ����
           ,QTD_ACML_BAL_AMT     --���ۼ����
           ,YTD_ACML_BAL_AMT     --���ۼ����           
           ,AC_CHRCTR_TP_ID	--�˻�����            
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------
            )    
SELECT
            CUR.ACG_OU_IP_ID         --�������          
           ,CUR.DEP_ACG_SBJ_ID       --����Ŀ�������룩
           ,COALESCE(CUR.PD_GRP_CD,'')            --��Ʒ�����        
           ,COALESCE(CUR.PD_SUB_CD,'')            --��Ʒ�ִ���        
           ,CUR.DEP_TM_TP_ID         --�����������      
           ,COALESCE(CUR.TM_MAT_SEG_ID,-1)        --������������      
           ,CUR.ENT_IDV_IND          --��ҵ/���˱�־     
           ,CUR.CCY                  --����
           ,CUR.ACG_DT               --����
           ,CUR.CDR_YR               --���
           ,CUR.CDR_MTH              --�·�
           ,CUR.NOD_IN_MTH           --����Ч����
           ,CUR.NOD_IN_QTR           --������Ч����
           ,CUR.NOD_IN_YEAR          --����Ч����
           ,CUR.NEW_ACG_SBJ_ID       --�¿�Ŀ
           ,CUR.BAL_AMT              --���
           ,case when NBR_AC = 0 then 0 else CUR.NBR_CST end             --�ͻ���
           ,CUR.NBR_AC               --�˻���
           ,CUR.NBR_NEW_AC           --�����˻���        
           ,CUR.NBR_NEW_CST          --�����ͻ���
           ,CUR.NBR_AC_CLS           --���������˻���
           ,CUR.CUR_CR_AMT           --����������
           ,CUR.CUR_DB_AMT           --�跽������
           ----------------------------------Start on 20100225----------------------------------------------
           /*

           ,CUR.NBR_NEW_AC --�������˻���

           ,CUR.NBR_NEW_AC --�������˻���

           ,CUR.NBR_NEW_AC --�������˻���

           ,CUR.NBR_NEW_CST --�������ͻ���

           ,CUR.NBR_NEW_CST --�������ͻ���

           ,CUR.NBR_NEW_CST --�������ͻ���

           ,CUR.NBR_AC_CLS --���ۼ������˻���

           ,CUR.NBR_AC_CLS --���ۼ������˻���

           ,CUR.NBR_AC_CLS --���ۼ������˻���

           ,CUR.CUR_CR_AMT  --�´���������

           ,CUR.CUR_CR_AMT  --������������

           ,CUR.CUR_CR_AMT  --�����������

           ,CUR.CUR_DB_AMT --�½跽������

           ,CUR.CUR_DB_AMT --���跽������

           ,CUR.CUR_DB_AMT --��跽������

           ,CUR.BAL_AMT --���ۼ����

           ,CUR.BAL_AMT --���ۼ����

           ,CUR.BAL_AMT --���ۼ����

           */
           ,TOT_MTD_NBR_NEW_AC   --�������˻���
           ,TOT_QTD_NBR_NEW_AC   --�������˻���
           ,TOT_YTD_NBR_NEW_AC   --�������˻���
           ,TOT_MTD_NBR_NEW_CST  --�������ͻ���
           ,TOT_QTD_NBR_NEW_CST  --�������ͻ���
           ,TOT_YTD_NBR_NEW_CST  --�������ͻ���
           ,TOT_MTD_NBR_AC_CLS   --���ۼ������˻���
           ,TOT_QTD_NBR_AC_CLS   --���ۼ������˻���
           ,TOT_YTD_NBR_AC_CLS   --���ۼ������˻���
           ,TOT_MTD_CR_AMT       --�´���������
           ,TOT_QTD_CR_AMT       --������������
           ,TOT_YTD_CR_AMT       --�����������
           ,TOT_MTD_DB_AMT       --�½跽������
           ,TOT_QTD_DB_AMT       --���跽������
           ,TOT_YTD_DB_AMT       --��跽������
           ,MTD_ACML_BAL_AMT     --���ۼ����
           ,QTD_ACML_BAL_AMT     --���ۼ����
           ,YTD_ACML_BAL_AMT     --���ۼ����  
           ----------------------------------End on 20100225----------------------------------------------
           ,CUR.AC_CHRCTR_TP_ID	--�˻�����                          	     
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,CUR_DAY                 --������������
         ,QTR_DAY                 --������������
         ,YR_DAY                  --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------
	 FROM SESSION.TMP_CUR  AS CUR
;--
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

  SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

/*�±�Ĳ���*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
  		SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
		  SET SMY_STEPDESC = '����������Ϊ�������һ��,���±�SMY.OU_DEP_MTHLY_SMY �в�������';   	--
		  
	INSERT INTO SMY.OU_DEP_MTHLY_SMY
 (
      ACG_OU_IP_ID         --�������          
     ,DEP_ACG_SBJ_ID       --����Ŀ�������룩
     ,PD_GRP_CD            --��Ʒ�����        
     ,PD_SUB_CD            --��Ʒ�ִ���        
     ,DEP_TM_TP_ID         --�����������      
     ,TM_MAT_SEG_ID        --������������      
     ,ENT_IDV_IND          --��ҵ/���˱�־     
     ,CCY                  --����
     ,ACG_DT               --����
     ,CDR_YR               --���
     ,CDR_MTH              --�·�
     ,NOD_IN_MTH           --����Ч����
     ,NOD_IN_QTR           --������Ч����
     ,NOD_IN_YEAR          --����Ч����
     ,NEW_ACG_SBJ_ID       --�¿�Ŀ
     ,BAL_AMT              --���
     ,NBR_CST              --�ͻ���
     ,NBR_AC               --�˻���
     ,NBR_NEW_AC           --�����˻���        
     ,NBR_NEW_CST          --�����ͻ���
     ,NBR_AC_CLS           --���������˻���
     ,CUR_CR_AMT           --����������
     ,CUR_DB_AMT           --�跽������
     ,TOT_MTD_NBR_NEW_AC   --�������˻���
     ,TOT_QTD_NBR_NEW_AC   --�������˻���
     ,TOT_YTD_NBR_NEW_AC   --�������˻���
     ,TOT_MTD_NBR_NEW_CST  --�������ͻ���
     ,TOT_QTD_NBR_NEW_CST  --�������ͻ���
     ,TOT_YTD_NBR_NEW_CST  --�������ͻ���
     ,TOT_MTD_NBR_AC_CLS   --���ۼ������˻���
     ,TOT_QTD_NBR_AC_CLS   --���ۼ������˻���
     ,TOT_YTD_NBR_AC_CLS   --���ۼ������˻���
     ,TOT_MTD_CR_AMT       --�´���������
     ,TOT_QTD_CR_AMT       --������������
     ,TOT_YTD_CR_AMT       --�����������
     ,TOT_MTD_DB_AMT       --�½跽������
     ,TOT_QTD_DB_AMT       --���跽������
     ,TOT_YTD_DB_AMT       --��跽������
     ,MTD_ACML_BAL_AMT     --���ۼ����
     ,QTD_ACML_BAL_AMT     --���ۼ����
     ,YTD_ACML_BAL_AMT     --���ۼ����
     ,AC_CHRCTR_TP_ID	--�˻�����            
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------
            )
   SELECT 
      ACG_OU_IP_ID         --�������          
     ,DEP_ACG_SBJ_ID       --����Ŀ�������룩
     ,COALESCE(PD_GRP_CD,'')            --��Ʒ�����        
     ,COALESCE(PD_SUB_CD,'')            --��Ʒ�ִ���        
     ,DEP_TM_TP_ID         --�����������      
     ,TM_MAT_SEG_ID        --������������      
     ,ENT_IDV_IND          --��ҵ/���˱�־     
     ,CCY                  --����
     ,ACG_DT               --����
     ,CDR_YR               --���
     ,CDR_MTH              --�·�
     ,NOD_IN_MTH           --����Ч����
     ,NOD_IN_QTR           --������Ч����
     ,NOD_IN_YEAR          --����Ч����
     ,NEW_ACG_SBJ_ID       --�¿�Ŀ
     ,BAL_AMT              --���
     ,NBR_CST              --�ͻ���
     ,NBR_AC               --�˻���
     ,NBR_NEW_AC           --�����˻���        
     ,NBR_NEW_CST          --�����ͻ���
     ,NBR_AC_CLS           --���������˻���
     ,CUR_CR_AMT           --����������
     ,CUR_DB_AMT           --�跽������
     ,TOT_MTD_NBR_NEW_AC   --�������˻���
     ,TOT_QTD_NBR_NEW_AC   --�������˻���
     ,TOT_YTD_NBR_NEW_AC   --�������˻���
     ,TOT_MTD_NBR_NEW_CST  --�������ͻ���
     ,TOT_QTD_NBR_NEW_CST  --�������ͻ���
     ,TOT_YTD_NBR_NEW_CST  --�������ͻ���
     ,TOT_MTD_NBR_AC_CLS   --���ۼ������˻���
     ,TOT_QTD_NBR_AC_CLS   --���ۼ������˻���
     ,TOT_YTD_NBR_AC_CLS   --���ۼ������˻���
     ,TOT_MTD_CR_AMT       --�´���������
     ,TOT_QTD_CR_AMT       --������������
     ,TOT_YTD_CR_AMT       --�����������
     ,TOT_MTD_DB_AMT       --�½跽������
     ,TOT_QTD_DB_AMT       --���跽������
     ,TOT_YTD_DB_AMT       --��跽������
     ,MTD_ACML_BAL_AMT     --���ۼ����
     ,QTD_ACML_BAL_AMT     --���ۼ����
     ,YTD_ACML_BAL_AMT     --���ۼ����
     ,AC_CHRCTR_TP_ID	--�˻�����  
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------
   FROM SMY.OU_DEP_DLY_SMY WHERE ACG_DT=ACCOUNTING_DATE ;--

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	     --
  END IF;--

	COMMIT;--
END@