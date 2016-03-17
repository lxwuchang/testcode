CREATE PROCEDURE SMY.PROC_OU_ACG_SBJ_BAL_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_OU_ACG_SBJ_BAL_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_OU_ACG_SBJ_BAL_DLY_SMY
-- Source Table:				sor.FT_DEP_AR union SOR.DMD_DEP_SUB_AR union SOR.INTRBNK_DEP_SUB_AR union SOR.LOAN_AR union SOR.DCN_CTR_AR union SOR.EQTY_AC_SUB_AR union SOR.ON_BST_AC_AR union SOR.OFF_BST_AC_AR
-- Target Table: 				SMY.OU_ACG_SBJ_BAL_DLY_SMY
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
-- 2009-11-17   Xu Yan          Modified some condition statements
-- 2009-11-21   Xu Yan          Added a new column BAL_ACG_EFF_TP_Id
-- 2009-11-23   Xu Yan          Added a new column NBR_AC
-- 2009-11-24   SHANG           �����±���
-- 2009-11-27   Xu Yan          Updated the column 'NEW_ACG_SBJ_ID'
-- 2009-11-30   Xu Yan          Updated 'NEW_ACG_SBJ_ID' joint table 
-- 2009-12-01   Xu Yan          Added a new column 'NBR_AC_WITH_BAL'
-- 2009-12-02   Xu Yan          Updated 'NEW_ACG_SBJ_ID' due to the SOR change
-- 2010-01-28   Xu Yan          Inserted the records of the last day which do not exist on the current day
-- 2010-08-10   Fang Yihua      Added three new columns 'NOCLD_IN_MTH','NOCLD_IN_QTR','NOCLD_IN_YEAR'
-- 2010-10-27   Xu Yan          Set the variable MTH_LAST_DAY
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN

/*�����쳣����ʹ�ñ���*/
		DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 0;                     --�����ڲ�λ�ñ��
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
		DECLARE CUR_QTR SMALLINT;--
		-- ���������µ����һ��
		DECLARE MTH_LAST_DAY DATE; 		    --

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
      SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
    
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      SET SMY_SQLCODE = SQLCODE;--
      SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
    END;--

   /*������ֵ*/
    SET SMY_PROCNM  ='PROC_OU_ACG_SBJ_BAL_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ���
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    ------------------------------------Start on 20101027------------------------------
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;  --
    ------------------------------------End on 20101027--------------------------------
    
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
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY;--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;--
		SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
       COMMIT;--
    END IF;--

/*�±�Ļָ�*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_ACG_SBJ_BAL_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
   		COMMIT;--
   	END IF;   --

--SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';--

	/*�����û���ʱ��*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.OU_ACG_SBJ_BAL_DLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID);--

 /*��������һ�ղ���Ҫ����*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
     ACG_OU_IP_ID       --�������
    ,ACG_SBJ_ID         --��Ŀ�������룩
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --����
    ------------------------End of modification on 2009-11-21--------------------------------------
    ,CCY                --����
    ,ACG_DT             --����
    ,CDR_YR             --���
    ,CDR_MTH            --�·�MM
    ,NOD_In_MTH         --������Ч����  
    ,NOD_In_QTR         --������Ч����
    ,NOD_In_Year        --������Ч����
    ,NEW_ACG_SBJ_ID     --�¿�Ŀ
    ,BAL_AMT            --���
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --�˻���
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --���ۼ����
    ,QTD_ACML_BAL_AMT   --���ۼ����
    ,YTD_ACML_BAL_AMT   --���ۼ����
    ------------------------Start of 2009-12-01-----------------------------------------------
    ,NBR_AC_WITH_BAL    --������˻���
    ------------------------End of 2009-12-01-----------------------------------------------    
         ) 
    SELECT 
     ACG_OU_IP_ID       --�������
    ,ACG_SBJ_ID         --��Ŀ�������룩
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --����
    ------------------------End of modification on 2009-11-21--------------------------------------
    ,CCY                --����
    ,ACG_DT             --����
    ,CDR_YR             --���
    ,CDR_MTH            --�·�MM
    ,NOD_In_MTH         --������Ч����  
    ,NOD_In_QTR         --������Ч����
    ,NOD_In_Year        --������Ч����
    ,NEW_ACG_SBJ_ID     --�¿�Ŀ
    ,BAL_AMT            --���
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --�˻���
    ------------------------End of modification on 2009-11-23--------------------------------------    
    ,MTD_ACML_BAL_AMT   --���ۼ����
    ,QTD_ACML_BAL_AMT   --���ۼ����
    ,YTD_ACML_BAL_AMT   --���ۼ����      
    ------------------------Start of 2009-12-01-----------------------------------------------
    ,NBR_AC_WITH_BAL    --������˻���
    ------------------------End of 2009-12-01-----------------------------------------------
  FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
 
END IF;  --
      
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --

	 IF  ACCOUNTING_DATE IN (YR_FIRST_DAY )  --�� �� �� ����
	 		THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
            NOD_In_MTH       =0 --������Ч����  
           ,NOD_In_QTR       =0 --������Ч����
           ,NOD_In_Year      =0 --������Ч����
           ,MTD_ACML_BAL_AMT =0 --���ۼ����
           ,QTD_ACML_BAL_AMT =0 --���ۼ����
           ,YTD_ACML_BAL_AMT =0 --���ۼ����   
	 			;--
	 ELSEIF ACCOUNTING_DATE IN (QTR_FIRST_DAY) --�� �� ����
	 	  THEN
	 			UPDATE SESSION.TMP 
	 				SET 
            NOD_In_MTH       =0 --������Ч����  
           ,NOD_In_QTR       =0 --������Ч����
           ,MTD_ACML_BAL_AMT =0 --���ۼ����
           ,QTD_ACML_BAL_AMT =0 --���ۼ����
	 	  	;--
	 ELSEIF ACCOUNTING_DATE IN (MTH_FIRST_DAY) --�¹���
	 	  THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
            NOD_In_MTH       =0 --������Ч����  
           ,MTD_ACML_BAL_AMT =0 --���ۼ����
	 			;	 	--
	 END IF;--

		--SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '�����û���ʱ��,���8�Ŵ�SOR����������ͳ������';	 --

	DECLARE GLOBAL TEMPORARY TABLE TMP_BAL_AMT AS 
  	(  	
  	  ------------------------Start of modification on 2009-11-23--------------------------------------  
  	  /*      
  		  SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.FT_DEP_AR            GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.DMD_DEP_SUB_AR       GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.INTRBNK_DEP_SUB_AR   GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.LOAN_AR              GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.DCN_CTR_AR           GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.EQTY_AC_SUB_AR       GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.ON_BST_AC_AR         GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.OFF_BST_AC_AR        GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID    
      */
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT, COUNT(DISTINCT INR_AC_AR_ID) AS NBR_AC, COUNT(1) AS NBR_AC_WITH_BAL FROM  SOR.OFF_BST_AC_AR        GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID    
  	) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     PARTITIONING KEY(ACG_OU_IP_ID);--

 INSERT INTO SESSION.TMP_BAL_AMT (
 				ACG_OU_IP_ID             --�������      
 			 ,ACG_SBJ_ID                --��Ŀ�������룩
 			 ,CCY               --����
 			 ,BAL_AMT										--���
 			 ,NBR_AC										--�˻���
 			 ,NBR_AC_WITH_BAL						--�������˻���
 )
 WITH TMP_AC as (
 					--���ڴ�� ����Ŀ
			    select 
			         DMD_DEP_AR_ID as AC_AR_ID
			        ,RPRG_OU_IP_ID
			        ,ACG_SBJ_ID
			        ,DNMN_CCY_ID
			        ,SUM(BAL_AMT) AS BAL_AMT        
			    from SOR.DMD_DEP_SUB_AR DMD
			    where DMD.DEL_F = 0
								and
								AR_LCS_TP_ID <> 20370008 -- ����					
								and
								BAL_AMT>=0									
				  GROUP BY DMD_DEP_AR_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID
				  
				  union all
	        --���ڴ�� ͸֧��Ŀ
          SELECT DMD_DEP_AR_ID AS AC_AR_ID 
                ,RPRG_OU_IP_ID
          			,OD_ACG_SBJ_ID AS ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT           			
        	FROM  SOR.DMD_DEP_SUB_AR  DMD     
          where DMD.DEL_F = 0
								and
								AR_LCS_TP_ID <> 20370008 -- ����					
								and
								BAL_AMT < 0									
          GROUP BY DMD_DEP_AR_ID,RPRG_OU_IP_ID,OD_ACG_SBJ_ID,DNMN_CCY_ID 
	  
	  			UNION ALL
	  			--ͬҵ���
          SELECT INTRBNK_DEP_AR_ID as AC_AR_ID
          			,RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT           			
          	FROM  SOR.INTRBNK_DEP_SUB_AR   SUB
          	where SUB.DEL_F = 0
						      and
						      SUB.AR_LCS_TP_ID <> 20370008 -- ����			
          	GROUP BY INTRBNK_DEP_AR_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 

          UNION ALL
          --�ɽ�
          SELECT EQTY_AC_AR_ID as AC_AR_ID
          			,RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT           			
          	FROM  SOR.EQTY_AC_SUB_AR    SUB
          	where SUB.DEL_F = 0
						  		and
						  		SUB.AR_LCS_TP_ID <> 20370008 --����   
          	GROUP BY EQTY_AC_AR_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
   
 ),
  T AS (  					
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			,SUM(BAL_AMT) AS BAL_AMT
          			,COUNT(1) AS NBR_AC
          		  ,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          from TMP_AC
          GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
					----------------------------Start of Modification on 2009-11-17----------------------------------------        
          --���ڴ��
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT 
          			------------------------Start of modification on 2009-11-23--------------------------------------  
          			, count(1) as NBR_AC
          			------------------------End of modification on 2009-11-23--------------------------------------            			
          			,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.FT_DEP_AR  FT         
          	WHERE FT.DEL_F = 0
						      AND
						      AR_LCS_TP_ID<> 20370008  -- ����
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          					
          union all
				    --���ÿ��ֻ�	����Ŀ
						select
							 RPRG_OU_IP_ID
							,DEP_ACG_SBJ_ID as ACG_SBJ_ID --Accounting Subject Item							
							,DNMN_CCY_ID 
							,SUM(BAL_AMT) as BAL_AMT	
							------------------------Start of modification on 2009-11-23--------------------------------------  
          		, count(1) as NBR_AC          		
          		------------------------End of modification on 2009-11-23--------------------------------------  				
          		,SUM(case when BAL_AMT>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
						from SOR.CC_AC_AR CC								 
						where CC.DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 20370007 --����
						  		and
						  		BAL_AMT >= 0 		  		
						GROUP BY RPRG_OU_IP_ID,DEP_ACG_SBJ_ID,DNMN_CCY_ID 
          
          union all
				    --���ÿ��ֻ�	͸֧��Ŀ
						select
							 RPRG_OU_IP_ID
							,OD_ACG_SBJ_ID as ACG_SBJ_ID --Accounting Subject Item							
							,DNMN_CCY_ID 
							,-SUM(BAL_AMT) as BAL_AMT	
							------------------------Start of modification on 2009-11-23--------------------------------------  
          		,count(1) as NBR_AC
          		------------------------End of modification on 2009-11-23--------------------------------------  								
          		,count(1) AS NBR_AC_WITH_BAL
						from SOR.CC_AC_AR CC								 
						where CC.DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 20370007 --����
						  		and
						  		BAL_AMT < 0 		  		
						GROUP BY RPRG_OU_IP_ID,OD_ACG_SBJ_ID,DNMN_CCY_ID
						 
          UNION ALL
          --���Ҵ���ֻ� BLFMAMTZ, ��ͨ����
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT 
								------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.LOAN_AR LN
          	WHERE LN.DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 13360003 --����             
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
          --���ַֻ�
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT 
          			------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.DCN_CTR_AR   DCN
          	where DCN.DEL_F = 0
		  						and
		  						AR_LCS_TP_ID = 13360003 --����
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
          --�����ڲ��˻�
          
          SELECT OBST.RPRG_OU_IP_ID as RPRG_OU_IP_ID
          			,OBST.ACG_SBJ_ID as ACG_SBJ_ID
          			,OBST.DNMN_CCY_ID as DNMN_CCY_ID
          	--------------------Start of modification on 2009-11-21---------------------------------------
          			,sum(case when OBST.BAL_ACG_EFF_TP_ID <> GACC.BAL_ACG_EFF_TP_ID AND OBST.BAL_ACG_EFF_TP_ID = 15070002 then -BAL_AMT else BAL_AMT end ) as BAL_AMT											
          			------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.ON_BST_AC_AR OBST
          				left join SOR.ACG_SBJ_ITM GACC on OBST.ACG_SBJ_ID = GACC.ACG_SBJ_ID
           --------------------End of modification on 2009-11-21---------------------------------------
          	where OBST.DEL_F = 0
						  		and
						  		OBST.AR_LCS_TP_ID = 20370007 --����		  				  					  	       
          	GROUP BY RPRG_OU_IP_ID,OBST.ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
          --�����ڲ��˻�
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT
          			------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			 
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.OFF_BST_AC_AR 
          	WHERE DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 20370007 --����	       
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID
          ----------------------------End of Modification on 2009-11-17----------------------------------------                                           
  )
   SELECT    
        RPRG_OU_IP_ID           --�������             
       ,ACG_SBJ_ID               --��Ŀ�������룩      
       , DNMN_CCY_ID             --����                
       , SUM(BAL_AMT)          	--���                 
       , SUM(NBR_AC)           	--�˻���               
       , SUM(NBR_AC_WITH_BAL)  	--�������˻���       
   FROM  T
   GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID   
;    --
 ---------------------------Start on 2010-01-28-----------------------------------------------
 create index SESSION.IDX_TMP_BAL on SESSION.TMP_BAL_AMT(ACG_OU_IP_ID, ACG_SBJ_ID, CCY);--
 create index SESSION.IDX_TMP on SESSION.TMP(ACG_OU_IP_ID, ACG_SBJ_ID, CCY);--
 
 Insert into SESSION.TMP_BAL_AMT
	 select  ACG_OU_IP_ID       --�������
			    ,ACG_SBJ_ID         --��Ŀ�������룩		    
			    ,CCY                --����
					,0                  --���    
	    		,0                  --�˻���
	    		,0              		--�������˻���  		    
	 from SESSION.TMP as PRE
	 where not exists (
	 		select
						1		
	 		from SESSION.TMP_BAL_AMT CUR
	 		where  CUR.ACG_OU_IP_ID = PRE.ACG_OU_IP_ID
       AND  CUR.ACG_SBJ_ID   = PRE.ACG_SBJ_ID  
       AND  CUR.CCY   			 = PRE.CCY 
	 );  --
	---------------------------End on 2010-01-28-----------------------------------------------

 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  --
	 

		--SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '����SMY.OU_ACG_SBJ_BAL_DLY_SMY �в���������Ϊ���������';--
		

		INSERT INTO SMY.OU_ACG_SBJ_BAL_DLY_SMY
 	(
     ACG_OU_IP_ID       --�������
    ,ACG_SBJ_ID         --��Ŀ�������룩
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --����
    ------------------------End of modification on 2009-11-21--------------------------------------    
    ,CCY                --����
    ,ACG_DT             --����
    ,CDR_YR             --���
    ,CDR_MTH            --�·�MM
    ,NOD_IN_MTH         --������Ч����  
    ,NOD_IN_QTR         --������Ч����
    ,NOD_IN_YEAR        --������Ч����
    ,NEW_ACG_SBJ_ID     --�¿�Ŀ
    ,BAL_AMT            --���
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --�˻���
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --���ۼ����
    ,QTD_ACML_BAL_AMT   --���ۼ����
    ,YTD_ACML_BAL_AMT   --���ۼ����
    ,NBR_AC_WITH_BAL						--�������˻���  
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------

      )              
		SELECT  
          CUR.ACG_OU_IP_ID       --�������
         ,CUR.ACG_SBJ_ID         --��Ŀ�������룩
			   ------------------------Start of modification on 2009-11-21--------------------------------------
			   --���ڽ��˫�����е�Ĭ��ȡ�跽��ͨ�������������ʶ����������
			   ,Value(case when GACC.BAL_ACG_EFF_TP_Id = 15070003 then 15070001 else GACC.BAL_ACG_EFF_TP_Id end,15070001) as BAL_ACG_EFF_TP_Id  --����
			   ------------------------End of modification on 2009-11-21--------------------------------------                  
         ,CUR.CCY                --����
         ,ACCOUNTING_DATE
         ,CUR_YEAR
         ,CUR_MONTH
         ,COALESCE(PRE.NOD_IN_MTH  ,0) + 1		   
         ,COALESCE(PRE.NOD_IN_QTR  ,0) + 1
         ,COALESCE(PRE.NOD_IN_YEAR ,0) + 1 
         ,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')   --�¿�Ŀ
         ,CUR.BAL_AMT
         ------------------------Start of modification on 2009-11-23--------------------------------------
			   ,CUR.NBR_AC             --�˻���
			   ------------------------End of modification on 2009-11-23--------------------------------------
         ,COALESCE(MTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT
         ,COALESCE(QTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT
         ,COALESCE(YTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT
         ,CUR.NBR_AC_WITH_BAL						--�������˻���
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,CUR_DAY                 --������������
         ,QTR_DAY                 --������������
         ,YR_DAY                  --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------
   	FROM SESSION.TMP_BAL_AMT    AS CUR   
   	LEFT OUTER JOIN SESSION.TMP AS PRE
   		ON 
            CUR.ACG_OU_IP_ID = PRE.ACG_OU_IP_ID
       AND  CUR.ACG_SBJ_ID   = PRE.ACG_SBJ_ID  
       AND  CUR.CCY   			 = PRE.CCY 
    ------------------------Start of modification on 2009-11-21--------------------------------------
    left join SOR.ACG_SBJ_ITM GACC on CUR.ACG_SBJ_ID = GACC.ACG_SBJ_ID    
    ------------------------END of modification on 2009-11-21--------------------------------------
    left join SOR.ACG_SBJ_CODE_MAPPING ACG_MAP on ACG_MAP.ACG_SBJ_ID = CUR.ACG_SBJ_ID and ACG_MAP.END_DT = '9999-12-31'
	;	--
		

 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

/*�±�Ĳ���*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN   		
		  SET SMY_STEPDESC = '����������Ϊ�������һ��,���±�SMY.OU_ACG_SBJ_BAL_MTHLY_SMY �в�������';  --
		INSERT INTO SMY.OU_ACG_SBJ_BAL_MTHLY_SMY
 	(
     ACG_OU_IP_ID       --�������
    ,ACG_SBJ_ID         --��Ŀ�������룩
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --����
    ------------------------End of modification on 2009-11-21--------------------------------------    
    ,CCY                --����
    ,ACG_DT             --����
    ,CDR_YR             --���
    ,CDR_MTH            --�·�MM
    ,NOD_IN_MTH         --������Ч����  
    ,NOD_IN_QTR         --������Ч����
    ,NOD_IN_YEAR        --������Ч����
    ,NEW_ACG_SBJ_ID     --�¿�Ŀ
    ,BAL_AMT            --���
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --�˻���
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --���ۼ����
    ,QTD_ACML_BAL_AMT   --���ۼ����
    ,YTD_ACML_BAL_AMT   --���ۼ����  
    ,NBR_AC_WITH_BAL						--�������˻���
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------
      )              
		SELECT 		
     ACG_OU_IP_ID       --�������
    ,ACG_SBJ_ID         --��Ŀ�������룩
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --����
    ------------------------End of modification on 2009-11-21--------------------------------------    
    ,CCY                --����
    ,ACG_DT             --����
    ,CDR_YR             --���
    ,CDR_MTH            --�·�MM
    ,NOD_IN_MTH         --������Ч����  
    ,NOD_IN_QTR         --������Ч����
    ,NOD_IN_YEAR        --������Ч����
    ,NEW_ACG_SBJ_ID     --�¿�Ŀ
    ,BAL_AMT            --���
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --�˻���
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --���ۼ����
    ,QTD_ACML_BAL_AMT   --���ۼ����
    ,YTD_ACML_BAL_AMT   --���ۼ����  
    ,NBR_AC_WITH_BAL						--�������˻���
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --������������
         ,NOCLD_IN_QTR        --������������
         ,NOCLD_IN_YEAR       --������������
------------------------------------End on 2010-08-10 ---------------------------------------------------
  FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
  
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
 END IF;  --


COMMIT;--
END@