CREATE PROCEDURE SMY.PROC_CST_DEP_MTHLY_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------                                              
-- (C) Copyright ZJRCU and IBM <date>                                                                                        
--                                                                                                                           
-- File name:           SMY.PROC_CST_DEP_MTHLY_SMY.sql                                                                       
-- Procedure name: 			SMY.PROC_CST_DEP_MTHLY_SMY                                                                           
-- Source Table:				SMY.DEP_AR_SMY,SMY.CST_INF                                                                           
-- Target Table: 				SMY.CST_DEP_MTHLY_SMY                                                                                
-- Project     :        ZJ RCCB EDW                                                                                          
-- NOTES       :                                                                                                             
-- Purpose     :                                                                                                             
-- PROCESS METHOD      :  UPDATE EACH DAY ,INSERT IN THE PERIOD OF ONE MONTH                                                 
-- OPTIMIZE :                                                                                                                
--            CREATE INDEX SMY.IDX_CST_DEP_MTHLY_SMY_ACG_DT ON SMY.CST_DEP_MTHLY_SMY(ACG_DT);                                --
--            CREATE INDEX SMY.IDX_DEP_AR_SMY_CST_ID ON SMY.DEP_AR_SMY(CST_ID);                                              --
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
-- 2009-12-04   Xu Yan          Rename the history table                                                                     
-- 2009-12-16   Xu Yan          Fixed a bug for reruning                                                                     
-- 2009-12-25   Xu Yan          Rewrite the merge statement to solve the problem which system temp tablespace is full
-- 2010-08-11   Peng Yi tao     Modify the method of calendar days Calculating 
-- 2011-05-31   Chen XiaoWen    1���ϲ�������ʱ��TMP���߼�,��������Ҫ��update
--                              2����������SESSION.IDX_S,�ɰ汾������ʱ��TMP��
-- 2011-08-05   Li Shen Yu      Add if-else clause for step 5 to deal the data of month first day and other days separately
-- 2012-03-16   Chen XiaoWen    1��SMY.CST_DEP_MTHLY_SMY�޸�ԭ��ѯ������ʹ��ACG_DT��������ѯ
--                              2��������ʱ��TMP_CUR�������м������ٽ���group by
--                              3�����һ��INSERT����ACG_DTɸѡ����
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
		DECLARE CUR_YEAR SMALLINT;                                                                                               --
		DECLARE CUR_MONTH SMALLINT;                                                                                              --
		DECLARE CUR_DAY INTEGER;                                                                                                 --
		DECLARE YR_FIRST_DAY DATE;                                                                                               --
		DECLARE QTR_FIRST_DAY DATE;                                                                                              --
		DECLARE YR_DAY SMALLINT;                                                                                                 --
		DECLARE QTR_DAY SMALLINT;                                                                                                --
		DECLARE MAX_ACG_DT DATE;                                                                                                 --
		DECLARE LAST_SMY_DATE DATE;                                                                                              --
		DECLARE MTH_FIRST_DAY DATE;                                                                                              --
		DECLARE V_T SMALLINT;                                                                                                    --
    DECLARE C_YR_DAY SMALLINT;                                                                                               --
		DECLARE C_QTR_DAY SMALLINT;                                                                                              --
		DECLARE QTR_LAST_DAY DATE;                                                                                               --
		DECLARE C_MON_DAY SMALLINT;                                                                                              --
		DECLARE CUR_QTR SMALLINT;                                                                                                --
		DECLARE MON_DAY SMALLINT;                                                                                                --
		DECLARE LAST_MONTH SMALLINT;                                                                                             --
		DECLARE EMP_SQL VARCHAR(200);                                                                                            --
		DECLARE MTH_LAST_DAY DATE;
                                                                                                                             
/*                                                                                                                           
	1.�������SQL�쳣����ľ��(EXIT��ʽ).                                                                                     
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.       
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.                           
*/                                                                                                                           
                                                                                                                             
		DECLARE CONTINUE HANDLER FOR NOT FOUND                                                                                   
		  SET V_T=0 ;                                                                                                            --
		                                                                                                                         
    DECLARE EXIT HANDLER FOR SQLEXCEPTION                                                                                    
    BEGIN                                                                                                                    
    	SET SMY_SQLCODE = SQLCODE;                                                                                             --
      ROLLBACK;                                                                                                              --
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP); --
      COMMIT;                                                                                                                --
      RESIGNAL;                                                                                                              --
    END;                                                                                                                     --
                                                                                                                             
    DECLARE CONTINUE HANDLER FOR SQLWARNING                                                                                  
    BEGIN                                                                                                                    
      SET SMY_SQLCODE = SQLCODE;                                                                                             --
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP); --
      COMMIT;                                                                                                                --
    END;                                                                                                                     --
                                                                                                                             
   /*������ֵ*/                                                                                                              
    SET SMY_PROCNM  ='PROC_CST_DEP_MTHLY_SMY';                                                                               --
    SET SMY_DATE    =ACCOUNTING_DATE;                                                                                        --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���                                                                    
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�                                                                    
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���                                                                  
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ����� 
--------------------------------------------start on 20100811-------------------------------------------------------------                                                     
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ��� 
    SET C_YR_DAY      =DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;--ȡ����ڼ���         
--------------------------------------------end on 20100811-------------------------------------------------------------                          
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����                                                                  
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,iso),1,7)||'-01'); --ȡ���³���                                       
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���                                                                  
                                                                                                                             
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;                                                                      --
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);                                                                                   --
    --��������������   
--------------------------------------------start on 20100811-------------------------------------------------------------                                                                                                          
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);                                                    --
    SET C_MON_DAY = DAY(ACCOUNTING_DATE);                                                                                      --
--------------------------------------------end on 20100811-------------------------------------------------------------                                                                                                                              
    --���㼾����������                                                                                                       
    IF CUR_QTR = 1                                                                                                           
       THEN                                                                                                                  
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');                                                              --
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;                                                     --
    ELSEIF CUR_QTR = 2                                                                                                       
       THEN                                                                                                                  
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');                                                              --
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;       	                                             --
    ELSEIF CUR_QTR = 3                                                                                                       
       THEN                                                                                                                  
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');                                                              --
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;       	                                             --
    ELSE                                                                                                                     
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');                                                               --
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');                                                               --
    END IF;                                                                                                                  --
                                                                                                                             
  /*ȡ������������*/       
--------------------------------------------start on 20100811-------------------------------------------------------------                                                                                                      
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;                                                        --
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;                                                            --
--------------------------------------------end on 20100811-------------------------------------------------------------                                                     --
		                                                                                                                         
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_DEP_MTHLY_SMY;                                    --
                                                                                                                             
/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/                                 
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;                                        --
			COMMIT;                                                                                                                --
		                                                                                                                         
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                  --
		                                                                                                                         
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;                                                                                 --
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                 --
                                                                                                                             
/*���ݻָ��뱸��*/                                                                                                           
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN                                                                                     
       --DELETE FROM SMY.CST_DEP_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;                                    --
       DELETE FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;
    /**ÿ�µ�һ�ղ���Ҫ����ʷ���лָ�**/                                                                                     
       IF MON_DAY <> 1 THEN                                                                                                  
      	 INSERT INTO SMY.CST_DEP_MTHLY_SMY SELECT * FROM HIS.CST_DEP_MTHLY_SMY ;                                             --
       END IF;                                                                                                               --
     ELSE                                                                                                                    
  /** ���hist ���ݱ� **/                                                                                                    
                                                                                                                             
	    SET EMP_SQL= 'Alter TABLE HIS.CST_DEP_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;                      --
		                                                                                                                         
		  EXECUTE IMMEDIATE EMP_SQL;                                                                                             --
                                                                                                                             
      COMMIT;                                                                                                                --
		  /**backup �������� **/                                                                                                 
		                                                                                                                         
		   --INSERT INTO HIS.CST_DEP_MTHLY_SMY SELECT * FROM SMY.CST_DEP_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
		   INSERT INTO HIS.CST_DEP_MTHLY_SMY SELECT * FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;
    END IF;                                                                                                                  --
                                                                                                                             
SET SMY_STEPNUM = 2 ;                                                                                                        --
SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';                                                                         --
                                                                                                                             
	/*�����û���ʱ��*/                                                                                                         
	                                                                                                                           
	DECLARE GLOBAL TEMPORARY TABLE TMP                                                                                         
		LIKE SMY.CST_DEP_MTHLY_SMY                                                                                               
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);                                --
	                                                                                                                           
	--�Ż�by James shang                                                                                                       
	CREATE INDEX SESSION.IDX_TMP ON SESSION.TMP(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY);                            --
                                                                                                                             
 /*��������һ�ղ���Ҫ����*/
	 IF  ACCOUNTING_DATE IN ( YR_FIRST_DAY )  --�� �� �� ����
	    THEN
	      COMMIT;
	 ELSEIF ACCOUNTING_DATE IN (QTR_FIRST_DAY ) --�� �� ����                                                                   
	 	  THEN                                                                                                                   
	 	    INSERT INTO SESSION.TMP
	 	    (                                                                                                                          
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
        )
        SELECT                                                                                                                   
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,0                     --������������                                                                             
           ,0                     --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,0                     --������Ч����                                                                             
           ,0                     --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,0                     --���ۼ����                                                                               
           ,0                     --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����
        FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT= LAST_SMY_DATE;
	 ELSEIF ACCOUNTING_DATE IN ( MTH_FIRST_DAY ) --�¹���                                                                      
	 	  THEN
	 	    INSERT INTO SESSION.TMP
	 	    (                                                                                                                          
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
        )
        SELECT                                                                                                                   
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,0                     --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,0                     --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,0                     --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����			                                                                         
        FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT= LAST_SMY_DATE;
   ELSE
        INSERT INTO SESSION.TMP
        (                                                                                                                          
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
        )
        SELECT                                                                                                                   
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����			                                                                         
        FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT= LAST_SMY_DATE;
	 END IF;

 /** �ռ�������Ϣ */		                                                                                                     
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                    --
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                    
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	                     --
                                                                                                                             
		SET SMY_STEPNUM = 3 ;                                                                                                    --
		SET SMY_STEPDESC = '������ʱ��SESSION.CUR, ��ŵ��ջ��ܺ������'; 		                                                   --
                                                                                                                             
 DECLARE GLOBAL TEMPORARY TABLE CUR AS (                                                                                     
		SELECT                                                                                                                   
          COALESCE(DEP_AR_SMY.CST_ID,'')         AS CST_ID                --�ͻ�����                                                      
         ,DEP_AR_SMY.RPRG_OU_IP_ID  AS AC_OU_ID              --�˻�����������                                                
         ,DEP_AR_SMY.DEP_TP_ID      AS DEP_TP_ID             --�������                                                      
         ,COALESCE(DEP_AR_SMY.PD_GRP_CODE,'')    AS PD_GRP_CD             --��Ʒ�����                                                    
         ,COALESCE(DEP_AR_SMY.PD_SUB_CODE,'')    AS PD_SUB_CD             --��Ʒ�Ӵ���                                                    
         ,DEP_AR_SMY.DNMN_CCY_ID    AS CCY                   --����                                                          
         ,1                         AS NOD_IN_MTH            --������Ч����                                                  
         ,1                         AS NOD_IN_QTR            --������Ч����                                                  
         ,1                         AS NOD_IN_YEAR           --������Ч����		                                               
         ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')     AS OU_ID                 --������                                           
         ,COALESCE(CST_INF.ENT_IDV_IND ,-1)      AS CST_TP_ID             --�ͻ�����                                         
         ,COUNT(DISTINCT DEP_AR_SMY.DEP_AR_ID) AS NBR_AC                --�˻�����                                           
         ,SUM(DEP_AR_SMY.BAL_AMT)   AS DEP_BAL               --������		                                                   
		FROM            SMY.DEP_AR_SMY  AS DEP_AR_SMY                                                                            
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DEP_AR_SMY.CST_ID=CST_INF.CST_ID                                          
		GROUP BY                                                                                                                 
          DEP_AR_SMY.CST_ID                                                                                                  
         ,DEP_AR_SMY.RPRG_OU_IP_ID                                                                                           
         ,DEP_AR_SMY.DEP_TP_ID                                                                                               
         ,DEP_AR_SMY.PD_GRP_CODE                                                                                             
         ,DEP_AR_SMY.PD_SUB_CODE                                                                                             
         ,DEP_AR_SMY.DNMN_CCY_ID                                                                                             
			   ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')                                                                                 
				 ,COALESCE(CST_INF.ENT_IDV_IND ,-1)                                                                                  
                                                                                                                             
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K                                         
     PARTITIONING KEY(CST_ID) ;                                                                                              --

	--�Ż�by James shang                                                                                                       
	CREATE INDEX SESSION.IDX_CUR ON SESSION.CUR(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY);       			               --
                                                                                                                             
  DECLARE GLOBAL TEMPORARY TABLE TMP_CUR AS (
    SELECT
          COALESCE(DEP_AR_SMY.CST_ID,'')         AS CST_ID
         ,DEP_AR_SMY.RPRG_OU_IP_ID  AS AC_OU_ID
         ,DEP_AR_SMY.DEP_TP_ID      AS DEP_TP_ID
         ,COALESCE(DEP_AR_SMY.PD_GRP_CODE,'')    AS PD_GRP_CD
         ,COALESCE(DEP_AR_SMY.PD_SUB_CODE,'')    AS PD_SUB_CD
         ,DEP_AR_SMY.DNMN_CCY_ID    AS CCY
         ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')    AS OU_ID
         ,COALESCE(CST_INF.ENT_IDV_IND ,-1)       AS CST_TP_ID
         ,DEP_AR_SMY.DEP_AR_ID AS NBR_AC
         ,DEP_AR_SMY.BAL_AMT   AS DEP_BAL
		FROM            SMY.DEP_AR_SMY  AS DEP_AR_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DEP_AR_SMY.CST_ID=CST_INF.CST_ID
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);

  CREATE INDEX SESSION.IDX_TMP_CUR ON SESSION.TMP_CUR(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY,OU_ID,CST_TP_ID);

  INSERT INTO SESSION.TMP_CUR
    SELECT
          COALESCE(DEP_AR_SMY.CST_ID,'')         AS CST_ID
         ,DEP_AR_SMY.RPRG_OU_IP_ID  AS AC_OU_ID
         ,DEP_AR_SMY.DEP_TP_ID      AS DEP_TP_ID
         ,COALESCE(DEP_AR_SMY.PD_GRP_CODE,'')    AS PD_GRP_CD
         ,COALESCE(DEP_AR_SMY.PD_SUB_CODE,'')    AS PD_SUB_CD
         ,DEP_AR_SMY.DNMN_CCY_ID    AS CCY
         ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')    AS OU_ID
         ,COALESCE(CST_INF.ENT_IDV_IND ,-1)       AS CST_TP_ID
         ,DEP_AR_SMY.DEP_AR_ID AS NBR_AC
         ,DEP_AR_SMY.BAL_AMT   AS DEP_BAL
		FROM            SMY.DEP_AR_SMY  AS DEP_AR_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DEP_AR_SMY.CST_ID=CST_INF.CST_ID
;

  INSERT INTO SESSION.CUR
		SELECT                                                                                                                   
          CST_ID
         ,AC_OU_ID
         ,DEP_TP_ID
         ,PD_GRP_CD
         ,PD_SUB_CD
         ,CCY
         ,1
         ,1
         ,1
         ,OU_ID
         ,CST_TP_ID
         ,COUNT(DISTINCT NBR_AC) AS NBR_AC
         ,SUM(DEP_BAL)   AS DEP_BAL
		FROM SESSION.TMP_CUR
		GROUP BY                                                                                                                 
          CST_ID
         ,AC_OU_ID
         ,DEP_TP_ID
         ,PD_GRP_CD
         ,PD_SUB_CD
         ,CCY
			   ,OU_ID
				 ,CST_TP_ID
;

 /** �ռ�������Ϣ */		                                                                                                     
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                    --
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                    
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                      --
	                                                                                                                           
		SET SMY_STEPNUM = 4 ;                                                                                                    --
		SET SMY_STEPDESC = '������ʱ��SESSION.S, �������DEP_AR_SMY ��SMY.CST_INF ���ܺ�Ҫ���µ�����'; 			                     --
                                                                                                                             
                                                                                                                             
/**/                                                                                                                         
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_DEP_MTHLY_SMY                                                               
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K                                                         
     PARTITIONING KEY(CST_ID);                                                                                               --
                                                                                                                             
	--�Ż�by James shang                                                                                                       
	CREATE INDEX SESSION.IDX_S ON SESSION.S(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY,CDR_YR,CDR_MTH);               --
                                                                                                                             
	INSERT INTO SESSION.S                                                                                                      
          (                                                                                                                  
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
            )
	SELECT                                                                                                                     
            COALESCE(CUR.CST_ID,'')                --�ͻ�����                                                                             
           ,CUR.AC_OU_ID              --�˻�����������                                                                       
           ,CUR.DEP_TP_ID             --�������                                                                             
           ,COALESCE(CUR.PD_GRP_CD,'')             --��Ʒ�����                                                                           
           ,COALESCE(CUR.PD_SUB_CD,'')             --��Ʒ�Ӵ���                                                                           
           ,CUR.CCY                   --����                                                                                 
           ,CUR_YEAR                --���YYYY                                                                               
           ,CUR_MONTH               --�·�MM                                                                                 
           ,ACCOUNTING_DATE                --����YYYY-MM-DD                                                                  
           ,C_MON_DAY        --������������                                                                                  
           ,C_QTR_DAY        --������������                                                                                  
           ,C_YR_DAY        --������������                                                                                   
           ,COALESCE(PRE.NOD_IN_MTH  ,0) + CUR.NOD_IN_MTH            --������Ч����                                          
           ,COALESCE(PRE.NOD_IN_QTR  ,0) + CUR.NOD_IN_QTR            --������Ч����                                          
           ,COALESCE(PRE.NOD_IN_YEAR ,0) + CUR.NOD_IN_YEAR           --������Ч����                                          
           ,CUR.OU_ID                 --������                                                                               
           ,CUR.CST_TP_ID             --�ͻ�����                                                                             
           ,CUR.NBR_AC                --�˻�����                                                                             
           ,COALESCE(PRE.DEP_BAL,0)           --�������                                                                     
           ,CUR.DEP_BAL               --������                                                                             
           ,COALESCE(MTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL  --���ۼ����                                                     
           ,COALESCE(QTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL  --���ۼ����                                                     
           ,COALESCE(YTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL  --���ۼ����                                                     
                                                                                                                             
	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON                                                           
         CUR.CST_ID     =PRE.CST_ID                                                                                          
     AND CUR.AC_OU_ID   =PRE.AC_OU_ID                                                                                        
     AND CUR.DEP_TP_ID  =PRE.DEP_TP_ID                                                                                       
     AND CUR.PD_GRP_CD  =PRE.PD_GRP_CD                                                                                       
     AND CUR.PD_SUB_CD  =PRE.PD_SUB_CD                                                                                       
     AND CUR.CCY        =PRE.CCY                                                                                             
      ;                                                                                                                      --
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	                                                                                 --
                                                                                                                             
 /** Insert the log**/                                                                                                       
                                                                                                                             
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	                     --
                                                                                                                             
		SET SMY_STEPNUM = 5 ;                                                                                                    --
		SET SMY_STEPDESC = 'ʹ��Merge���,����SMY ��'; 		--
	                                                                     
/*                                                                                                                            
MERGE INTO SMY.CST_DEP_MTHLY_SMY AS T                                                                                        
 		USING  SESSION.S AS S                                                                                                    
 	  ON                                                                                                                       
         S.CST_ID     =T.CST_ID                                                                                              
     AND S.AC_OU_ID   =T.AC_OU_ID                                                                                            
     AND S.DEP_TP_ID  =T.DEP_TP_ID                                                                                           
     AND S.PD_GRP_CD  =T.PD_GRP_CD                                                                                           
     AND S.PD_SUB_CD  =T.PD_SUB_CD                                                                                           
     AND S.CCY        =T.CCY                                                                                                 
     AND S.CDR_YR     =T.CDR_YR                                                                                              
		 AND S.CDR_MTH    =T.CDR_MTH                                                                                             
WHEN MATCHED THEN UPDATE SET                                                                                                 
                                                                                                                             
        ACG_DT               =S.ACG_DT                --����YYYY-MM-DD                                                       
       ,NOCLD_IN_MTH         =S.NOCLD_IN_MTH          --������������                                                         
       ,NOCLD_IN_QTR         =S.NOCLD_IN_QTR          --������������                                                         
       ,NOCLD_IN_YEAR        =S.NOCLD_IN_YEAR         --������������                                                         
       ,NOD_IN_MTH           =S.NOD_IN_MTH            --������Ч����                                                         
       ,NOD_IN_QTR           =S.NOD_IN_QTR            --������Ч����                                                         
       ,NOD_IN_YEAR          =S.NOD_IN_YEAR           --������Ч����                                                         
       ,OU_ID                =S.OU_ID                 --������                                                               
       ,CST_TP_ID            =S.CST_TP_ID             --�ͻ�����                                                             
       ,NBR_AC               =S.NBR_AC                --�˻�����                                                             
       ,LST_DAY_BAL          =S.LST_DAY_BAL           --�������                                                             
       ,DEP_BAL              =S.DEP_BAL               --������                                                             
       ,MTD_ACML_DEP_BAL_AMT =S.MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                           
       ,QTD_ACML_DEP_BAL_AMT =S.QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                           
       ,YTD_ACML_DEP_BAL_AMT =S.YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                           
                                                                                                                             
WHEN NOT MATCHED THEN INSERT  	                                                                                             
	 (                                                                                                                         
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
        )                                                                                                                    
    VALUES                                                                                                                   
    (                                                                                                                        
            COALESCE(S.CST_ID,'')                --�ͻ�����                                                                               
           ,S.AC_OU_ID              --�˻�����������                                                                         
           ,S.DEP_TP_ID             --�������                                                                               
           ,COALESCE(S.PD_GRP_CD,'')             --��Ʒ�����                                                                             
           ,COALESCE(S.PD_SUB_CD,'')             --��Ʒ�Ӵ���                                                                             
           ,S.CCY                   --����                                                                                   
           ,S.CDR_YR                --���YYYY                                                                               
           ,S.CDR_MTH               --�·�MM                                                                                 
           ,S.ACG_DT                --����YYYY-MM-DD                                                                         
           ,S.NOCLD_IN_MTH          --������������                                                                           
           ,S.NOCLD_IN_QTR          --������������                                                                           
           ,S.NOCLD_IN_YEAR         --������������                                                                           
           ,S.NOD_IN_MTH            --������Ч����                                                                           
           ,S.NOD_IN_QTR            --������Ч����                                                                           
           ,S.NOD_IN_YEAR           --������Ч����                                                                           
           ,S.OU_ID                 --������                                                                                 
           ,S.CST_TP_ID             --�ͻ�����                                                                               
           ,S.NBR_AC                --�˻�����                                                                               
           ,S.LST_DAY_BAL           --�������                                                                               
           ,S.DEP_BAL               --������                                                                               
           ,S.MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
           ,S.QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
           ,S.YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
    )	  	                                                                                                                   
	;  --
*/       
---------------------------Start of Modification on 20091225-------------------------------------------------------------------
IF ACCOUNTING_DATE = MTH_FIRST_DAY AND MAX_ACG_DT <= ACCOUNTING_DATE THEN
  INSERT INTO SMY.CST_DEP_MTHLY_SMY 
  (                                                                                                                         
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
        )                                                                                                                    
    SELECT                                                                                                                                                                                                                                                
            COALESCE(S.CST_ID,'')                --�ͻ�����                                                                               
           ,S.AC_OU_ID              --�˻�����������                                                                         
           ,S.DEP_TP_ID             --�������                                                                               
           ,COALESCE(S.PD_GRP_CD,'')             --��Ʒ�����                                                                             
           ,COALESCE(S.PD_SUB_CD,'')             --��Ʒ�Ӵ���                                                                             
           ,S.CCY                   --����                                                                                   
           ,S.CDR_YR                --���YYYY                                                                               
           ,S.CDR_MTH               --�·�MM                                                                                 
           ,S.ACG_DT                --����YYYY-MM-DD                                                                         
           ,S.NOCLD_IN_MTH          --������������                                                                           
           ,S.NOCLD_IN_QTR          --������������                                                                           
           ,S.NOCLD_IN_YEAR         --������������                                                                           
           ,S.NOD_IN_MTH            --������Ч����                                                                           
           ,S.NOD_IN_QTR            --������Ч����                                                                           
           ,S.NOD_IN_YEAR           --������Ч����                                                                           
           ,S.OU_ID                 --������                                                                                 
           ,S.CST_TP_ID             --�ͻ�����                                                                               
           ,S.NBR_AC                --�˻�����                                                                               
           ,S.LST_DAY_BAL           --�������                                                                               
           ,S.DEP_BAL               --������                                                                               
           ,S.MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
           ,S.QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
           ,S.YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
    FROM SESSION.S as S                                                                                                             
	;
ELSE
	MERGE INTO (select * from SMY.CST_DEP_MTHLY_SMY where ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY) AS T 
	 		USING  SESSION.S AS S                                               
	 	  ON                                                                  
	         S.CST_ID     =T.CST_ID                                         
	     AND S.AC_OU_ID   =T.AC_OU_ID                                       
	     AND S.DEP_TP_ID  =T.DEP_TP_ID                                      
	     AND S.PD_GRP_CD  =T.PD_GRP_CD                                      
	     AND S.PD_SUB_CD  =T.PD_SUB_CD                                      
	     AND S.CCY        =T.CCY                                            
	     AND S.CDR_YR     =T.CDR_YR                                         
			 AND S.CDR_MTH    =T.CDR_MTH                                        
	WHEN MATCHED THEN UPDATE SET                                            
	                                                                        
	        ACG_DT               =S.ACG_DT                --����YYYY-MM-DD  
	       ,NOCLD_IN_MTH         =S.NOCLD_IN_MTH          --������������    
	       ,NOCLD_IN_QTR         =S.NOCLD_IN_QTR          --������������    
	       ,NOCLD_IN_YEAR        =S.NOCLD_IN_YEAR         --������������    
	       ,NOD_IN_MTH           =S.NOD_IN_MTH            --������Ч����    
	       ,NOD_IN_QTR           =S.NOD_IN_QTR            --������Ч����    
	       ,NOD_IN_YEAR          =S.NOD_IN_YEAR           --������Ч����    
	       ,OU_ID                =S.OU_ID                 --������          
	       ,CST_TP_ID            =S.CST_TP_ID             --�ͻ�����        
	       ,NBR_AC               =S.NBR_AC                --�˻�����        
	       ,LST_DAY_BAL          =S.LST_DAY_BAL           --�������        
	       ,DEP_BAL              =S.DEP_BAL               --������        
	       ,MTD_ACML_DEP_BAL_AMT =S.MTD_ACML_DEP_BAL_AMT  --���ۼ����      
	       ,QTD_ACML_DEP_BAL_AMT =S.QTD_ACML_DEP_BAL_AMT  --���ۼ����      
	       ,YTD_ACML_DEP_BAL_AMT =S.YTD_ACML_DEP_BAL_AMT  --���ۼ����      
  ;

  INSERT INTO SMY.CST_DEP_MTHLY_SMY 
  (                                                                                                                         
            CST_ID                --�ͻ�����                                                                                 
           ,AC_OU_ID              --�˻�����������                                                                           
           ,DEP_TP_ID             --�������                                                                                 
           ,PD_GRP_CD             --��Ʒ�����                                                                               
           ,PD_SUB_CD             --��Ʒ�Ӵ���                                                                               
           ,CCY                   --����                                                                                     
           ,CDR_YR                --���YYYY                                                                                 
           ,CDR_MTH               --�·�MM                                                                                   
           ,ACG_DT                --����YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --������������                                                                             
           ,NOCLD_IN_QTR          --������������                                                                             
           ,NOCLD_IN_YEAR         --������������                                                                             
           ,NOD_IN_MTH            --������Ч����                                                                             
           ,NOD_IN_QTR            --������Ч����                                                                             
           ,NOD_IN_YEAR           --������Ч����                                                                             
           ,OU_ID                 --������                                                                                   
           ,CST_TP_ID             --�ͻ�����                                                                                 
           ,NBR_AC                --�˻�����                                                                                 
           ,LST_DAY_BAL           --�������                                                                                 
           ,DEP_BAL               --������                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                               
        )                                                                                                                    
    SELECT                                                                                                                                                                                                                                                
            COALESCE(S.CST_ID,'')                --�ͻ�����                                                                               
           ,S.AC_OU_ID              --�˻�����������                                                                         
           ,S.DEP_TP_ID             --�������                                                                               
           ,COALESCE(S.PD_GRP_CD,'')             --��Ʒ�����                                                                             
           ,COALESCE(S.PD_SUB_CD,'')             --��Ʒ�Ӵ���                                                                             
           ,S.CCY                   --����                                                                                   
           ,S.CDR_YR                --���YYYY                                                                               
           ,S.CDR_MTH               --�·�MM                                                                                 
           ,S.ACG_DT                --����YYYY-MM-DD                                                                         
           ,S.NOCLD_IN_MTH          --������������                                                                           
           ,S.NOCLD_IN_QTR          --������������                                                                           
           ,S.NOCLD_IN_YEAR         --������������                                                                           
           ,S.NOD_IN_MTH            --������Ч����                                                                           
           ,S.NOD_IN_QTR            --������Ч����                                                                           
           ,S.NOD_IN_YEAR           --������Ч����                                                                           
           ,S.OU_ID                 --������                                                                                 
           ,S.CST_TP_ID             --�ͻ�����                                                                               
           ,S.NBR_AC                --�˻�����                                                                               
           ,S.LST_DAY_BAL           --�������                                                                               
           ,S.DEP_BAL               --������                                                                               
           ,S.MTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
           ,S.QTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
           ,S.YTD_ACML_DEP_BAL_AMT  --���ۼ����                                                                             
    FROM SESSION.S as S
    WHERE NOT EXISTS (
         SELECT 1 FROM SMY.CST_DEP_MTHLY_SMY T
         WHERE S.CST_ID     =T.CST_ID                                         
					     AND S.AC_OU_ID   =T.AC_OU_ID                                       
					     AND S.DEP_TP_ID  =T.DEP_TP_ID                                      
					     AND S.PD_GRP_CD  =T.PD_GRP_CD                                      
					     AND S.PD_SUB_CD  =T.PD_SUB_CD                                      
					     AND S.CCY        =T.CCY                                            
					     AND S.CDR_YR     =T.CDR_YR                                         
							 AND S.CDR_MTH    =T.CDR_MTH 
							 AND T.ACG_DT>=MTH_FIRST_DAY AND T.ACG_DT<=MTH_LAST_DAY
    )                                                                                                                  
	;
END IF;
---------------------------End of Modification on 20091225-------------------------------------------------------------------
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                  --
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                      --
                                                                                                                             
		SET SMY_STEPNUM = 6 ;                                                                                                    --
		SET SMY_STEPDESC = '�洢���̽�����'; 		                                                                                 --
                                                                                                                             
	 	SET SMY_RCOUNT = 0;                                                                                                      --
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                      --
	 	                                                                                                                         
	 COMMIT;                                                                                                                   --
END@