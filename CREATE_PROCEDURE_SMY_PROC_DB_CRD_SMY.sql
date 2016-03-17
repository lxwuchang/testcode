CREATE PROCEDURE SMY.PROC_DB_CRD_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_DB_CRD_SMY.sql
-- Procedure name:                      SMY.PROC_DB_CRD_SMY
-- Source Table:                                SOR.DB_CRD,SOR.CRD,SOR.DMD_DEP_SUB_AR
-- Target Table:                                SMY.DB_CRD_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :        借记卡汇总          
-- PROCESS METHOD      :  empty and INSERT
--=============================================================================
-- Creation Date:       2009.11.13
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-13   JAMES SHANG     Create SP File  
-- 2009-12-01   Xu Yan          Added a new column CRD_BRAND_TP_ID      
-- 2010-01-05   Xu Yan          Updated the value of the 'OU_ID' from 'DEAL_OU_IP_ID' to 'RPRG_OU_IP_ID'
-- 2011-05-03   Chen XiaoWen    引入中间临时表T_SMY_CRD，解决关联时分区键不一致的情况
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN

/*声明异常处理使用变量*/
                DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
                DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
                DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
                DECLARE SMY_DATE DATE;        --临时日期变量
                DECLARE SMY_RCOUNT INT;       --DML语句作用记录数
                DECLARE SMY_PROCNM VARCHAR(100);                        --存储过程名称

/*声明存储过程使用变量*/
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
                DECLARE MON_DAY SMALLINT;--
                DECLARE LAST_MONTH SMALLINT;--
                DECLARE EMP_SQL VARCHAR(200);  --

/*
        1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.
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

   /*变量赋值*/
    SET SMY_PROCNM  ='PROC_DB_CRD_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,iso),1,7)||'-01'); --取当月初日
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      --
    --计算月日历天数
    SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
    
    --计算季度日历天数
    IF CUR_QTR = 1  
       THEN 
        SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
        SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;--
    ELSEIF CUR_QTR = 2
       THEN 
        SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
        SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;        --
    ELSEIF CUR_QTR = 3
       THEN 
        SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
        SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;        --
    ELSE
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');       --
    END IF;--

  /*取当季日历天数*/ 
        SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
                

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
                DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
                        COMMIT;--
                
                GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
                
                SET SMY_STEPDESC =      '存储过程开始运行' ;--
                INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
                                VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--


           SET EMP_SQL= 'Alter TABLE SMY.DB_CRD_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
                
                  EXECUTE IMMEDIATE EMP_SQL;       --
      
      COMMIT;--


        /*声明用户临时表*/
        
 
                SET SMY_STEPNUM = 2 ;--
                SET SMY_STEPDESC = '声明临时表, 存放从SOR.DMD_DEP_SUB_AR 汇总后的数据';                  --
 
        DECLARE GLOBAL TEMPORARY TABLE T_DMD_DEP_SUB_AR
   AS 
   (
                SELECT
                                 DMD_DEP_AR_ID   AS DMD_DEP_AR_ID
          ,DNMN_CCY_ID     AS DNMN_CCY_ID  --
          ,RPRG_OU_IP_ID           AS RPRG_OU_IP_ID         --     
          ,SUM(BAL_AMT)                   AS BAL_AMT              --账户余额                             
                FROM SOR.DMD_DEP_SUB_AR
                     GROUP BY  DMD_DEP_AR_ID,DNMN_CCY_ID ,RPRG_OU_IP_ID                      
      )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     IN TS_USR_TMP32K 
     PARTITIONING KEY(DMD_DEP_AR_ID) ;          --
     
 INSERT INTO SESSION.T_DMD_DEP_SUB_AR 
       SELECT  
                                 DMD_DEP_AR_ID                AS DMD_DEP_AR_ID
          ,DNMN_CCY_ID     AS DNMN_CCY_ID  --
          ,RPRG_OU_IP_ID           AS RPRG_OU_IP_ID         --     
          ,SUM(BAL_AMT)                   AS BAL_AMT              --账户余额                             
                FROM SOR.DMD_DEP_SUB_AR
                     GROUP BY  DMD_DEP_AR_ID,DNMN_CCY_ID ,RPRG_OU_IP_ID                 
                ;     --
 /** 收集操作信息 */                                         
        GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
        INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
         VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
         
                SET SMY_STEPNUM = 3 ;--
                SET SMY_STEPDESC = '往汇总表DB_CRD_SMY,插入当日的数据';          --

------------------------------------Start on 2011-05-03 -------------------------------------------------
        DECLARE GLOBAL TEMPORARY TABLE T_SMY_CRD
   AS 
   (
                SELECT
           DB_CRD.DB_CRD_NO as CRD_NO
          ,CRD.RPRG_OU_IP_ID as OU_ID
          ,CRD.CRD_TP_ID as DB_CRD_TP_ID
          ,CRD.CRD_LCS_TP_ID as CRD_LCS_TP_ID
          ,DB_CRD.PSBK_RLTD_F as PSBK_RLTD_F
          ,(CASE WHEN LEFT(DB_CRD.DB_CRD_NO,6)='940036' THEN 1 ELSE 0 END) as IS_NONGXIN_CRD_F
          ,CRD.ENT_IDV_CST_IND as ENT_IDV_IND
          ,DB_CRD.PRIM_CCY_ID as PRIM_CCY_ID
          ,CRD.AC_AR_ID as AC_AR_Id
          ,CRD.PD_GRP_CD as PD_GRP_CODE
          ,CRD.PD_SUB_CD as PD_SUB_CODE
          ,CRD.EFF_DT as EFF_DT
          ,CRD.END_DT as END_DT
          ,CRD.PRIM_CST_ID as CST_ID
          ,CRD.LAST_CST_AVY_DT as LST_CST_AVY_DT
          ,CRD.NGO_CRD_IND as NGO_CRD_IND
          ,CRD.BIZ_TP_ID as BIZ_CGY_TP_ID
          ,DB_CRD.EXP_DT as EXP_MTH_YEAR
          ,CRD.CRD_BRND_TP_ID as CRD_BRAND_TP_ID
          ,DB_CRD.CRD_CHG_DT as CRD_CHG_DT
          ,DB_CRD_AC_AR_ID as DB_CRD_AC_AR_ID
        FROM      SOR.DB_CRD AS DB_CRD INNER  JOIN       SOR.CRD              AS  CRD           
                                                                        ON DB_CRD.DB_CRD_NO=CRD.CRD_NO               
      )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     IN TS_USR_TMP32K 
     PARTITIONING KEY(DB_CRD_AC_AR_ID) ;
  
  INSERT INTO SESSION.T_SMY_CRD
  SELECT
                                DB_CRD.DB_CRD_NO
          ,CRD.RPRG_OU_IP_ID
          ,CRD.CRD_TP_ID
          ,CRD.CRD_LCS_TP_ID
          ,DB_CRD.PSBK_RLTD_F
          ,(CASE WHEN LEFT(DB_CRD.DB_CRD_NO,6)='940036' THEN 1 ELSE 0 END)
          ,CRD.ENT_IDV_CST_IND
          ,DB_CRD.PRIM_CCY_ID
          ,CRD.AC_AR_ID
          ,CRD.PD_GRP_CD
          ,CRD.PD_SUB_CD
          ,CRD.EFF_DT
          ,CRD.END_DT
          ,CRD.PRIM_CST_ID
          ,CRD.LAST_CST_AVY_DT
          ,CRD.NGO_CRD_IND
          ,CRD.BIZ_TP_ID
          ,DB_CRD.EXP_DT     
          ,CRD.CRD_BRND_TP_ID
          ,DB_CRD.CRD_CHG_DT
          ,DB_CRD_AC_AR_ID
          FROM      SOR.DB_CRD AS DB_CRD INNER  JOIN       SOR.CRD              AS  CRD           
                                                                        ON DB_CRD.DB_CRD_NO=CRD.CRD_NO
  ;
------------------------------------End on 2011-05-03 -------------------------------------------------
 
  INSERT INTO SMY.DB_CRD_SMY
  
  (          
        CRD_NO           --卡号             
       ,OU_ID            --受理机构号 注：此处更改为归属结构号 updated on 2010-01-05
       ,DB_CRD_TP_ID     --卡类型           
       ,CRD_LCS_TP_ID    --卡状态           
       ,PSBK_RLTD_F      --卡折相关标识     
       ,IS_NONGXIN_CRD_F --丰收卡/农信卡标识
       ,ENT_IDV_IND      --卡对象           
       ,CCY              --币种             
       ,AC_AR_Id         --账号             
       ,AC_BAL_AMT       --账户余额
       ,PD_GRP_CODE      --产品组代码                  
       ,PD_SUB_CODE      --产品子代码            
       ,EFF_DT           --开卡日期         
       ,END_DT           --销卡日期         
       ,CST_ID           --客户内码         
       ,LST_CST_AVY_DT   --客户最后活动日期 
       ,AC_OU_ID         --账户归属机构号   
       ,NGO_CRD_IND      --协议卡类型       
       ,BIZ_CGY_TP_ID    --业务类别         
       ,EXP_MTH_YEAR     --到期年月 
       ,CRD_BRAND_TP_ID  --卡品牌  
       ,CRD_CHG_DT       --换卡日期                                  
  
  )
        SELECT
------------------------------------Start on 2011-05-03 -------------------------------------------------
           T_SMY_CRD.CRD_NO
          --------------Start on 2010-01-05----------------------
          --,DB_CRD.DEAL_OU_IP_ID
          ,T_SMY_CRD.OU_ID
          --------------End on 2010-01-05----------------------
          ,T_SMY_CRD.DB_CRD_TP_ID
          ,T_SMY_CRD.CRD_LCS_TP_ID
          ,T_SMY_CRD.PSBK_RLTD_F
          ,(CASE WHEN LEFT(T_SMY_CRD.CRD_NO,6)='940036' THEN 1 ELSE 0 END)
          ,T_SMY_CRD.ENT_IDV_IND
          ,T_SMY_CRD.PRIM_CCY_ID
          ,T_SMY_CRD.AC_AR_ID
          ,COALESCE(T_DMD_DEP_SUB_AR.BAL_AMT,0)
          ,T_SMY_CRD.PD_GRP_CODE
          ,T_SMY_CRD.PD_SUB_CODE
          ,T_SMY_CRD.EFF_DT
          ,T_SMY_CRD.END_DT
          ,T_SMY_CRD.CST_ID
          ,T_SMY_CRD.LST_CST_AVY_DT
          ,COALESCE(T_DMD_DEP_SUB_AR.RPRG_OU_IP_ID,'')
          ,T_SMY_CRD.NGO_CRD_IND
          ,T_SMY_CRD.BIZ_CGY_TP_ID
          ,T_SMY_CRD.EXP_MTH_YEAR
          ,T_SMY_CRD.CRD_BRAND_TP_ID  --卡品牌
          ,T_SMY_CRD.CRD_CHG_DT          --换卡日期
--      FROM      SOR.DB_CRD AS DB_CRD INNER  JOIN       SOR.CRD              AS  CRD           
--                                                                      ON DB_CRD.DB_CRD_NO=CRD.CRD_NO
        FROM SESSION.T_SMY_CRD as T_SMY_CRD
        LEFT OUTER JOIN  SESSION.T_DMD_DEP_SUB_AR   AS  T_DMD_DEP_SUB_AR    
                                                                        ON T_SMY_CRD.DB_CRD_AC_AR_ID =T_DMD_DEP_SUB_AR.DMD_DEP_AR_ID AND T_SMY_CRD.PRIM_CCY_ID=T_DMD_DEP_SUB_AR.DNMN_CCY_ID
   ;
------------------------------------End on 2011-05-03 -------------------------------------------------

 /** 收集操作信息 */                                         
        GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
        INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
         VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
         
         COMMIT;--
END@