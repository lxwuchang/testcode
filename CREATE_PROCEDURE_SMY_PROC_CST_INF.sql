CREATE PROCEDURE SMY.PROC_CST_INF(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_INF.sql
-- Procedure name: 			SMY.PROC_CST_INF
-- Source Table:				SOR.CST,SOR.ORG,SOR.IP_ALT_ID,SOR.IP,SOR.CST_CR_RSK_RTG,SOR.CST_X_PST_ADR,SOR.PST_ADR,SOR.CST_X_TEL_ADR
-- Target Table: 				SMY.CST_INF
-- Project:             ZJ RCCB EDW
-- update method:       refresh each day
-- Purpose:
--
--=============================================================================
-- Creation Date:       2009.11.05
-- Origin Author:       Peng Jie
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-10-28   Peng Jie     Create SP File
-- 2009-12-04   James shang  format and optimize
-- 2009-12-28   Xu Yan       Updated the ORG_FNC_INF conditional statement
-- 2010-01-26   Xu Yan       Added two columns 'LTST_PST_ADR', 'LTST_ADR_PSTCD_Area_Id'
-- 2010-04-21   Peng Yi tao  Update 'LTST_PST_ADR' conditional statement
-- 2010-06-24   PE           Added three columns 'ACG_DT', 'SPEC_ID_NO','SPEC_ID_TP_ID' ,'EXP_DT' And three columns 'ACG_DT', 'SPEC_ID_NO','SPEC_ID_TP_ID','EXP_DT' conditional statement
-- 2010-08-24   Wang Youbing Tuning the procedure.Delete function PEMAX.
-- 2010-11-16   Zhang Na     Added two column 'LTST_CST_PST_ADR', 'LTST_CST_ADR_PSTCD_AREA_ID'
-- 2011-01-10   Xu Yan       Updated 'TEL_NBR' per PiYi's request
-- 2011-08-09   Chen XiaoWen 修复"客户基本信息最近更新时间"的bug
-- 2011-08-18   Li SHuWen    修改现有客户个人身份证件号码/企业代码证的取法
-- 2012-04-18   Chen XiaoWen 添加联系人、客户简称、客户英文名、客户状态字段
-- 2012-05-29   Chen XiaoWen 添加联系电话、手机号码字段
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
LANGUAGE SQL
Begin
/*声明异常处理使用变量*/
   DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
   DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
   DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
   DECLARE SMY_DATE DATE;                                 --临时日期变量
   DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
   DECLARE SMY_PROCNM VARCHAR(100);                        --存储过程名称
   DECLARE EMP_SQL VARCHAR(200);--

 DECLARE EXIT HANDLER FOR SQLEXCEPTION
 BEGIN
 	SET SMY_SQLCODE = SQLCODE;--
   ROLLBACK;--
   INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
   COMMIT;--
   RESIGNAL;--
 END;--
 DECLARE CONTINUE HANDLER FOR SQLWARNING
 BEGIN
   SET SMY_SQLCODE = SQLCODE;--
   INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
   COMMIT;--
   SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
 END;--

--变量赋值
  SET SMY_PROCNM = 'PROC_CST_INF';--
  SET SMY_DATE=ACCOUNTING_DATE;--

  DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
  COMMIT;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '存储过程开始运行.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
  COMMIT;--

--清空表数据

	SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
	SET SMY_STEPDESC = '清空SMY.CST_INF表' ;--

	SET EMP_SQL= 'Alter TABLE SMY.CST_INF ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
	EXECUTE IMMEDIATE EMP_SQL;--
  COMMIT;--
 /*插入日志信息*/
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
  COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '汇总SOR.CST表数据插入SMY表.' ;--

	INSERT INTO SMY.CST_INF
    (
       CST_ID                    --客户内码
      ,CST_NO                    --客户号
      ,CST_NM                    --客户名称
      ,ID_NO                     --个人身份证件号码/企业代码证
      ,CST_CTY_Id                --客户所在国家，冗余信息
      ,ENT_IDV_IND               --客户类型
      ,CST_CR_RSK_RTG_ID         --客户资信等级
      ,RSDNT_PST_ADR             --客户住宅地址
      ,OFC_PST_ADR_NM            --客户办公地址
      ,RSDNT_ADR_PSTCD_Area_Id   --住宅地址邮编
      ,OFC_ADR_PSTCD_Area_Id     --办公地址邮编
      ,TEL_NBR                   --固定电话号码
      ,MBL_NBR                   --手机号
      ,CST_RLN_VAL_TP_Id         --客户VIP级别
      ,CNRL_BNK_IDY_CL_ID        --行业类别
      ,Farmer_TP_Id              --农户类别
      ,RPRG_OU_IP_ID             --归属机构
      ,EFF_CST_DT                --客户开户日期
      ,ORG_Scale_TP_Id           --企业规模类型)
      ,AREA_LVL1_TP_ID
      ,AREA_LVL2_TP_ID
      ,AREA_LVL3_TP_ID
      ,ORG_RGST_TP_Id
      ,IDV_EMPR_RGST_TP_Id
      ,LTST_PST_ADR              --最新地址
      ,LTST_ADR_PSTCD_Area_Id    --最新地址邮编
-----------------------------------------------------------Start on 20101116---------------------------------------------------      
      ,LTST_CST_PST_ADR           --最新客戶地址
      ,LTST_CST_ADR_PSTCD_AREA_ID --最新客戶地址邮编
-----------------------------------------------------------End on 20101116---------------------------------------------------      
      
      ,SPEC_ID_NO                --证件号码
      ,SPEC_ID_TP_ID             --证件类型
      ,ACG_DT                    --客户基本信息最新更新时间
      ,EXP_DT                    --实效日期
      ,CTC_PSN                   --联系人
      ,CST_SHRT_NM               --客户简称
      ,CST_EN_NM                 --客户英文名称
      ,CST_SPCL_ST_TP_ID         --客户状态
      ,CTC_TEL_NO                --联系电话
      ,MBL_TEL_NO                --手机号码
      )
WITH TMP_IP_ALT_ID AS
    	(
    		select IP_ID, ALT_ID, IP_ALT_ID_TP_ID from (
						select IP_ID, ALT_ID, IP_ALT_ID_TP_ID, row_number() over(partition by IP_ID order by length(rtrim(ALT_ID)) desc, ALT_ID desc) as row_nm
						FROM SOR.IP_ALT_ID where IP_ID like '81%' and IP_ALT_ID_TP_ID=10150001
				) a where row_nm=1
				union
				select IP_ID, ALT_ID, IP_ALT_ID_TP_ID from (
						select IP_ID, ALT_ID, IP_ALT_ID_TP_ID, row_number() over(partition by IP_ID order by ALT_ID desc) as row_nm
						FROM SOR.IP_ALT_ID where IP_ID not like '81%' and IP_ALT_ID_TP_ID=10150015
				) a where row_nm=1
     	),   --按长度最长，最大数为准
     TMP_CST_CR_RSK_RTG AS    -- 取RT_YR 最大的记录
     (
    		SELECT CST_ID, CST_CR_RSK_RTG_ID ,LAST_ETL_ACG_DT,CTC_PSN,CTC_TEL_NO,MBL_TEL_NO
        	FROM
        		(
        			SELECT CST_ID, CST_CR_RSK_EVAL_AR_ID,RT_YR ,CST_CR_RSK_RTG_ID ,LAST_ETL_ACG_DT,row_number()over(partition by CST_ID order by RT_YR desc) as row_nm
        			,CTC_PSN --联系人
        			,CTC_TEL_NO --联系电话
        			,MBL_TEL_NO --手机号码
              FROM sor.CST_CR_RSK_RTG
             ) as a
         WHERE a.row_nm = 1
      ),
-----------------------------------------------------------Start on 20101116---------------------------------------------------            
      TMP_CST_X_PST_ADR_ALL AS
      (
        SELECT a.IP_ID,a.PST_ADR_ID,b.PSTCD_AREA_ID,a.RANK, ORIG_PST_ADR_STR,a.LAST_ETL_ACG_DT
      	FROM (
               SELECT IP_ID, PST_ADR_ID , RANK ,LAST_ETL_ACG_DT
               FROM SOR.CST_X_PST_ADR
               WHERE  DEL_F = 0
              ) AS a LEFT OUTER JOIN SOR.PST_ADR b on a.PST_ADR_ID = b.PST_ADR_ID
        ),
-----------------------------------------------------------End on 20101116---------------------------------------------------              
     TMP_CST_X_PST_ADR_OFFIC AS
      (
      	SELECT a.IP_ID,a.PST_ADR_ID,b.PSTCD_AREA_ID, ORIG_PST_ADR_STR,a.LAST_ETL_ACG_DT --LAST_ETL_ACG_DT Added by PE
      	FROM (
               SELECT IP_ID, PST_ADR_ID , RANK ,LAST_ETL_ACG_DT,row_number()over(partition by IP_ID order by RANK desc) as row_nm
               FROM SOR.CST_X_PST_ADR
               WHERE IP_X_PST_ADR_TP_ID =11060002 AND DEL_F = 0
              ) AS a LEFT OUTER JOIN SOR.PST_ADR b on a.PST_ADR_ID = b.PST_ADR_ID
        WHERE row_nm =1
        ) ,
     TMP_CST_X_PST_ADR_HOME AS
      (
      	SELECT a.IP_ID,a.PST_ADR_ID,b.PSTCD_AREA_ID, ORIG_PST_ADR_STR,a.LAST_ETL_ACG_DT  --LAST_ETL_ACG_DT Added by PE
      	FROM
      		(
       			SELECT IP_ID, PST_ADR_ID , RANK ,LAST_ETL_ACG_DT,row_number()over(partition by IP_ID order by RANK desc) as row_nm
            FROM SOR.CST_X_PST_ADR
            WHERE IP_X_PST_ADR_TP_ID =11060007 AND DEL_F = 0
           ) AS a LEFT JOIN SOR.PST_ADR b on a.PST_ADR_ID = b.PST_ADR_ID
        WHERE row_nm =1
        ) ,
     TMP_CST_X_TEL_ADR_FIX as
      (
      	SELECT a.IP_ID,a.TEL_ADR_ID,b.FULL_TEL_NO,a.LAST_ETL_ACG_DT --LAST_ETL_ACG_DT Added by PE
      	FROM
      		(
            SELECT IP_ID,TEL_ADR_ID , RANK ,LAST_ETL_ACG_DT,row_number()over(partition by IP_ID order by RANK desc) as row_nm
            FROM SOR.CST_X_TEL_ADR
            -----------------------Start on 2010-01-10----------------------------------
            --WHERE IP_X_TEL_ADR_TP_ID IN (11070003,11070004 )
            WHERE IP_X_TEL_ADR_TP_ID IN (11070002,11070003,11070004,11070007 )
                  AND DEL_F = 0
                  --Added new statement on 2010-01-10
                  ORDER BY IP_X_TEL_ADR_TP_ID
            -----------------------End on 2010-01-10----------------------------------
           ) AS a LEFT OUTER JOIN SOR.TEL_ADR b on a.TEL_ADR_ID = b.TEL_ADR_ID
        WHERE row_nm = 1
      ),
     TMP_CST_X_TEL_ADR_MOBILE as
      (
        SELECT a.IP_ID, a.TEL_ADR_ID, FULL_TEL_NO,a.LAST_ETL_ACG_DT --LAST_ETL_ACG_DT Added by PE
        FROM
         (
          SELECT IP_ID,TEL_ADR_ID , RANK ,LAST_ETL_ACG_DT,row_number()over(partition by IP_ID order by RANK desc) as row_nm
          FROM SOR.CST_X_TEL_ADR
          WHERE IP_X_TEL_ADR_TP_ID = 11070007 AND DEL_F = 0
          ) AS a LEFT OUTER JOIN SOR.TEL_ADR b on a.TEL_ADR_ID = b.TEL_ADR_ID
        WHERE row_nm = 1)
       ----------------------------Start on 20100126---------------------------------------------------
      ,TMP_CST_X_PST_ADR as (
        SELECT a.IP_ID,a.PST_ADR_ID,b.PSTCD_AREA_ID, ORIG_PST_ADR_STR,a.LAST_ETL_ACG_DT --LAST_ETL_ACG_DT Added by PE
      	FROM (
               SELECT IP_ID, PST_ADR_ID , RANK ,LAST_ETL_ACG_DT,row_number()over(partition by IP_ID order by RANK desc) as row_nm
               FROM SOR.CST_X_PST_ADR
       ----------------------------Start on 20100421---------------------------------------------------
               -- WHERE DEL_F = 0
              WHERE IP_X_PST_ADR_TP_ID =11060001 AND DEL_F = 0
       ----------------------------End on 20100421---------------------------------------------------
              ) AS a LEFT OUTER JOIN SOR.PST_ADR b on a.PST_ADR_ID = b.PST_ADR_ID
        WHERE row_nm =1
      ),
      ----------------------------End on 20100126---------------------------------------------------
      ------------------------------------MODIFIED BY PE ON 2010-06-24-----------------------------------------------------------------------
      TMP_IP_ALT_ID_SPEC as
      (
       SELECT a.IP_ID,a.ALT_ID,a.IP_ALT_ID_TP_ID,a.EXP_DT,a.LAST_ETL_ACG_DT --LAST_ETL_ACG_DT Added by PE
       FROM
       (
        SELECT IP_ID,ALT_ID,IP_ALT_ID_TP_ID,EXP_DT,LAST_ETL_ACG_DT,row_number()over(partition by IP_ID order by IP_ALT_ID_TP_ID asc,ALT_ID desc) as row_nm  ----MODIFIED BY Wang Youbing ON 2010-08-24 Modify the order by columns
              FROM sor.IP_ALT_ID WHERE DEL_F = 0 AND IP_ALT_ID_TP_ID>0
             ) as a
         WHERE a.row_nm = 1
       )
-------------------------------------END BY PE ON 2010-06-24------------------------------------------------------------------------------
      SELECT
	     a.CST_ID
	    ,a.CST_NO
	    ,a.CST_FUL_NM
	    ,c.ALT_ID
	    ,a.CST_CTY_Id
	    ,d.IP_TP_ID
	    ,coalesce(e.CST_CR_RSK_RTG_ID, -1)
	    ,g.ORIG_PST_ADR_STR
	    ,f.ORIG_PST_ADR_STR
	    ,g.PSTCD_AREA_ID
	    ,f.PSTCD_AREA_ID
	    ,h.FULL_TEL_NO
	    ,i.FULL_TEL_NO
	    ,a.CST_RLN_VAL_TP_Id
	    ,b.CNRL_BNK_IDY_CL_ID
	    ,j.LGL_FARMER_TP_ID
	    ,a.PRIM_BR_ID
	    ,a.EFF_CST_DT
	    ,b.ORG_Scale_TP_Id
	    ,a.AREA_LVL1_TP_ID
	    ,a.AREA_LVL2_TP_ID
	    ,a.AREA_LVL3_TP_ID
	    ,k.RGST_ENT_TP_ID
	    ,m.EMPR_RGST_TP_ID
      ,n.ORIG_PST_ADR_STR              --最新地址
      ,n.PSTCD_AREA_ID                 --最新地址邮编
      ,s.ORIG_PST_ADR_STR              --最新客戶地址 
      ,s.PSTCD_AREA_ID                 --最新客戶地址邮编
      ,COALESCE(c.ALT_ID, p.ALT_ID)		 
	    ,COALESCE(c.IP_ALT_ID_TP_ID, p.IP_ALT_ID_TP_ID)
--------------------------------2011-08-09 by Chen XiaoWen start---------------------------------
	    ,(case when b.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or d.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or e.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or f.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or g.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or h.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or i.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or j.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or k.EFF_DT = ACCOUNTING_DATE
	        or m.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or n.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or p.LAST_ETL_ACG_DT = ACCOUNTING_DATE
	        or s.LAST_ETL_ACG_DT = ACCOUNTING_DATE
        then ACCOUNTING_DATE    
        else a.LAST_ETL_ACG_DT
        end)
--------------------------------2011-08-09 by Chen XiaoWen end---------------------------------
	    ,p.EXP_DT
	    ,e.CTC_PSN             --联系人
	    ,a.CST_SHRT_NM         --客户简称
	    ,a.CST_EN_NM           --客户英文名称
	    ,a.CST_SPCL_ST_TP_ID   --客户状态
	    ,e.CTC_TEL_NO          --联系电话
	    ,e.MBL_TEL_NO          --手机号码
	     FROM SOR.CST a left join SOR.ORG b                on a.CST_ID = b.ORG_ID
               left join TMP_IP_ALT_ID c            on a.CST_ID = c.IP_ID
               left join SOR.IP d                   on a.CST_ID = d.IP_ID
               left join TMP_CST_CR_RSK_RTG e       on a.CST_ID = e.CST_ID
               left join TMP_CST_X_PST_ADR_OFFIC f  on a.CST_ID = f.IP_ID
               left join TMP_CST_X_PST_ADR_HOME g   on a.CST_ID = g.IP_ID
               left join TMP_CST_X_TEL_ADR_FIX h    on a.CST_ID = h.IP_ID
               left join TMP_CST_X_TEL_ADR_MOBILE i on a.CST_ID = i.IP_ID
               left join SOR.IDV j                  on a.CST_ID = j.IDV_ip_ID
               ---------------------------------------MODEFIED BY PE ON 2010-06-24------------------------------
               left join TMP_IP_ALT_ID_SPEC p       on a.CST_ID =p.IP_ID
               ----------------------------------------END BY PE ON 2010-06-24----------------------------------
               ---------------------------------------Start on 20091228------------------------------------------
               --left join SOR.ORG_FNC_INF k          on a.CST_ID = k.ORG_IP_ID
               left join SOR.ORG_FNC_INF k          on a.CST_ID = k.ORG_IP_ID and k.END_DT='9999-12-31'
               ---------------------------------------End on 20091228------------------------------------------
               left join SOR.IDV_CST_EMPR_INFO m    on a.CST_ID =m.CST_ID and rank = 1
               left join TMP_CST_X_PST_ADR n          on a.CST_ID = N.IP_ID
-----------------------------------------------------------Start on 20101116---------------------------------------------------            
               left join TMP_CST_X_PST_ADR_ALL s     on a.CST_ID=s.IP_ID and a.PST_ADR_RANK=s.RANK

-----------------------------------------------------------End on 20101116---------------------------------------------------            
---------------------------               
;--

	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

	INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	COMMIT;--

   SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --4
   SET SMY_STEPDESC = '存储过程结束！' ;--
   SET SMY_RCOUNT =0 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
 	VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	COMMIT;--
End@