CREATE PROCEDURE SMY.PROC_CRD_PRFL_CRD_DLY_SMY(ACCOUNTING_DATE DATE)

-------------------------------------------------------------------------------

-- (C) Copyright ZJRCU and IBM <date>

--

-- File name:           SMY.PROC_CRD_PRFL_CRD_DLY_SMY.sql

-- Procedure name: 			SMY.PROC_CRD_PRFL_CRD_DLY_SMY

-- Source Table:				SOR.CR_CRD,SOR.CRD,SOR.CC_AC_AR,SOR.DB_CRD

-- Target Table: 				SMY.CRD_PRFL_CRD_DLY_SMY

-- Project:             ZJ RCCB EDW

-- Note                 Delete and Insert and Update

-- Purpose:             

--

--=============================================================================

-- Creation Date:       CUR_YEAR.11.09

-- Origin Author:       Peng Jie

--

-- Version: %1.0%

--

-- Modification History

-- --------------------

--

-- Date         ByPerson        Description

-- ----------   --------------  -----------------------------------------------

-- 2009-11-09   Peng Jie     Create SP File		

-- 2009-11-25   Xu Yan       Restructed the script

-- 2009-12-03   Xu Yan       Added a new column 'NBR_CST'

-- 2009-12-08   Xu Yan       Added 3 new columns 'NBR_DRMT_AC' ,'NBR_DRMT_CRD_WITH_LOW_BAL', 'NBR_DRMT_AC_WITH_LOW_BAL'

-- 2009-12-14   Xu Yan       Updated the 'NBR_DRMT_CRD', 'ACT_NBR_AC', 'NBR_DRMT_AC_WITH_LOW_BAL', 'NBR_DRMT_AC', 'NBR_NEW_CST', 'NBR_AC_Opened', 'NBR_AC_CLS'

-- 2009-12-15   Xu Yan       Updated the 'NBR_DRMT_CRD' for the credit cards due to the business rule change

-- 2009-12-22   Xu Yan       Updated the DB_CRD part to take IS_NONGXIN_CRD_F directly 

-- 2009-12-23   Xu Yan       Due to the business request, change the 'NBR_NEW_CRD' definition to describe the number of new delivered cards.

-- 2010-01-14   Xu Yan       Fixed a previous bug for acumulated NBR_AC_CLS and accumulated NBR_CRD_CLECTD

-- 2010-01-14   Xu Yan       Updated the rules for dormant cards

-- 2010-05-21   Xu Yan       Updated the NBR_DRMT_AC for Credit Cards, according to 柴善良's request.

--                           取账户下所有卡片都为睡眠卡，则账户为睡眠户。

--                           Updated the NBR_CST for Debit Cards, according to 柴善良's request.

--                           对客户内码去重
-- 2012-02-27   Chen XiaoWen 增加临时表TMP，缓存TMP_CRD_PRFL_CRD_DLY_SMY_CR的中间结果，最后再根据索引进行group by操作。

-------------------------------------------------------------------------------

LANGUAGE SQL

BEGIN

	

/*声明异常处理使用变量*/

DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE

DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记

DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述

DECLARE SMY_DATE DATE;                                 --临时日期变量

DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数

DECLARE SMY_PROCNM VARCHAR(100);    --

DECLARE CUR_YEAR SMALLINT;--

DECLARE CUR_MONTH SMALLINT;--

DECLARE CUR_DAY INTEGER;--

DECLARE MAX_ACG_DT DATE;--

Declare CUR_MTH_YEAR CHAR(6) ; --年月



DECLARE EXIT HANDLER FOR SQLEXCEPTION

BEGIN

	SET SMY_SQLCODE = SQLCODE;--

  ROLLBACK;--

  set SMY_STEPNUM = SMY_STEPNUM + 1;--

  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--

  COMMIT;--

  RESIGNAL;--

END;--







/*变量赋值*/

SET SMY_PROCNM = 'PROC_CRD_PRFL_CRD_DLY_SMY';--

SET SMY_DATE=ACCOUNTING_DATE;--

SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份

SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份

if CUR_MONTH<10 

	THEN 

		SET CUR_MTH_YEAR = CHAR(CUR_YEAR)||'0'||CHAR(CUR_MONTH) ;--

	ELSE 

		SET CUR_MTH_YEAR = CHAR(CUR_YEAR)||CHAR(CUR_MONTH);--

end if;		--



SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日

SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT;	--



/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/

DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--

COMMIT;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '存储过程开始运行.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

COMMIT;--



DELETE FROM SMY.CRD_PRFL_CRD_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE;--





SET SMY_STEPDESC = '创建临时表,并把当日数据插入';--



DECLARE GLOBAL TEMPORARY TABLE TMP_CRD_PRFL_CRD_DLY_SMY_CR(

				 OU_ID                                CHARACTER(18)--机构号

				,CRD_TP_ID                            INTEGER      --卡类型

				,Is_CR_CRD_F                          SMALLINT     --是否为贷记卡

				,CRD_Brand_TP_Id                      INTEGER      --卡品牌类型

				,CRD_PRVL_TP_ID                       INTEGER      --卡级别

				,PSBK_RLTD_F                          SMALLINT     --卡折相关标识

				,IS_NONGXIN_CRD_F                     SMALLINT     --丰收卡/农信卡标识

				,ENT_IDV_IND                          INTEGER      --卡对象

				,MST_CRD_IND                          INTEGER      --主/副卡标志

				,NGO_CRD_IND                          INTEGER      --协议卡类型

				,MULT_CCY_F                           SMALLINT     --双币卡标志

				,PD_GRP_CD                            CHARACTER(2) --产品类

				,PD_SUB_CD                            CHARACTER(3) --产品子代码

				,BIZ_CGY_TP_ID                        INTEGER      --业务类别

				,CCY                                  CHARACTER(3) --币种

				,ACG_DT                               DATE         --日期YYYY-MM-DD

				,CDR_YR                               SMALLINT     --年份YYYY

				,CDR_MTH                              SMALLINT     --月份MM

				,ACT_NBR_AC                           INTEGER      --实际账户数

				,NBR_EFF_CRD                          INTEGER      --正常卡数

				,NBR_UNATVD_CR_CRD                    INTEGER      --未启用信用卡数量

				,NBR_UNATVD_CHGD_CRD                  INTEGER      --已换卡未启用卡数

				,NBR_EXP_CRD                          INTEGER      --过期卡数

				,NBR_DRMT_CRD                         INTEGER      --睡眠卡数量

				,NBR_CRD_CLECTD                       INTEGER      --已收卡数

				,NBR_NEW_CRD                          INTEGER      --新开卡数

				,NBR_CRD_CLD                          INTEGER      --销卡数

				,NBR_NEW_CST                          INTEGER      --新增客户数

				,NBR_CST_CLD                          INTEGER      --客户销户数

				,NBR_CRD_CHG                          INTEGER      --换卡数

				,NBR_AC_Opened                        INTEGER      --开户账户数

				,NBR_AC_CLS                           INTEGER      --销户账户数

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              INTEGER      --客户数

				-----------------------------Start of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            INTEGER      --余额小于10的睡眠卡数量

				,NBR_DRMT_AC_WITH_LOW_BAL							INTEGER      --余额小于10的睡眠户数量

				,NBR_DRMT_AC													INTEGER      --睡眠户数量

				-----------------------------End of 20091208---------------------------------

)			

ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); 	--



DECLARE GLOBAL TEMPORARY TABLE TMP_CRD_PRFL_CRD_DLY_SMY_DB(

				 OU_ID                                CHARACTER(18)--机构号

				,CRD_TP_ID                            INTEGER      --卡类型

				,Is_CR_CRD_F                          SMALLINT     --是否为贷记卡

				,CRD_Brand_TP_Id                      INTEGER      --卡品牌类型

				,CRD_PRVL_TP_ID                       INTEGER      --卡级别

				,PSBK_RLTD_F                          SMALLINT     --卡折相关标识

				,IS_NONGXIN_CRD_F                     SMALLINT     --丰收卡/农信卡标识

				,ENT_IDV_IND                          INTEGER      --卡对象

				,MST_CRD_IND                          INTEGER      --主/副卡标志

				,NGO_CRD_IND                          INTEGER      --协议卡类型

				,MULT_CCY_F                           SMALLINT     --双币卡标志

				,PD_GRP_CD                            CHARACTER(2) --产品类

				,PD_SUB_CD                            CHARACTER(3) --产品子代码

				,BIZ_CGY_TP_ID                        INTEGER      --业务类别

				,CCY                                  CHARACTER(3) --币种

				,ACG_DT                               DATE         --日期YYYY-MM-DD

				,CDR_YR                               SMALLINT     --年份YYYY

				,CDR_MTH                              SMALLINT     --月份MM

				,ACT_NBR_AC                           INTEGER      --实际账户数

				,NBR_EFF_CRD                          INTEGER      --正常卡数

				,NBR_UNATVD_CR_CRD                    INTEGER      --未启用信用卡数量

				,NBR_UNATVD_CHGD_CRD                  INTEGER      --已换卡未启用卡数

				,NBR_EXP_CRD                          INTEGER      --过期卡数

				,NBR_DRMT_CRD                         INTEGER      --睡眠卡数量

				,NBR_CRD_CLECTD                       INTEGER      --已收卡数

				,NBR_NEW_CRD                          INTEGER      --新开卡数

				,NBR_CRD_CLD                          INTEGER      --销卡数

				,NBR_NEW_CST                          INTEGER      --新增客户数

				,NBR_CST_CLD                          INTEGER      --客户销户数

				,NBR_CRD_CHG                          INTEGER      --换卡数

				,NBR_AC_Opened                        INTEGER      --开户账户数

				,NBR_AC_CLS                           INTEGER      --销户账户数

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              INTEGER      --客户数

				-----------------------------End of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            INTEGER      --余额小于10的睡眠卡数量

				,NBR_DRMT_AC_WITH_LOW_BAL							INTEGER      --余额小于10的睡眠户数量

				,NBR_DRMT_AC													INTEGER      --睡眠户数量

				-----------------------------End of 20091208---------------------------------

)			

ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); --



INSERT INTO SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR

(	   OU_ID                                --机构号

		,CRD_TP_ID                            --卡类型

		,Is_CR_CRD_F                          --是否为贷记卡

		,CRD_Brand_TP_Id                      --卡品牌类型

		,CRD_PRVL_TP_ID                       --卡级别

		,PSBK_RLTD_F                          --卡折相关标识

		,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

		,ENT_IDV_IND                          --卡对象

		,MST_CRD_IND                          --主/副卡标志

		,NGO_CRD_IND                          --协议卡类型

		,MULT_CCY_F                           --双币卡标志

		,PD_GRP_CD                            --产品类

		,PD_SUB_CD                            --产品子代码

		,BIZ_CGY_TP_ID                        --业务类别

		,CCY                                  --币种

		,ACG_DT                               --日期YYYY-MM-DD

		,CDR_YR                               --年份YYYY

		,CDR_MTH                              --月份MM

		,ACT_NBR_AC                           --实际账户数

		,NBR_EFF_CRD                          --正常卡数

		,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

		,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

		,NBR_EXP_CRD                          --过期卡数

		,NBR_DRMT_CRD                         --睡眠卡数量

		,NBR_CRD_CLECTD                       --已收卡数

		,NBR_NEW_CRD                          --新开卡数

		,NBR_CRD_CLD                          --销卡数

		,NBR_NEW_CST                          --新增客户数

		,NBR_CST_CLD                          --客户销户数

		,NBR_CRD_CHG                          --换卡数

		,NBR_AC_Opened                        --开户账户数

		,NBR_AC_CLS                           --销户账户数 

		-----------------------------Start of 20091203---------------------------------

		,NBR_CST                              --客户数

		-----------------------------End of 20091203---------------------------------

		-----------------------------Start of 20091208---------------------------------

		,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		,NBR_DRMT_AC										      --睡眠户数量

		-----------------------------End of 20091208---------------------------------		

)

----------------------Start on 20100521-------------------------------------------

With T_ACT_CC_AC as (

    select distinct AC_AR_Id as CC_AC_AR_ID                    

    from SMY.CR_CRD_SMY

    where DRMT_CRD_F = 0   --Exists at least one non-dormant card

)

----------------------End on 20100521-------------------------------------------	

SELECT 

  	 CR_CRD.OU_ID      AS OU_ID                           --机构号              

  	,CR_CRD.CR_CRD_TP_ID               AS CRD_TP_ID                       --卡类型              

  	,1                        AS Is_CR_CRD_F                     --是否为贷记卡        

  	,CR_CRD.CRD_BRAND_TP_ID         AS CRD_Brand_TP_Id                 --卡品牌类型          

  	,CR_CRD.CRD_PRVL_TP_ID         AS CRD_PRVL_TP_ID                  --卡级别              

  	,0                        AS PSBK_RLTD_F                     --卡折相关标识        

  	,0                        AS IS_NONGXIN_CRD_F                --丰收卡/农信卡标识   

  	,CR_CRD.ENT_IDV_IND        AS ENT_IDV_IND                     --卡对象              

  	,CR_CRD.MST_CRD_IND            AS MST_CRD_IND                     --主/副卡标志         

  	,CR_CRD.NGO_CRD_IND            AS NGO_CRD_IND                     --协议卡类型          

  	,CR_CRD.MULT_CCY_F            AS MULT_CCY_F                      --双币卡标志          

  	,CR_CRD.PD_GRP_CD              AS PD_GRP_CD                       --产品类                     

  	,CR_CRD.PD_SUB_CD              AS PD_SUB_CD                       --产品子代码          

  	,CR_CRD.BIZ_CGY_TP_ID          AS BIZ_CGY_TP_ID                   --业务类别            

  	,CR_CRD.CCY               AS CCY                             --币种                                                          

  	,SMY_DATE                 AS ACG_DT                          --日期YYYY-MM-DD      

  	,CUR_YEAR                 AS CDR_YR                          --年份YYYY            

  	,CUR_MONTH                AS CDR_MTH                         --月份MM              

  	,SUM(

  				case when CC_AC.AR_LCS_TP_ID = 20370007 --正常账户  

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --作废卡            

  	      then 1 else 0 end

  			) AS ACT_NBR_AC                      --实际账户数          

  	,SUM(

  				case when CRD_LCS_TP_ID = 11920001 --正常               

  	      then 1 else 0 end

  	    )   AS NBR_EFF_CRD                     --正常卡数            

  	,SUM(

  			  case when CRD_LCS_TP_ID in (11920002,11920003)

  			  then 1 else 0 end

  			)                     AS NBR_UNATVD_CR_CRD               --未启用信用卡数量    

  	,SUM(

  			  case when CRD_LCS_TP_ID = 11920003

  			  then 1 else 0 end

  			)                        AS NBR_UNATVD_CHGD_CRD             --已换卡未启用卡数    

  	,SUM(

  			  case when EXP_MTH_YEAR < CUR_MTH_YEAR AND CRD_LCS_TP_ID = 11920001 --正常

  			  then 1 else 0 end

  			)                        AS NBR_EXP_CRD                     --过期卡数            

  	,SUM(

  	--------------Start on 20100521-----------------------------------------------------------------  	

		--  	----------------------------------------Start on 20100114------------------------------------------

		--  	--			case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 

		--  	--			----------------------------Start on 20091215------------------------------------------

		--  	--								and DAYS(SMY_DATE) - DAYS(CR_CRD.EFF_DT) > 180

		--  	--								and DAYS(SMY_DATE) - DAYS(CRD_DLVD_DT) > 180

		--  	--			----------------------------End on 20091215------------------------------------------  	

		--  	        case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180   				

		--  									and DAYS(SMY_DATE) - DAYS(CR_CRD.EFF_DT) >= 180

		--  									and DAYS(SMY_DATE) - DAYS(CRD_DLVD_DT) >= 180

		--     ----------------------------------------End on 20100114------------------------------------------  				

		--  									and CRD_LCS_TP_ID in ( 11920001 --正常

		--  																				,11920002 --新发卡未启用

		--  																				,11920003 --新换卡未启用

		--  																			 )

		--  			  then 1 else 0 end

         CR_CRD.DRMT_CRD_F  			  

    ---------------End on 20100521-----------------------------------------------------------------   			  	

  			)                        AS NBR_DRMT_CRD                    --睡眠卡数量          

  	,SUM(

  			  case when CRD_LCS_TP_ID = 11920004 --已收卡

  			  then 1 else 0 end  			

  			)                        AS NBR_CRD_CLECTD                  --已收卡数            

  	,SUM(

  	      --case when CR_CRD.EFF_DT = SMY_DATE

  	      case when CR_CRD.CRD_DLVD_DT = SMY_DATE                   

  	    	then 1 else 0 end

  			)                        AS NBR_NEW_CRD                     --新开卡数 ->新发卡数         

  	,SUM(

  			  case when CR_CRD.END_DT = SMY_DATE AND CRD_LCS_TP_ID = 11920005

  	    	then 1 else 0 end

  			)                        AS NBR_CRD_CLD                     --销卡数              

  	,SUM(

  				case when CST_INF.EFF_CST_DT = SMY_DATE

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --作废卡            

  	    	then 1 else 0 end

  			)                        AS NBR_NEW_CST                     --新增客户数                     

  	,0                        AS NBR_CST_CLD                     --客户销户数          

  	,SUM(

  			  case when CR_CRD.CRD_CHG_DT = SMY_DATE

  	    	then 1 else 0 end

  			)                     AS NBR_CRD_CHG                     --换卡数              

  	,SUM(

  				case when CC_AC.EFF_DT = SMY_DATE

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --作废卡            

  	    	then 1 else 0 end

  			)                        AS NBR_AC_Opened                   --开户账户数          

  	,SUM(

  				case when CC_AC.END_DT = SMY_DATE

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --作废卡            

  	    	then 1 else 0 end

  			)                        AS NBR_AC_CLS                      --销户账户数 		    

  	-----------------------------Start of 20091203---------------------------------

		,SUM(

					case when CR_CRD.CRD_LCS_TP_ID = 11920001 --正常

  			  then 1 else 0 end

				)

		AS NBR_CST                                --客户数

	  -----------------------------End of 20091203---------------------------------

		

	  -----------------------------Start of 20091208---------------------------------

		,SUM(

					-----------------------------Start of 20100521---------------------------------  		

					--		  -------------------------------------Start on 20100114----------------------------------------------------------

					--		  ----case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 and CRD_LCS_TP_ID = 11920001 and CR_CRD.AC_BAL_AMT < 10

					--		  --case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180 and CRD_LCS_TP_ID = 11920001 and CR_CRD.AC_BAL_AMT < 10

					--		  -------------------------------------End on 20100114----------------------------------------------------------

					case when CR_CRD.DRMT_CRD_F = 1 and CR_CRD.AC_BAL_AMT < 10

				  -----------------------------End of 20100521---------------------------------  		

  			  then 1 else 0 end

				)  as NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量				

	  --------------------------------Start on 20100521---------------------------------------

	  --根据柴善良的需求，贷记卡不需要统计这个指标，因此不针对余额进行区分，只取与NBR_DRMT_AC相同的值

		--,SUM(

		--      -------------------------------------Start on 20100114----------------------------------------------------------

		--			--case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 and CC_AC.AR_LCS_TP_ID = 20370007 and CC_AC.BAL_AMT < 10

		--			case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180 and CC_AC.AR_LCS_TP_ID = 20370007 and CC_AC.BAL_AMT < 10

		--			-------------------------------------End on 20100114----------------------------------------------------------

		--								and CR_CRD.CRD_LCS_TP_ID <> 11920006 --作废卡            

  	--		  then 1 else 0 end

		--		)  as	NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		,count(distinct CR_CRD.AC_AR_ID) - count(distinct T_ACT_CC_AC.CC_AC_AR_ID) as	NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

	  --------------------------------End on 20100521---------------------------------------

		--------------------------------Start on 20100521---------------------------------------

		--,SUM(

		--			-------------------------------------Start on 20100114----------------------------------------------------------

		--			--case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 and CC_AC.AR_LCS_TP_ID = 20370007 

		--			case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180 and CC_AC.AR_LCS_TP_ID = 20370007 

		--			-------------------------------------End on 20100114----------------------------------------------------------

		--								and CR_CRD.CRD_LCS_TP_ID <> 11920006 --作废卡            

  	--		  then 1 else 0 end

		--		)  as NBR_DRMT_AC										      --睡眠户数量

		,count(distinct CR_CRD.AC_AR_ID) - count(distinct T_ACT_CC_AC.CC_AC_AR_ID) as NBR_DRMT_AC										      --睡眠户数量

	  --------------------------------End on 20100521---------------------------------------

		-----------------------------End of 20091208---------------------------------				

FROM SMY.CR_CRD_SMY AS CR_CRD

LEFT JOIN SMY.CST_INF AS CST_INF on CR_CRD.CST_ID = CST_INF.CST_ID

LEFT JOIN SOR.CC_AC_AR AS CC_AC  on CR_CRD.AC_AR_ID = CC_AC.CC_AC_AR_ID AND CR_CRD.CCY =CC_AC.DNMN_CCY_ID

-----------------------Start on 20100521------------------------

left join T_ACT_CC_AC on CR_CRD.AC_AR_ID = T_ACT_CC_AC.CC_AC_AR_ID

-----------------------End on 20100521------------------------

GROUP BY 

  	 CR_CRD.OU_ID                   

  	,CR_CRD.CR_CRD_TP_ID                            

  	,CR_CRD.CRD_BRAND_TP_ID                      

  	,CR_CRD.CRD_PRVL_TP_ID                      

  	,CR_CRD.ENT_IDV_IND                     

  	,CR_CRD.MST_CRD_IND                         

  	,CR_CRD.NGO_CRD_IND                         

  	,CR_CRD.MULT_CCY_F                         

  	,CR_CRD.PD_GRP_CD                               

  	,CR_CRD.PD_SUB_CD                           

  	,CR_CRD.BIZ_CGY_TP_ID                           

  	,CR_CRD.CCY                                                               

  	,SMY_DATE                              

  	,CUR_YEAR                              

  	,CUR_MONTH

;--

DECLARE GLOBAL TEMPORARY TABLE TMP(

				 OU_ID                                CHARACTER(18)--机构号

				,CRD_TP_ID                            INTEGER      --卡类型

				,Is_CR_CRD_F                          SMALLINT     --是否为贷记卡

				,CRD_Brand_TP_Id                      INTEGER      --卡品牌类型

				,CRD_PRVL_TP_ID                       INTEGER      --卡级别

				,PSBK_RLTD_F                          SMALLINT     --卡折相关标识

				,IS_NONGXIN_CRD_F                     SMALLINT     --丰收卡/农信卡标识

				,ENT_IDV_IND                          INTEGER      --卡对象

				,MST_CRD_IND                          INTEGER      --主/副卡标志

				,NGO_CRD_IND                          INTEGER      --协议卡类型

				,MULT_CCY_F                           SMALLINT     --双币卡标志

				,PD_GRP_CD                            CHARACTER(2) --产品类

				,PD_SUB_CD                            CHARACTER(3) --产品子代码

				,BIZ_CGY_TP_ID                        INTEGER      --业务类别

				,CCY                                  CHARACTER(3) --币种

				,ACG_DT                               DATE         --日期YYYY-MM-DD

				,CDR_YR                               SMALLINT     --年份YYYY

				,CDR_MTH                              SMALLINT     --月份MM

				,ACT_NBR_AC                           INTEGER      --实际账户数

				,NBR_EFF_CRD                          INTEGER      --正常卡数

				,NBR_UNATVD_CR_CRD                    INTEGER      --未启用信用卡数量

				,NBR_UNATVD_CHGD_CRD                  INTEGER      --已换卡未启用卡数

				,NBR_EXP_CRD                          INTEGER      --过期卡数

				,NBR_DRMT_CRD                         INTEGER      --睡眠卡数量

				,NBR_CRD_CLECTD                       INTEGER      --已收卡数

				,NBR_NEW_CRD                          INTEGER      --新开卡数

				,NBR_CRD_CLD                          INTEGER      --销卡数

				,NBR_NEW_CST                          INTEGER      --新增客户数

				,NBR_CST_CLD                          INTEGER      --客户销户数

				,NBR_CRD_CHG                          INTEGER      --换卡数

				,NBR_AC_Opened                        INTEGER      --开户账户数

				,NBR_AC_CLS                           INTEGER      --销户账户数

				-----------------------------Start of 20091203---------------------------------

				,CST_ID                              CHARACTER(18)      --客户ID

				-----------------------------End of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            INTEGER      --余额小于10的睡眠卡数量

				,NBR_DRMT_AC_WITH_LOW_BAL							INTEGER      --余额小于10的睡眠户数量

				,NBR_DRMT_AC													INTEGER      --睡眠户数量

				-----------------------------End of 20091208---------------------------------

)			
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); --

CREATE INDEX SESSION.TMP_IDX ON SESSION.TMP(OU_ID,CRD_TP_ID,CRD_BRAND_TP_ID,ENT_IDV_IND,NGO_CRD_IND,PD_GRP_CD,PD_SUB_CD,BIZ_CGY_TP_ID,CCY,ACG_DT,CDR_YR,CDR_MTH,IS_NONGXIN_CRD_F,PSBK_RLTD_F);

INSERT INTO SESSION.TMP

	( OU_ID                                --机构号

	 ,CRD_TP_ID                            --卡类型

	 ,Is_CR_CRD_F                          --是否为贷记卡

	 ,CRD_Brand_TP_Id                      --卡品牌类型

	 ,CRD_PRVL_TP_ID                       --卡级别

	 ,PSBK_RLTD_F                          --卡折相关标识

	 ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

	 ,ENT_IDV_IND                          --卡对象

	 ,MST_CRD_IND                          --主/副卡标志

	 ,NGO_CRD_IND                          --协议卡类型

	 ,MULT_CCY_F                           --双币卡标志

	 ,PD_GRP_CD                            --产品类

	 ,PD_SUB_CD                            --产品子代码

	 ,BIZ_CGY_TP_ID                        --业务类别

	 ,CCY                                  --币种

	 ,ACG_DT                               --日期YYYY-MM-DD

	 ,CDR_YR                               --年份YYYY

	 ,CDR_MTH                              --月份MM

	 ,ACT_NBR_AC                           --实际账户数

	 ,NBR_EFF_CRD                          --正常卡数

	 ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

	 ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

	 ,NBR_EXP_CRD                          --过期卡数

	 ,NBR_DRMT_CRD                         --睡眠卡数量

	 ,NBR_CRD_CLECTD                       --已收卡数

	 ,NBR_NEW_CRD                          --新开卡数

	 ,NBR_CRD_CLD                          --销卡数

	 ,NBR_NEW_CST                          --新增客户数

	 ,NBR_CST_CLD                          --客户销户数

	 ,NBR_CRD_CHG                          --换卡数

	 ,NBR_AC_Opened                        --开户账户数

	 ,NBR_AC_CLS                            --销户账户数  

  -----------------------------Start of 20091203---------------------------------

	 ,CST_ID                              --客户ID

	-----------------------------End of 20091203---------------------------------	     

	-----------------------------Start of 20091208---------------------------------

	,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

	,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

	,NBR_DRMT_AC										      --睡眠户数量

  -----------------------------End of 20091208---------------------------------			

)

---------------Start on 20100521-------------------------------

with T_ACT_CST as (

  select  CRD_NO

         ,CST_ID

  from SMY.DB_CRD_SMY

  where CRD_LCS_TP_ID = 11920001 --正常

)

---------------End on 20100521-------------------------------

SELECT 

  	 DB_CRD.OU_ID      AS OU_ID                           --机构号              

  	,DB_CRD.DB_CRD_TP_ID               AS CRD_TP_ID       --卡类型              

  	,0                        AS Is_CR_CRD_F              --是否为贷记卡        

  	,DB_CRD.CRD_BRAND_TP_ID         AS CRD_Brand_TP_Id    --卡品牌类型          

  	,-1         AS CRD_PRVL_TP_ID                         --卡级别              

  	,PSBK_RLTD_F                        AS PSBK_RLTD_F    --卡折相关标识        

  	,DB_CRD.IS_NONGXIN_CRD_F AS IS_NONGXIN_CRD_F                --丰收卡/农信卡标识   

  	,DB_CRD.ENT_IDV_IND        AS ENT_IDV_IND             --卡对象              

  	,-1            AS MST_CRD_IND                          --主/副卡标志         

  	,DB_CRD.NGO_CRD_IND            AS NGO_CRD_IND         --协议卡类型          

  	,-1            AS MULT_CCY_F                          --双币卡标志          

  	,DB_CRD.PD_GRP_CODE              AS PD_GRP_CD           --产品类                     

  	,DB_CRD.PD_SUB_CODE              AS PD_SUB_CD           --产品子代码          

  	,DB_CRD.BIZ_CGY_TP_ID          AS BIZ_CGY_TP_ID       --业务类别            

  	,DB_CRD.CCY               AS CCY                      --币种                                                          

  	,SMY_DATE                 AS ACG_DT                   --日期YYYY-MM-DD      

  	,CUR_YEAR                 AS CDR_YR                   --年份YYYY            

  	,CUR_MONTH                AS CDR_MTH                  --月份MM              

  	,case when DEP_AC.AR_LCS_TP_ID = 20370007 --正常账户              

  									and CRD_LCS_TP_ID = 11920001 --正常               

  	      then 1 else 0 end

  	 AS ACT_NBR_AC                                   --实际账户数          

  	,case when CRD_LCS_TP_ID = 11920001 --正常               

  	      then 1 else 0 end

  	 AS NBR_EFF_CRD                     --正常卡数            

  	,0                     AS NBR_UNATVD_CR_CRD               --未启用信用卡数量    

  	,0                     AS NBR_UNATVD_CHGD_CRD             --已换卡未启用卡数    

  	,case when EXP_MTH_YEAR < CUR_MTH_YEAR AND CRD_LCS_TP_ID = 11920001 --正常

  			  then 1 else 0 end

  	 AS NBR_EXP_CRD                     --过期卡数            

  	,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and CRD_LCS_TP_ID = 11920001

  			  then 1 else 0 end

  	 AS NBR_DRMT_CRD                    --睡眠卡数量          

  	,case when CRD_LCS_TP_ID = 11920004 --已收卡

  			  then 1 else 0 end  			

  	 AS NBR_CRD_CLECTD                  --已收卡数            

  	,case when DB_CRD.EFF_DT = SMY_DATE

  	    	then 1 else 0 end

  	 AS NBR_NEW_CRD                     --新开卡数            

  	,case when DB_CRD.END_DT = SMY_DATE AND CRD_LCS_TP_ID = 11920005

  	    	then 1 else 0 end

  	 AS NBR_CRD_CLD                     --销卡数              

  	,case when CST_INF.EFF_CST_DT = SMY_DATE

  									and CRD_LCS_TP_ID = 11920001 --正常               

  	    	then 1 else 0 end

  	 AS NBR_NEW_CST                     --新增客户数                     

  	,0                        AS NBR_CST_CLD                     --客户销户数          

  	,case when DB_CRD.CRD_CHG_DT = SMY_DATE

  	    	then 1 else 0 end

  	 AS NBR_CRD_CHG                     --换卡数              

  	,case when DEP_AC.EFF_DT = SMY_DATE

  									and CRD_LCS_TP_ID = 11920001 --正常               

  	    	then 1 else 0 end

  	 AS NBR_AC_Opened                   --开户账户数          

  	,case when DEP_AC.END_DT = SMY_DATE

  									and CRD_LCS_TP_ID = 11920001 --正常               

  	    	then 1 else 0 end

  	 AS NBR_AC_CLS                      --销户账户数 

  	-------------------Start on 20100521-------------------------------------

  	-------------------------------Start of 20091203---------------------------------

		--,SUM(

		--			case when DB_CRD.CRD_LCS_TP_ID = 11920001 --正常

  	--		  then 1 else 0 end

		--		)

		--AS NBR_CST                                --客户数

		-------------------------------End of 20091203---------------------------------		    

		,T_ACT_CST.CST_ID AS CST_ID                                --客户ID

		-------------------Start on 20100521-------------------------------------

		-----------------------------Start of 20091208---------------------------------

		,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and CRD_LCS_TP_ID = 11920001 and DB_CRD.AC_BAL_AMT < 10

					------------------------------------------End on 20100114------------------------------------

  			  then 1 else 0 end

		 as NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and DEP_AC.AR_LCS_TP_ID = 20370007 and DB_CRD.AC_BAL_AMT < 10

					------------------------------------------End on 20100114------------------------------------

										and CRD_LCS_TP_ID = 11920001 --正常               

  			  then 1 else 0 end

		 as	NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and DEP_AC.AR_LCS_TP_ID = 20370007 

				 ------------------------------------------End on 20100114------------------------------------	

										and CRD_LCS_TP_ID = 11920001 --正常               

  			  then 1 else 0 end

		 as NBR_DRMT_AC										      --睡眠户数量

		-----------------------------End of 20091208---------------------------------				

FROM SMY.DB_CRD_SMY AS DB_CRD

LEFT JOIN SMY.CST_INF AS CST_INF on DB_CRD.CST_ID = CST_INF.CST_ID

LEFT JOIN SOR.DMD_DEP_MN_AR AS DEP_AC  on DB_CRD.AC_AR_ID = DEP_AC.DMD_DEP_AR_ID

------------------------Start on 20100521----------------------

left join T_ACT_CST on DB_CRD.CRD_NO = T_ACT_CST.CRD_NO

------------------------End on 20100521----------------------
;

INSERT INTO SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB
(
    OU_ID                                --机构号
	 ,CRD_TP_ID                            --卡类型
	 ,Is_CR_CRD_F                          --是否为贷记卡
	 ,CRD_Brand_TP_Id                      --卡品牌类型
	 ,CRD_PRVL_TP_ID                       --卡级别
	 ,PSBK_RLTD_F                          --卡折相关标识
	 ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识
	 ,ENT_IDV_IND                          --卡对象
	 ,MST_CRD_IND                          --主/副卡标志
	 ,NGO_CRD_IND                          --协议卡类型
	 ,MULT_CCY_F                           --双币卡标志
	 ,PD_GRP_CD                            --产品类
	 ,PD_SUB_CD                            --产品子代码
	 ,BIZ_CGY_TP_ID                        --业务类别
	 ,CCY                                  --币种
	 ,ACG_DT                               --日期YYYY-MM-DD
	 ,CDR_YR                               --年份YYYY
	 ,CDR_MTH                              --月份MM
	 ,ACT_NBR_AC                           --实际账户数
	 ,NBR_EFF_CRD                          --正常卡数
	 ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量
	 ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数
	 ,NBR_EXP_CRD                          --过期卡数
	 ,NBR_DRMT_CRD                         --睡眠卡数量
	 ,NBR_CRD_CLECTD                       --已收卡数
	 ,NBR_NEW_CRD                          --新开卡数
	 ,NBR_CRD_CLD                          --销卡数
	 ,NBR_NEW_CST                          --新增客户数
	 ,NBR_CST_CLD                          --客户销户数
	 ,NBR_CRD_CHG                          --换卡数
	 ,NBR_AC_Opened                        --开户账户数
	 ,NBR_AC_CLS                           --销户账户数  
	 ,NBR_CST                              --客户数
	 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量
	 ,NBR_DRMT_AC_WITH_LOW_BAL					   --余额小于10的睡眠户数量
	 ,NBR_DRMT_AC										       --睡眠户数量
)
SELECT
  	 T.OU_ID
  	,T.CRD_TP_ID
  	,0 AS Is_CR_CRD_F
  	,T.CRD_BRAND_TP_ID
  	,-1 AS CRD_PRVL_TP_ID
  	,T.PSBK_RLTD_F
  	,T.IS_NONGXIN_CRD_F
  	,T.ENT_IDV_IND
  	,-1 AS MST_CRD_IND
  	,T.NGO_CRD_IND
  	,-1 AS MULT_CCY_F
  	,T.PD_GRP_CD
  	,T.PD_SUB_CD
  	,T.BIZ_CGY_TP_ID
  	,T.CCY
  	,T.ACG_DT
  	,T.CDR_YR
  	,T.CDR_MTH
  	,SUM(T.ACT_NBR_AC)
  	,SUM(T.NBR_EFF_CRD)
  	,0 AS NBR_UNATVD_CR_CRD
  	,0 AS NBR_UNATVD_CHGD_CRD
  	,SUM(T.NBR_EXP_CRD)
  	,SUM(T.NBR_DRMT_CRD)
  	,SUM(T.NBR_CRD_CLECTD)
  	,SUM(T.NBR_NEW_CRD)
  	,SUM(T.NBR_CRD_CLD)
  	,SUM(T.NBR_NEW_CST)
  	,0 AS NBR_CST_CLD
  	,SUM(T.NBR_CRD_CHG)
  	,SUM(T.NBR_AC_Opened)
  	,SUM(T.NBR_AC_CLS)
		,count(distinct T.CST_ID) AS NBR_CST
		,SUM(T.NBR_DRMT_CRD_WITH_LOW_BAL)
		,SUM(T.NBR_DRMT_AC_WITH_LOW_BAL)
		,SUM(T.NBR_DRMT_AC)
FROM SESSION.TMP AS T
GROUP BY 
  	 T.OU_ID
  	,T.CRD_TP_ID
  	,T.CRD_BRAND_TP_ID
  	,T.ENT_IDV_IND
  	,T.NGO_CRD_IND
  	,T.PD_GRP_CD
  	,T.PD_SUB_CD
  	,T.BIZ_CGY_TP_ID
  	,T.CCY
  	,T.ACG_DT
  	,T.CDR_YR
  	,T.CDR_MTH
  	,T.IS_NONGXIN_CRD_F
  	,T.PSBK_RLTD_F
;  	--



GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--



SET SMY_STEPDESC = '插入信用卡数据数据';--



IF CUR_DAY = 1 THEN  

   IF CUR_MONTH = 1 THEN --年初

      INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

      (  OU_ID                                --机构号

				,CRD_TP_ID                            --卡类型

				,Is_CR_CRD_F                          --是否为贷记卡

				,CRD_Brand_TP_Id                      --卡品牌类型

				,CRD_PRVL_TP_ID                       --卡级别

				,PSBK_RLTD_F                          --卡折相关标识

				,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				,ENT_IDV_IND                          --卡对象

				,MST_CRD_IND                          --主/副卡标志

				,NGO_CRD_IND                          --协议卡类型

				,MULT_CCY_F                           --双币卡标志

				,PD_GRP_CD                            --产品类

				,PD_SUB_CD                            --产品子代码

				,BIZ_CGY_TP_ID                        --业务类别

				,CCY                                  --币种

				,ACG_DT                               --日期YYYY-MM-DD

				,CDR_YR                               --年份YYYY

				,CDR_MTH                              --月份MM

				,ACT_NBR_AC                           --实际账户数

				,NBR_EFF_CRD                          --正常卡数

				,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				,NBR_EXP_CRD                          --过期卡数

				,NBR_DRMT_CRD                         --睡眠卡数量

				,NBR_CRD_CLECTD                       --已收卡数

				,NBR_NEW_CRD                          --新开卡数

				,NBR_CRD_CLD                          --销卡数

				,NBR_NEW_CST                          --新增客户数

				,NBR_CST_CLD                          --客户销户数

				,NBR_CRD_CHG                          --换卡数

				,NBR_AC_Opened                        --开户账户数

				,NBR_AC_CLS                           --销户账户数

				,TOT_MTD_NBR_NEW_CRD                  --月累计新开卡数

				,TOT_MTD_NBR_CRD_CLD                  --月累计销卡数

				,TOT_MTD_NBR_NEW_CST                  --月累计新增客户数

				,TOT_MTD_NBR_CST_CLD                  --月累计客户销户数

				,TOT_MTD_NBR_CRD_CHG                  --月累计换卡数

				,TOT_MTD_NBR_AC_Opened                --月累计开户账户数

				,TOT_MTD_NBR_AC_CLS                   --月累计销户账户数

				,TOT_MTD_NBR_CRD_CLECTD               --月累计已收卡数

				,TOT_QTD_NBR_NEW_CRD                  --季累计新开卡数

				,TOT_QTD_NBR_CRD_CLD                  --季累计销卡数

				,TOT_QTD_NBR_NEW_CST                  --季累计新增客户数

				,TOT_QTD_NBR_CST_CLD                  --季累计客户销户数

				,TOT_QTD_NBR_CRD_CHG                  --季累计换卡数

				,TOT_QTD_NBR_AC_Opened                --季累计开户账户数

				,TOT_QTD_NBR_AC_CLS                   --季累计销户账户数

				,TOT_QTD_NBR_CRD_CLECTD               --季累计已收卡数

				,TOT_YTD_NBR_NEW_CRD                  --年累计新开卡数

				,TOT_YTD_NBR_CRD_CLD                  --年累计销卡数

				,TOT_YTD_NBR_NEW_CST                  --年累计新增客户数

				,TOT_YTD_NBR_CST_CLD                  --年累计客户销户数

				,TOT_YTD_NBR_CRD_CHG                  --年累计换卡数

				,TOT_YTD_NBR_AC_Opened                --年累计开户账户数

				,TOT_YTD_NBR_AC_CLS                   --年累计销户账户数

				,TOT_YTD_NBR_CRD_CLECTD               --年累计已收卡数

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              --客户数

				-----------------------------End of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

				,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

				,NBR_DRMT_AC										      --睡眠户数量

			  -----------------------------End of 20091208---------------------------------			

			)				

      SELECT 

         OU_ID                                --机构号

				,CRD_TP_ID                            --卡类型

				,Is_CR_CRD_F                          --是否为贷记卡

				,CRD_Brand_TP_Id                      --卡品牌类型

				,CRD_PRVL_TP_ID                       --卡级别

				,PSBK_RLTD_F                          --卡折相关标识

				,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				,ENT_IDV_IND                          --卡对象

				,MST_CRD_IND                          --主/副卡标志

				,NGO_CRD_IND                          --协议卡类型

				,MULT_CCY_F                           --双币卡标志

				,PD_GRP_CD                            --产品类

				,PD_SUB_CD                            --产品子代码

				,BIZ_CGY_TP_ID                        --业务类别

				,CCY                                  --币种

				,ACG_DT                               --日期YYYY-MM-DD

				,CDR_YR                               --年份YYYY

				,CDR_MTH                              --月份MM

				,ACT_NBR_AC                           --实际账户数

				,NBR_EFF_CRD                          --正常卡数

				,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				,NBR_EXP_CRD                          --过期卡数

				,NBR_DRMT_CRD                         --睡眠卡数量

				,NBR_CRD_CLECTD                       --已收卡数

				,NBR_NEW_CRD                          --新开卡数

				,NBR_CRD_CLD                          --销卡数

				,NBR_NEW_CST                          --新增客户数

				,NBR_CST_CLD                          --客户销户数

				,NBR_CRD_CHG                          --换卡数

				,NBR_AC_Opened                        --开户账户数

				,NBR_AC_CLS                           --销户账户数

				,NBR_NEW_CRD                          --月累计新开卡数    

				,NBR_CRD_CLD                          --月累计销卡数      

				,NBR_NEW_CST                          --月累计新增客户数  

				,NBR_CST_CLD                          --月累计客户销户数  

				,NBR_CRD_CHG                          --月累计换卡数      

				,NBR_AC_Opened                        --月累计开户账户数  

				,NBR_AC_CLS                           --月累计销户账户数  

				,NBR_CRD_CLECTD                       --月累计已收卡数    

				,NBR_NEW_CRD                          --季累计新开卡数    

				,NBR_CRD_CLD                          --季累计销卡数      

				,NBR_NEW_CST                          --季累计新增客户数  

				,NBR_CST_CLD                          --季累计客户销户数  

				,NBR_CRD_CHG                          --季累计换卡数      

				,NBR_AC_Opened                        --季累计开户账户数  

				,NBR_AC_CLS                           --季累计销户账户数  

				,NBR_CRD_CLECTD                       --季累计已收卡数    

				,NBR_NEW_CRD                          --年累计新开卡数    

				,NBR_CRD_CLD                          --年累计销卡数      

				,NBR_NEW_CST                          --年累计新增客户数  

				,NBR_CST_CLD                          --年累计客户销户数  

				,NBR_CRD_CHG                          --年累计换卡数      

				,NBR_AC_Opened                        --年累计开户账户数  

				,NBR_AC_CLS                           --年累计销户账户数  

				,NBR_CRD_CLECTD                       --年累计已收卡数    

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              --客户数

				-----------------------------End of 20091203---------------------------------						

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

				,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

				,NBR_DRMT_AC										      --睡眠户数量

			  -----------------------------End of 20091208---------------------------------			

		  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR;--

		  

   ELSEIF CUR_MONTH IN (4, 7 ,10) THEN --季初

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          ( OU_ID                                --机构号

				   ,CRD_TP_ID                            --卡类型

				   ,Is_CR_CRD_F                          --是否为贷记卡

				   ,CRD_Brand_TP_Id                      --卡品牌类型

				   ,CRD_PRVL_TP_ID                       --卡级别

				   ,PSBK_RLTD_F                          --卡折相关标识

				   ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				   ,ENT_IDV_IND                          --卡对象

				   ,MST_CRD_IND                          --主/副卡标志

				   ,NGO_CRD_IND                          --协议卡类型

				   ,MULT_CCY_F                           --双币卡标志

				   ,PD_GRP_CD                            --产品类

				   ,PD_SUB_CD                            --产品子代码

				   ,BIZ_CGY_TP_ID                        --业务类别

				   ,CCY                                  --币种

				   ,ACG_DT                               --日期YYYY-MM-DD

				   ,CDR_YR                               --年份YYYY

				   ,CDR_MTH                              --月份MM

				   ,ACT_NBR_AC                           --实际账户数

				   ,NBR_EFF_CRD                          --正常卡数

				   ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				   ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				   ,NBR_EXP_CRD                          --过期卡数

				   ,NBR_DRMT_CRD                         --睡眠卡数量

				   ,NBR_CRD_CLECTD                       --已收卡数

				   ,NBR_NEW_CRD                          --新开卡数

				   ,NBR_CRD_CLD                          --销卡数

				   ,NBR_NEW_CST                          --新增客户数

				   ,NBR_CST_CLD                          --客户销户数

				   ,NBR_CRD_CHG                          --换卡数

				   ,NBR_AC_Opened                        --开户账户数

				   ,NBR_AC_CLS                           --销户账户数

				   ,TOT_MTD_NBR_NEW_CRD                  --月累计新开卡数        

				   ,TOT_MTD_NBR_CRD_CLD                  --月累计销卡数          

				   ,TOT_MTD_NBR_NEW_CST                  --月累计新增客户数      

				   ,TOT_MTD_NBR_CST_CLD                  --月累计客户销户数      

				   ,TOT_MTD_NBR_CRD_CHG                  --月累计换卡数          

				   ,TOT_MTD_NBR_AC_Opened                --月累计开户账户数      

				   ,TOT_MTD_NBR_AC_CLS                   --月累计销户账户数      

				   ,TOT_MTD_NBR_CRD_CLECTD               --月累计已收卡数        

				   ,TOT_QTD_NBR_NEW_CRD                  --季累计新开卡数        

				   ,TOT_QTD_NBR_CRD_CLD                  --季累计销卡数          

				   ,TOT_QTD_NBR_NEW_CST                  --季累计新增客户数      

				   ,TOT_QTD_NBR_CST_CLD                  --季累计客户销户数      

				   ,TOT_QTD_NBR_CRD_CHG                  --季累计换卡数          

				   ,TOT_QTD_NBR_AC_Opened                --季累计开户账户数      

				   ,TOT_QTD_NBR_AC_CLS                   --季累计销户账户数      

				   ,TOT_QTD_NBR_CRD_CLECTD               --季累计已收卡数        

				   ,TOT_YTD_NBR_NEW_CRD                  --年累计新开卡数        

				   ,TOT_YTD_NBR_CRD_CLD                  --年累计销卡数          

				   ,TOT_YTD_NBR_NEW_CST                  --年累计新增客户数      

				   ,TOT_YTD_NBR_CST_CLD                  --年累计客户销户数      

				   ,TOT_YTD_NBR_CRD_CHG                  --年累计换卡数          

				   ,TOT_YTD_NBR_AC_Opened                --年累计开户账户数      

				   ,TOT_YTD_NBR_AC_CLS                   --年累计销户账户数      

				   ,TOT_YTD_NBR_CRD_CLECTD               --年累计已收卡数        

				   -----------------------------Start of 20091203---------------------------------

					 ,NBR_CST                              --客户数

					 -----------------------------End of 20091203---------------------------------

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

					 ,NBR_DRMT_AC										      --睡眠户数量

				   -----------------------------End of 20091208---------------------------------			

				)

        SELECT 

          a.OU_ID                                --机构号

				  ,a.CRD_TP_ID                            --卡类型

				  ,a.Is_CR_CRD_F                          --是否为贷记卡

				  ,a.CRD_Brand_TP_Id                      --卡品牌类型

				  ,a.CRD_PRVL_TP_ID                       --卡级别

				  ,a.PSBK_RLTD_F                          --卡折相关标识

				  ,a.IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				  ,a.ENT_IDV_IND                          --卡对象

				  ,a.MST_CRD_IND                          --主/副卡标志

				  ,a.NGO_CRD_IND                          --协议卡类型

				  ,a.MULT_CCY_F                           --双币卡标志

				  ,a.PD_GRP_CD                            --产品类

				  ,a.PD_SUB_CD                            --产品子代码

				  ,a.BIZ_CGY_TP_ID                        --业务类别

				  ,a.CCY                                  --币种

				  ,a.ACG_DT                               --日期YYYY-MM-DD

				  ,a.CDR_YR                               --年份YYYY

				  ,a.CDR_MTH                              --月份MM

				  ,a.ACT_NBR_AC                           --实际账户数

				  ,a.NBR_EFF_CRD                          --正常卡数

				  ,a.NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				  ,a.NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				  ,a.NBR_EXP_CRD                          --过期卡数

				  ,a.NBR_DRMT_CRD                         --睡眠卡数量

				  ,a.NBR_CRD_CLECTD                       --已收卡数

				  ,a.NBR_NEW_CRD                          --新开卡数

				  ,a.NBR_CRD_CLD                          --销卡数

				  ,a.NBR_NEW_CST                          --新增客户数

				  ,a.NBR_CST_CLD                          --客户销户数

				  ,a.NBR_CRD_CHG                          --换卡数

				  ,a.NBR_AC_Opened                        --开户账户数

				  ,a.NBR_AC_CLS                           --销户账户数

				  ,a.NBR_NEW_CRD                          --月累计新开卡数    

				  ,a.NBR_CRD_CLD                          --月累计销卡数      

				  ,a.NBR_NEW_CST                          --月累计新增客户数  

				  ,a.NBR_CST_CLD                          --月累计客户销户数  

				  ,a.NBR_CRD_CHG                          --月累计换卡数      

				  ,a.NBR_AC_Opened                        --月累计开户账户数  

				  ,a.NBR_AC_CLS                           --月累计销户账户数  

				  ,a.NBR_CRD_CLECTD                       --月累计已收卡数    

				  ,a.NBR_NEW_CRD                          --季累计新开卡数    

				  ,a.NBR_CRD_CLD                          --季累计销卡数      

				  ,a.NBR_NEW_CST                          --季累计新增客户数  

				  ,a.NBR_CST_CLD                          --季累计客户销户数  

				  ,a.NBR_CRD_CHG                          --季累计换卡数      

				  ,a.NBR_AC_Opened                        --季累计开户账户数  

				  ,a.NBR_AC_CLS                           --季累计销户账户数  

          ,a.NBR_CRD_CLECTD                       --季累计已收卡数    

				  ,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     		--年累计新开卡数    																		        

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     			--年累计销卡数      														

          ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     			--年累计新增客户数  														

          ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     			--年累计客户销户数  														

          ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     			--年累计换卡数      														

          ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 			--年累计开户账户数

          --------------------------Start on 20100114----------------------------------------------------   														

          --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       			--年累计销户账户数  														

          --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)    --年累计已收卡数   

          ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       			--年累计销户账户数  														

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)    --年累计已收卡数 

          --------------------------End on 20100114---------------------------------------------------- 

          -----------------------------Start of 20091203---------------------------------

					,a.NBR_CST                              --客户数

					-----------------------------Start of 20091203--------------------------------- 																										

  			  -----------------------------Start of 20091208---------------------------------

	  			,a.NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

				  ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

				  ,a.NBR_DRMT_AC										      --睡眠户数量

			    -----------------------------End of 20091208---------------------------------								

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT  

    			 ;         --

      ELSE     --月初

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          (OU_ID                               --机构号

				   ,CRD_TP_ID                            --卡类型

				   ,Is_CR_CRD_F                          --是否为贷记卡

				   ,CRD_Brand_TP_Id                      --卡品牌类型

				   ,CRD_PRVL_TP_ID                       --卡级别

				   ,PSBK_RLTD_F                          --卡折相关标识

				   ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				   ,ENT_IDV_IND                          --卡对象

				   ,MST_CRD_IND                          --主/副卡标志

				   ,NGO_CRD_IND                          --协议卡类型

				   ,MULT_CCY_F                           --双币卡标志

				   ,PD_GRP_CD                            --产品类

				   ,PD_SUB_CD                            --产品子代码

				   ,BIZ_CGY_TP_ID                        --业务类别

				   ,CCY                                  --币种

				   ,ACG_DT                               --日期YYYY-MM-DD

				   ,CDR_YR                               --年份YYYY

				   ,CDR_MTH                              --月份MM

				   ,ACT_NBR_AC                           --实际账户数

				   ,NBR_EFF_CRD                          --正常卡数

				   ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				   ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				   ,NBR_EXP_CRD                          --过期卡数

				   ,NBR_DRMT_CRD                         --睡眠卡数量

				   ,NBR_CRD_CLECTD                       --已收卡数

				   ,NBR_NEW_CRD                          --新开卡数

				   ,NBR_CRD_CLD                          --销卡数

				   ,NBR_NEW_CST                          --新增客户数

				   ,NBR_CST_CLD                          --客户销户数

				   ,NBR_CRD_CHG                          --换卡数

				   ,NBR_AC_Opened                        --开户账户数

				   ,NBR_AC_CLS                           --销户账户数

				   ,TOT_MTD_NBR_NEW_CRD                  --月累计新开卡数        

				   ,TOT_MTD_NBR_CRD_CLD                  --月累计销卡数          

				   ,TOT_MTD_NBR_NEW_CST                  --月累计新增客户数      

				   ,TOT_MTD_NBR_CST_CLD                  --月累计客户销户数      

				   ,TOT_MTD_NBR_CRD_CHG                  --月累计换卡数          

				   ,TOT_MTD_NBR_AC_Opened                --月累计开户账户数      

				   ,TOT_MTD_NBR_AC_CLS                   --月累计销户账户数      

				   ,TOT_MTD_NBR_CRD_CLECTD               --月累计已收卡数        

				   ,TOT_QTD_NBR_NEW_CRD                  --季累计新开卡数        

				   ,TOT_QTD_NBR_CRD_CLD                  --季累计销卡数          

				   ,TOT_QTD_NBR_NEW_CST                  --季累计新增客户数      

				   ,TOT_QTD_NBR_CST_CLD                  --季累计客户销户数      

				   ,TOT_QTD_NBR_CRD_CHG                  --季累计换卡数          

				   ,TOT_QTD_NBR_AC_Opened                --季累计开户账户数      

				   ,TOT_QTD_NBR_AC_CLS                   --季累计销户账户数      

				   ,TOT_QTD_NBR_CRD_CLECTD               --季累计已收卡数        

				   ,TOT_YTD_NBR_NEW_CRD                  --年累计新开卡数        

				   ,TOT_YTD_NBR_CRD_CLD                  --年累计销卡数          

				   ,TOT_YTD_NBR_NEW_CST                  --年累计新增客户数      

				   ,TOT_YTD_NBR_CST_CLD                  --年累计客户销户数      

				   ,TOT_YTD_NBR_CRD_CHG                  --年累计换卡数          

				   ,TOT_YTD_NBR_AC_Opened                --年累计开户账户数      

				   ,TOT_YTD_NBR_AC_CLS                   --年累计销户账户数      

				   ,TOT_YTD_NBR_CRD_CLECTD               --年累计已收卡数        

				   -----------------------------Start of 20091203---------------------------------

					 ,NBR_CST                              --客户数

					 -----------------------------End of 20091203--------------------------------- 																										

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

					 ,NBR_DRMT_AC										      --睡眠户数量

				   -----------------------------End of 20091208---------------------------------			



				)

        SELECT 

           a.OU_ID                                --机构号

				  ,a.CRD_TP_ID                            --卡类型

				  ,a.Is_CR_CRD_F                          --是否为贷记卡

				  ,a.CRD_Brand_TP_Id                      --卡品牌类型

				  ,a.CRD_PRVL_TP_ID                       --卡级别

				  ,a.PSBK_RLTD_F                          --卡折相关标识

				  ,a.IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				  ,a.ENT_IDV_IND                          --卡对象

				  ,a.MST_CRD_IND                          --主/副卡标志

				  ,a.NGO_CRD_IND                          --协议卡类型

				  ,a.MULT_CCY_F                           --双币卡标志

				  ,a.PD_GRP_CD                            --产品类

				  ,a.PD_SUB_CD                            --产品子代码

				  ,a.BIZ_CGY_TP_ID                        --业务类别

				  ,a.CCY                                  --币种

				  ,a.ACG_DT                               --日期YYYY-MM-DD

				  ,a.CDR_YR                               --年份YYYY

				  ,a.CDR_MTH                              --月份MM

				  ,a.ACT_NBR_AC                           --实际账户数

				  ,a.NBR_EFF_CRD                          --正常卡数

				  ,a.NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				  ,a.NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				  ,a.NBR_EXP_CRD                          --过期卡数

				  ,a.NBR_DRMT_CRD                         --睡眠卡数量

				  ,a.NBR_CRD_CLECTD                       --已收卡数

				  ,a.NBR_NEW_CRD                          --新开卡数

				  ,a.NBR_CRD_CLD                          --销卡数

				  ,a.NBR_NEW_CST                          --新增客户数

				  ,a.NBR_CST_CLD                          --客户销户数

				  ,a.NBR_CRD_CHG                          --换卡数

				  ,a.NBR_AC_Opened                        --开户账户数

				  ,a.NBR_AC_CLS                           --销户账户数

				  ,a.NBR_NEW_CRD                          --月累计新开卡数                                           

				  ,a.NBR_CRD_CLD                          --月累计销卡数          

				  ,a.NBR_NEW_CST                          --月累计新增客户数      

				  ,a.NBR_CST_CLD                          --月累计客户销户数      

				  ,a.NBR_CRD_CHG                          --月累计换卡数          

				  ,a.NBR_AC_Opened                        --月累计开户账户数      

				  ,a.NBR_AC_CLS                           --月累计销户账户数      

				  ,a.NBR_CRD_CLECTD                       --月累计已收卡数        

				  ,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)    						--季累计新开卡数        																							        

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)    						--季累计销卡数          																				

          ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)    						--季累计新增客户数      																				

          ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)    						--季累计客户销户数      																				

          ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)    						--季累计换卡数          																				

          ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0)						--季累计开户账户数 

          --------------------------Start on 20100114----------------------------------------------------     																				

          --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)      						--季累计销户账户数      																				

          --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)         --季累计已收卡数  

          ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)      						--季累计销户账户数      																				

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)         --季累计已收卡数   

          --------------------------End on 20100114----------------------------------------------------    

				  ,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     					--年累计新开卡数        																								        

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     						--年累计销卡数          																				

          ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     						--年累计新增客户数      																				

          ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     						--年累计客户销户数      																				

          ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     						--年累计换卡数          																				

          ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 						--年累计开户账户数   

          --------------------------Start on 20100114----------------------------------------------------   																				

          --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       						--年累计销户账户数      																				

          --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)         --年累计已收卡数 

          ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       						--年累计销户账户数      																				

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)         --年累计已收卡数     

          --------------------------End on 20100114----------------------------------------------------   

          -----------------------------Start of 20091203---------------------------------

					,a.NBR_CST                              --客户数

					-----------------------------End of 20091203--------------------------------- 																										

					-----------------------------Start of 20091208---------------------------------

					,a.NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

					,a.NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

					,a.NBR_DRMT_AC										      --睡眠户数量

				  -----------------------------End of 20091208---------------------------------								

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT   ;               				  				  				      	--

   END IF;--

ELSE  --非月初

	

  INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

    (OU_ID                               --机构号

	   ,CRD_TP_ID                            --卡类型

	   ,Is_CR_CRD_F                          --是否为贷记卡

	   ,CRD_Brand_TP_Id                      --卡品牌类型

	   ,CRD_PRVL_TP_ID                       --卡级别

	   ,PSBK_RLTD_F                          --卡折相关标识

	   ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

	   ,ENT_IDV_IND                          --卡对象

	   ,MST_CRD_IND                          --主/副卡标志

	   ,NGO_CRD_IND                          --协议卡类型

	   ,MULT_CCY_F                           --双币卡标志

	   ,PD_GRP_CD                            --产品类

	   ,PD_SUB_CD                            --产品子代码

	   ,BIZ_CGY_TP_ID                        --业务类别

	   ,CCY                                  --币种

	   ,ACG_DT                               --日期YYYY-MM-DD

	   ,CDR_YR                               --年份YYYY

	   ,CDR_MTH                              --月份MM

	   ,ACT_NBR_AC                           --实际账户数

	   ,NBR_EFF_CRD                          --正常卡数

	   ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

	   ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

	   ,NBR_EXP_CRD                          --过期卡数

	   ,NBR_DRMT_CRD                         --睡眠卡数量

	   ,NBR_CRD_CLECTD                       --已收卡数

	   ,NBR_NEW_CRD                          --新开卡数

	   ,NBR_CRD_CLD                          --销卡数

	   ,NBR_NEW_CST                          --新增客户数

	   ,NBR_CST_CLD                          --客户销户数

	   ,NBR_CRD_CHG                          --换卡数

	   ,NBR_AC_Opened                        --开户账户数

	   ,NBR_AC_CLS                           --销户账户数

	   ,TOT_MTD_NBR_NEW_CRD                  --月累计新开卡数        

	   ,TOT_MTD_NBR_CRD_CLD                  --月累计销卡数          

	   ,TOT_MTD_NBR_NEW_CST                  --月累计新增客户数      

	   ,TOT_MTD_NBR_CST_CLD                  --月累计客户销户数      

	   ,TOT_MTD_NBR_CRD_CHG                  --月累计换卡数          

	   ,TOT_MTD_NBR_AC_Opened                --月累计开户账户数      

	   ,TOT_MTD_NBR_AC_CLS                   --月累计销户账户数      

	   ,TOT_MTD_NBR_CRD_CLECTD               --月累计已收卡数        

	   ,TOT_QTD_NBR_NEW_CRD                  --季累计新开卡数        

	   ,TOT_QTD_NBR_CRD_CLD                  --季累计销卡数          

	   ,TOT_QTD_NBR_NEW_CST                  --季累计新增客户数      

	   ,TOT_QTD_NBR_CST_CLD                  --季累计客户销户数      

	   ,TOT_QTD_NBR_CRD_CHG                  --季累计换卡数          

	   ,TOT_QTD_NBR_AC_Opened                --季累计开户账户数      

	   ,TOT_QTD_NBR_AC_CLS                   --季累计销户账户数      

	   ,TOT_QTD_NBR_CRD_CLECTD               --季累计已收卡数        

	   ,TOT_YTD_NBR_NEW_CRD                  --年累计新开卡数        

	   ,TOT_YTD_NBR_CRD_CLD                  --年累计销卡数          

	   ,TOT_YTD_NBR_NEW_CST                  --年累计新增客户数      

	   ,TOT_YTD_NBR_CST_CLD                  --年累计客户销户数      

	   ,TOT_YTD_NBR_CRD_CHG                  --年累计换卡数          

	   ,TOT_YTD_NBR_AC_Opened                --年累计开户账户数      

	   ,TOT_YTD_NBR_AC_CLS                   --年累计销户账户数      

	   ,TOT_YTD_NBR_CRD_CLECTD               --年累计已收卡数        

	  -----------------------------Start of 20091203---------------------------------

		 ,NBR_CST                              --客户数

		-----------------------------Start of 20091203--------------------------------- 																										

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		 ,NBR_DRMT_AC										      --睡眠户数量

	   -----------------------------End of 20091208---------------------------------			

		

	)

  SELECT 

     a.OU_ID                                --机构号

	  ,a.CRD_TP_ID                            --卡类型

	  ,a.Is_CR_CRD_F                          --是否为贷记卡

	  ,a.CRD_Brand_TP_Id                      --卡品牌类型

	  ,a.CRD_PRVL_TP_ID                       --卡级别

	  ,a.PSBK_RLTD_F                          --卡折相关标识

	  ,a.IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

	  ,a.ENT_IDV_IND                          --卡对象

	  ,a.MST_CRD_IND                          --主/副卡标志

	  ,a.NGO_CRD_IND                          --协议卡类型

	  ,a.MULT_CCY_F                           --双币卡标志

	  ,a.PD_GRP_CD                            --产品类

	  ,a.PD_SUB_CD                            --产品子代码

	  ,a.BIZ_CGY_TP_ID                        --业务类别

	  ,a.CCY                                  --币种

	  ,a.ACG_DT                               --日期YYYY-MM-DD

	  ,a.CDR_YR                               --年份YYYY

	  ,a.CDR_MTH                              --月份MM

	  ,a.ACT_NBR_AC                           --实际账户数

	  ,a.NBR_EFF_CRD                          --正常卡数

	  ,a.NBR_UNATVD_CR_CRD                    --未启用信用卡数量

	  ,a.NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

	  ,a.NBR_EXP_CRD                          --过期卡数

	  ,a.NBR_DRMT_CRD                         --睡眠卡数量

	  ,a.NBR_CRD_CLECTD                       --已收卡数

	  ,a.NBR_NEW_CRD                          --新开卡数

	  ,a.NBR_CRD_CLD                          --销卡数

	  ,a.NBR_NEW_CST                          --新增客户数

	  ,a.NBR_CST_CLD                          --客户销户数

	  ,a.NBR_CRD_CHG                          --换卡数

	  ,a.NBR_AC_Opened                        --开户账户数

	  ,a.NBR_AC_CLS                           --销户账户数

		,COALESCE(b.TOT_MTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--月累计新开卡数        																												        

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--月累计销卡数          																								

    ,COALESCE(b.TOT_MTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--月累计新增客户数      																								

    ,COALESCE(b.TOT_MTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--月累计客户销户数      																								

    ,COALESCE(b.TOT_MTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--月累计换卡数          																								

    ,COALESCE(b.TOT_MTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--月累计开户账户数 

    --------------------------Start on 20100114----------------------------------------------------      																								

    --,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--月累计销户账户数      																								

    --,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --月累计已收卡数  

    ,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--月累计销户账户数      																								

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --月累计已收卡数 

    --------------------------End on 20100114----------------------------------------------------        

		,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--季累计新开卡数        																												        

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--季累计销卡数          																								

    ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--季累计新增客户数      																								

    ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--季累计客户销户数      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--季累计换卡数          																								

    ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--季累计开户账户数    

    --------------------------Start on 20100114----------------------------------------------------    																								

    --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--季累计销户账户数      																								

    --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --季累计已收卡数  

    ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--季累计销户账户数      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --季累计已收卡数   

    --------------------------End on 20100114----------------------------------------------------       

		,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--年累计新开卡数        																												        

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--年累计销卡数          																								

    ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--年累计新增客户数      																								

    ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--年累计客户销户数      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--年累计换卡数          																								

    ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--年累计开户账户数 

    --------------------------Start on 20100114----------------------------------------------------       																								

    --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--年累计销户账户数      																								

    --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --年累计已收卡数 

    ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--年累计销户账户数      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --年累计已收卡数  

    --------------------------End on 20100114----------------------------------------------------        

    -----------------------------Start of 20091203---------------------------------

		,a.NBR_CST                              --客户数

		-----------------------------Start of 20091203--------------------------------- 																										

		 -----------------------------Start of 20091208---------------------------------

		 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		 ,a.NBR_DRMT_AC										      --睡眠户数量

	   -----------------------------End of 20091208---------------------------------					

  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

  ON a.OU_ID = b.OU_ID AND

     a.CRD_TP_ID = b.CRD_TP_ID AND

  	 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

  	 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

  	 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

  	 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

  	 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

  	 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

  	 a.MST_CRD_IND      = b.MST_CRD_IND AND

  	 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

  	 a.MULT_CCY_F       = b.MULT_CCY_F AND

  	 a.PD_GRP_CD        = b.PD_GRP_CD AND

  	 a.PD_SUB_CD        = b.PD_SUB_CD AND

  	 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

  	 a.CCY              = b.CCY AND

  	 a.ACG_DT - 1 day   = b.ACG_DT   ; 	--

	   

END IF  ;            --



GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

   

SET SMY_STEPDESC = '插入借记卡数据数据';--



IF CUR_DAY = 1 THEN  

   IF CUR_MONTH = 1 THEN --年初

      INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

        ( OU_ID                               --机构号

				 ,CRD_TP_ID                            --卡类型

				 ,Is_CR_CRD_F                          --是否为贷记卡

				 ,CRD_Brand_TP_Id                      --卡品牌类型

				 ,CRD_PRVL_TP_ID                       --卡级别

				 ,PSBK_RLTD_F                          --卡折相关标识

				 ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				 ,ENT_IDV_IND                          --卡对象

				 ,MST_CRD_IND                          --主/副卡标志

				 ,NGO_CRD_IND                          --协议卡类型

				 ,MULT_CCY_F                           --双币卡标志

				 ,PD_GRP_CD                            --产品类

				 ,PD_SUB_CD                            --产品子代码

				 ,BIZ_CGY_TP_ID                        --业务类别

				 ,CCY                                  --币种

				 ,ACG_DT                               --日期YYYY-MM-DD

				 ,CDR_YR                               --年份YYYY

				 ,CDR_MTH                              --月份MM

				 ,ACT_NBR_AC                           --实际账户数

				 ,NBR_EFF_CRD                          --正常卡数

				 ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				 ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				 ,NBR_EXP_CRD                          --过期卡数

				 ,NBR_DRMT_CRD                         --睡眠卡数量

				 ,NBR_CRD_CLECTD                       --已收卡数

				 ,NBR_NEW_CRD                          --新开卡数

				 ,NBR_CRD_CLD                          --销卡数

				 ,NBR_NEW_CST                          --新增客户数

				 ,NBR_CST_CLD                          --客户销户数

				 ,NBR_CRD_CHG                          --换卡数

				 ,NBR_AC_Opened                        --开户账户数

				 ,NBR_AC_CLS                           --销户账户数

				 ,TOT_MTD_NBR_NEW_CRD                  --月累计新开卡数        

				 ,TOT_MTD_NBR_CRD_CLD                  --月累计销卡数          

				 ,TOT_MTD_NBR_NEW_CST                  --月累计新增客户数      

				 ,TOT_MTD_NBR_CST_CLD                  --月累计客户销户数      

				 ,TOT_MTD_NBR_CRD_CHG                  --月累计换卡数          

				 ,TOT_MTD_NBR_AC_Opened                --月累计开户账户数      

				 ,TOT_MTD_NBR_AC_CLS                   --月累计销户账户数      

				 ,TOT_MTD_NBR_CRD_CLECTD               --月累计已收卡数        

				 ,TOT_QTD_NBR_NEW_CRD                  --季累计新开卡数        

				 ,TOT_QTD_NBR_CRD_CLD                  --季累计销卡数          

				 ,TOT_QTD_NBR_NEW_CST                  --季累计新增客户数      

				 ,TOT_QTD_NBR_CST_CLD                  --季累计客户销户数      

				 ,TOT_QTD_NBR_CRD_CHG                  --季累计换卡数          

				 ,TOT_QTD_NBR_AC_Opened                --季累计开户账户数      

				 ,TOT_QTD_NBR_AC_CLS                   --季累计销户账户数      

				 ,TOT_QTD_NBR_CRD_CLECTD               --季累计已收卡数        

				 ,TOT_YTD_NBR_NEW_CRD                  --年累计新开卡数        

				 ,TOT_YTD_NBR_CRD_CLD                  --年累计销卡数          

				 ,TOT_YTD_NBR_NEW_CST                  --年累计新增客户数      

				 ,TOT_YTD_NBR_CST_CLD                  --年累计客户销户数      

				 ,TOT_YTD_NBR_CRD_CHG                  --年累计换卡数          

				 ,TOT_YTD_NBR_AC_Opened                --年累计开户账户数      

				 ,TOT_YTD_NBR_AC_CLS                   --年累计销户账户数      

				 ,TOT_YTD_NBR_CRD_CLECTD               --年累计已收卡数        

				-----------------------------Start of 20091203---------------------------------

			 	 ,NBR_CST                              --客户数

				-----------------------------End of 20091203--------------------------------- 																														

			 -----------------------------Start of 20091208---------------------------------

			 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

			 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

			 ,NBR_DRMT_AC										      --睡眠户数量

		   -----------------------------End of 20091208---------------------------------							

			)

      SELECT 

        OU_ID                                --机构号

				,CRD_TP_ID                            --卡类型

				,Is_CR_CRD_F                          --是否为贷记卡

				,CRD_Brand_TP_Id                      --卡品牌类型

				,CRD_PRVL_TP_ID                       --卡级别

				,PSBK_RLTD_F                          --卡折相关标识

				,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				,ENT_IDV_IND                          --卡对象

				,MST_CRD_IND                          --主/副卡标志

				,NGO_CRD_IND                          --协议卡类型

				,MULT_CCY_F                           --双币卡标志

				,PD_GRP_CD                            --产品类

				,PD_SUB_CD                            --产品子代码

				,BIZ_CGY_TP_ID                        --业务类别

				,CCY                                  --币种

				,ACG_DT                               --日期YYYY-MM-DD

				,CDR_YR                               --年份YYYY

				,CDR_MTH                              --月份MM

				,ACT_NBR_AC                           --实际账户数

				,NBR_EFF_CRD                          --正常卡数

				,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				,NBR_EXP_CRD                          --过期卡数

				,NBR_DRMT_CRD                         --睡眠卡数量

				,NBR_CRD_CLECTD                       --已收卡数

				,NBR_NEW_CRD                          --新开卡数

				,NBR_CRD_CLD                          --销卡数

				,NBR_NEW_CST                          --新增客户数

				,NBR_CST_CLD                          --客户销户数

				,NBR_CRD_CHG                          --换卡数

				,NBR_AC_Opened                        --开户账户数

				,NBR_AC_CLS                           --销户账户数

				,NBR_NEW_CRD                          --月累计新开卡数        

				,NBR_CRD_CLD                          --月累计销卡数          

				,NBR_NEW_CST                          --月累计新增客户数      

				,NBR_CST_CLD                          --月累计客户销户数      

				,NBR_CRD_CHG                          --月累计换卡数          

				,NBR_AC_Opened                        --月累计开户账户数      

				,NBR_AC_CLS                           --月累计销户账户数      

				,NBR_CRD_CLECTD                       --月累计已收卡数        

				,NBR_NEW_CRD                          --季累计新开卡数        

				,NBR_CRD_CLD                          --季累计销卡数          

				,NBR_NEW_CST                          --季累计新增客户数      

				,NBR_CST_CLD                          --季累计客户销户数      

				,NBR_CRD_CHG                          --季累计换卡数          

				,NBR_AC_Opened                        --季累计开户账户数      

				,NBR_AC_CLS                           --季累计销户账户数      

				,NBR_CRD_CLECTD                       --季累计已收卡数        

				,NBR_NEW_CRD                          --年累计新开卡数        

				,NBR_CRD_CLD                          --年累计销卡数          

				,NBR_NEW_CST                          --年累计新增客户数      

				,NBR_CST_CLD                          --年累计客户销户数      

				,NBR_CRD_CHG                          --年累计换卡数          

				,NBR_AC_Opened                        --年累计开户账户数      

				,NBR_AC_CLS                           --年累计销户账户数      

				,NBR_CRD_CLECTD                       --年累计已收卡数        

				-----------------------------Start of 20091203---------------------------------

			 	,NBR_CST                              --客户数

				-----------------------------End of 20091203--------------------------------- 																																						

				 -----------------------------Start of 20091208---------------------------------

				 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

				 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

				 ,NBR_DRMT_AC										      --睡眠户数量

			   -----------------------------End of 20091208---------------------------------							

		  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB;--

		  

   ELSEIF CUR_MONTH IN (4, 7 ,10) THEN  --季初

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          (OU_ID                               --机构号

				  ,CRD_TP_ID                            --卡类型

				  ,Is_CR_CRD_F                          --是否为贷记卡

				  ,CRD_Brand_TP_Id                      --卡品牌类型

				  ,CRD_PRVL_TP_ID                       --卡级别

				  ,PSBK_RLTD_F                          --卡折相关标识

				  ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				  ,ENT_IDV_IND                          --卡对象

				  ,MST_CRD_IND                          --主/副卡标志

				  ,NGO_CRD_IND                          --协议卡类型

				  ,MULT_CCY_F                           --双币卡标志

				  ,PD_GRP_CD                            --产品类

				  ,PD_SUB_CD                            --产品子代码

				  ,BIZ_CGY_TP_ID                        --业务类别

				  ,CCY                                  --币种

				  ,ACG_DT                               --日期YYYY-MM-DD

				  ,CDR_YR                               --年份YYYY

				  ,CDR_MTH                              --月份MM

				  ,ACT_NBR_AC                           --实际账户数

				  ,NBR_EFF_CRD                          --正常卡数

				  ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				  ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				  ,NBR_EXP_CRD                          --过期卡数

				  ,NBR_DRMT_CRD                         --睡眠卡数量

				  ,NBR_CRD_CLECTD                       --已收卡数

				  ,NBR_NEW_CRD                          --新开卡数

				  ,NBR_CRD_CLD                          --销卡数

				  ,NBR_NEW_CST                          --新增客户数

				  ,NBR_CST_CLD                          --客户销户数

				  ,NBR_CRD_CHG                          --换卡数

				  ,NBR_AC_Opened                        --开户账户数

				  ,NBR_AC_CLS                           --销户账户数

				  ,TOT_MTD_NBR_NEW_CRD                  --月累计新开卡数        

				  ,TOT_MTD_NBR_CRD_CLD                  --月累计销卡数          

				  ,TOT_MTD_NBR_NEW_CST                  --月累计新增客户数      

				  ,TOT_MTD_NBR_CST_CLD                  --月累计客户销户数      

				  ,TOT_MTD_NBR_CRD_CHG                  --月累计换卡数          

				  ,TOT_MTD_NBR_AC_Opened                --月累计开户账户数      

				  ,TOT_MTD_NBR_AC_CLS                   --月累计销户账户数      

				  ,TOT_MTD_NBR_CRD_CLECTD               --月累计已收卡数        

				  ,TOT_QTD_NBR_NEW_CRD                  --季累计新开卡数        

				  ,TOT_QTD_NBR_CRD_CLD                  --季累计销卡数          

				  ,TOT_QTD_NBR_NEW_CST                  --季累计新增客户数      

				  ,TOT_QTD_NBR_CST_CLD                  --季累计客户销户数      

				  ,TOT_QTD_NBR_CRD_CHG                  --季累计换卡数          

				  ,TOT_QTD_NBR_AC_Opened                --季累计开户账户数      

				  ,TOT_QTD_NBR_AC_CLS                   --季累计销户账户数      

				  ,TOT_QTD_NBR_CRD_CLECTD               --季累计已收卡数        

				  ,TOT_YTD_NBR_NEW_CRD                  --年累计新开卡数        

				  ,TOT_YTD_NBR_CRD_CLD                  --年累计销卡数          

				  ,TOT_YTD_NBR_NEW_CST                  --年累计新增客户数      

				  ,TOT_YTD_NBR_CST_CLD                  --年累计客户销户数      

				  ,TOT_YTD_NBR_CRD_CHG                  --年累计换卡数          

				  ,TOT_YTD_NBR_AC_Opened                --年累计开户账户数      

				  ,TOT_YTD_NBR_AC_CLS                   --年累计销户账户数      

				  ,TOT_YTD_NBR_CRD_CLECTD               --年累计已收卡数        

				  -----------------------------Start of 20091203---------------------------------

			 		,NBR_CST                              --客户数

					-----------------------------End of 20091203--------------------------------- 																																						

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

					 ,NBR_DRMT_AC										      --睡眠户数量

				   -----------------------------End of 20091208---------------------------------			

					

				)

        SELECT 

          a.OU_ID                                --机构号

				  , a.CRD_TP_ID                            --卡类型

				  , a.Is_CR_CRD_F                          --是否为贷记卡

				  , a.CRD_Brand_TP_Id                      --卡品牌类型

				  , a.CRD_PRVL_TP_ID                       --卡级别

				  , a.PSBK_RLTD_F                          --卡折相关标识

				  , a.IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				  , a.ENT_IDV_IND                          --卡对象

				  , a.MST_CRD_IND                          --主/副卡标志

				  , a.NGO_CRD_IND                          --协议卡类型

				  , a.MULT_CCY_F                           --双币卡标志

				  , a.PD_GRP_CD                            --产品类

				  , a.PD_SUB_CD                            --产品子代码

				  , a.BIZ_CGY_TP_ID                        --业务类别

				  , a.CCY                                  --币种

				  , a.ACG_DT                               --日期YYYY-MM-DD

				  , a.CDR_YR                               --年份YYYY

				  , a.CDR_MTH                              --月份MM

				  , a.ACT_NBR_AC                           --实际账户数

				  , a.NBR_EFF_CRD                          --正常卡数

				  , a.NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				  , a.NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				  , a.NBR_EXP_CRD                          --过期卡数

				  , a.NBR_DRMT_CRD                         --睡眠卡数量

				  , a.NBR_CRD_CLECTD                       --已收卡数

				  , a.NBR_NEW_CRD                          --新开卡数

				  , a.NBR_CRD_CLD                          --销卡数

				  , a.NBR_NEW_CST                          --新增客户数

				  , a.NBR_CST_CLD                          --客户销户数

				  , a.NBR_CRD_CHG                          --换卡数

				  , a.NBR_AC_Opened                        --开户账户数

				  , a.NBR_AC_CLS                           --销户账户数

				  , a.NBR_NEW_CRD                          --月累计新开卡数         

				  , a.NBR_CRD_CLD                          --月累计销卡数          

				  , a.NBR_NEW_CST                          --月累计新增客户数      

				  , a.NBR_CST_CLD                          --月累计客户销户数      

				  , a.NBR_CRD_CHG                          --月累计换卡数          

				  , a.NBR_AC_Opened                        --月累计开户账户数      

				  , a.NBR_AC_CLS                           --月累计销户账户数      

				  , a.NBR_CRD_CLECTD                       --月累计已收卡数        

				  , a.NBR_NEW_CRD                          --季累计新开卡数        

				  , a.NBR_CRD_CLD                          --季累计销卡数          

				  , a.NBR_NEW_CST                          --季累计新增客户数      

				  , a.NBR_CST_CLD                          --季累计客户销户数      

				  , a.NBR_CRD_CHG                          --季累计换卡数          

				  , a.NBR_AC_Opened                        --季累计开户账户数      

				  , a.NBR_AC_CLS                           --季累计销户账户数      

          , a.NBR_CRD_CLECTD                       --季累计已收卡数        

				  , COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     					--年累计新开卡数        																								        

          , COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     					--年累计销卡数          																					

          , COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     					--年累计新增客户数      																					

          , COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     					--年累计客户销户数      																					

          , COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     					--年累计换卡数          																					

          , COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 					--年累计开户账户数 

          --------------------------Start on 20100114----------------------------------------------------     																					

          --, COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       					--年累计销户账户数      																					

          --, COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)         --年累计已收卡数  

          , COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       					--年累计销户账户数      																					

          , COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)         --年累计已收卡数   

          --------------------------End on 20100114----------------------------------------------------   

          -----------------------------Start of 20091203---------------------------------

			 		,a.NBR_CST                              --客户数

					-----------------------------End of 20091203---------------------------------																									

					 -----------------------------Start of 20091208---------------------------------

					 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

					 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

					 ,a.NBR_DRMT_AC										      --睡眠户数量

				   -----------------------------End of 20091208---------------------------------								

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT  ;        --

      ELSE              --月初

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          (OU_ID                               --机构号

				  ,CRD_TP_ID                            --卡类型

				  ,Is_CR_CRD_F                          --是否为贷记卡

				  ,CRD_Brand_TP_Id                      --卡品牌类型

				  ,CRD_PRVL_TP_ID                       --卡级别

				  ,PSBK_RLTD_F                          --卡折相关标识

				  ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				  ,ENT_IDV_IND                          --卡对象

				  ,MST_CRD_IND                          --主/副卡标志

				  ,NGO_CRD_IND                          --协议卡类型

				  ,MULT_CCY_F                           --双币卡标志

				  ,PD_GRP_CD                            --产品类

				  ,PD_SUB_CD                            --产品子代码

				  ,BIZ_CGY_TP_ID                        --业务类别

				  ,CCY                                  --币种

				  ,ACG_DT                               --日期YYYY-MM-DD

				  ,CDR_YR                               --年份YYYY

				  ,CDR_MTH                              --月份MM

				  ,ACT_NBR_AC                           --实际账户数

				  ,NBR_EFF_CRD                          --正常卡数

				  ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				  ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				  ,NBR_EXP_CRD                          --过期卡数

				  ,NBR_DRMT_CRD                         --睡眠卡数量

				  ,NBR_CRD_CLECTD                       --已收卡数

				  ,NBR_NEW_CRD                          --新开卡数

				  ,NBR_CRD_CLD                          --销卡数

				  ,NBR_NEW_CST                          --新增客户数

				  ,NBR_CST_CLD                          --客户销户数

				  ,NBR_CRD_CHG                          --换卡数

				  ,NBR_AC_Opened                        --开户账户数

				  ,NBR_AC_CLS                           --销户账户数

				  ,TOT_MTD_NBR_NEW_CRD                  --月累计新开卡数        

				  ,TOT_MTD_NBR_CRD_CLD                  --月累计销卡数          

				  ,TOT_MTD_NBR_NEW_CST                  --月累计新增客户数      

				  ,TOT_MTD_NBR_CST_CLD                  --月累计客户销户数      

				  ,TOT_MTD_NBR_CRD_CHG                  --月累计换卡数          

				  ,TOT_MTD_NBR_AC_Opened                --月累计开户账户数      

				  ,TOT_MTD_NBR_AC_CLS                   --月累计销户账户数      

				  ,TOT_MTD_NBR_CRD_CLECTD               --月累计已收卡数        

				  ,TOT_QTD_NBR_NEW_CRD                  --季累计新开卡数        

				  ,TOT_QTD_NBR_CRD_CLD                  --季累计销卡数          

				  ,TOT_QTD_NBR_NEW_CST                  --季累计新增客户数      

				  ,TOT_QTD_NBR_CST_CLD                  --季累计客户销户数      

				  ,TOT_QTD_NBR_CRD_CHG                  --季累计换卡数          

				  ,TOT_QTD_NBR_AC_Opened                --季累计开户账户数      

				  ,TOT_QTD_NBR_AC_CLS                   --季累计销户账户数      

				  ,TOT_QTD_NBR_CRD_CLECTD               --季累计已收卡数        

				  ,TOT_YTD_NBR_NEW_CRD                  --年累计新开卡数        

				  ,TOT_YTD_NBR_CRD_CLD                  --年累计销卡数          

				  ,TOT_YTD_NBR_NEW_CST                  --年累计新增客户数      

				  ,TOT_YTD_NBR_CST_CLD                  --年累计客户销户数      

				  ,TOT_YTD_NBR_CRD_CHG                  --年累计换卡数          

				  ,TOT_YTD_NBR_AC_Opened                --年累计开户账户数      

				  ,TOT_YTD_NBR_AC_CLS                   --年累计销户账户数      

				  ,TOT_YTD_NBR_CRD_CLECTD               --年累计已收卡数        

				  -----------------------------Start of 20091203---------------------------------

			 		,NBR_CST                              --客户数

					-----------------------------End of 20091203---------------------------------

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

					 ,NBR_DRMT_AC										      --睡眠户数量

				   -----------------------------End of 20091208---------------------------------								

				)

        SELECT 

          a.OU_ID                                 --机构号

				  ,a.CRD_TP_ID                            --卡类型

				  ,a.Is_CR_CRD_F                          --是否为贷记卡

				  ,a.CRD_Brand_TP_Id                      --卡品牌类型

				  ,a.CRD_PRVL_TP_ID                       --卡级别

				  ,a.PSBK_RLTD_F                          --卡折相关标识

				  ,a.IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

				  ,a.ENT_IDV_IND                          --卡对象

				  ,a.MST_CRD_IND                          --主/副卡标志

				  ,a.NGO_CRD_IND                          --协议卡类型

				  ,a.MULT_CCY_F                           --双币卡标志

				  ,a.PD_GRP_CD                            --产品类

				  ,a.PD_SUB_CD                            --产品子代码

				  ,a.BIZ_CGY_TP_ID                        --业务类别

				  ,a.CCY                                  --币种

				  ,a.ACG_DT                               --日期YYYY-MM-DD

				  ,a.CDR_YR                               --年份YYYY

				  ,a.CDR_MTH                              --月份MM

				  ,a.ACT_NBR_AC                           --实际账户数

				  ,a.NBR_EFF_CRD                          --正常卡数

				  ,a.NBR_UNATVD_CR_CRD                    --未启用信用卡数量

				  ,a.NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

				  ,a.NBR_EXP_CRD                          --过期卡数

				  ,a.NBR_DRMT_CRD                         --睡眠卡数量

				  ,a.NBR_CRD_CLECTD                       --已收卡数

				  ,a.NBR_NEW_CRD                          --新开卡数

				  ,a.NBR_CRD_CLD                          --销卡数

				  ,a.NBR_NEW_CST                          --新增客户数

				  ,a.NBR_CST_CLD                          --客户销户数

				  ,a.NBR_CRD_CHG                          --换卡数

				  ,a.NBR_AC_Opened                        --开户账户数

				  ,a.NBR_AC_CLS                           --销户账户数

				  ,a.NBR_NEW_CRD                          --月累计新开卡数                    

				  ,a.NBR_CRD_CLD                          --月累计销卡数          

				  ,a.NBR_NEW_CST                          --月累计新增客户数      

				  ,a.NBR_CST_CLD                          --月累计客户销户数      

				  ,a.NBR_CRD_CHG                          --月累计换卡数          

				  ,a.NBR_AC_Opened                        --月累计开户账户数      

				  ,a.NBR_AC_CLS                           --月累计销户账户数      

				  ,a.NBR_CRD_CLECTD                       --月累计已收卡数        

				  ,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     			--季累计新开卡数        																									        

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     				--季累计销卡数          																						

          ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     				--季累计新增客户数      																						

          ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     				--季累计客户销户数      																						

          ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     				--季累计换卡数          																						

          ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 				--季累计开户账户数  

          --------------------------Start on 20100114----------------------------------------------------    																						

          --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       				--季累计销户账户数      																						

          --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)      --季累计已收卡数 

          ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       				--季累计销户账户数      																						

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)      --季累计已收卡数      

          --------------------------End on 20100114----------------------------------------------------   

				  ,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     			--年累计新开卡数        																									        

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     				--年累计销卡数          																						

          ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     				--年累计新增客户数      																						

          ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     				--年累计客户销户数      																						

          ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     				--年累计换卡数          																						

          ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 				--年累计开户账户数 

          --------------------------Start on 20100114----------------------------------------------------     																						

          --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       				--年累计销户账户数      																						

          --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)       --年累计已收卡数 

          ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       				--年累计销户账户数      																						

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)       --年累计已收卡数              

          --------------------------End on 20100114----------------------------------------------------    

          -----------------------------Start of 20091203---------------------------------

			 		,a.NBR_CST                              --客户数

					-----------------------------Start of 20091203---------------------------------

					 -----------------------------Start of 20091208---------------------------------

					 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

					 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

					 ,a.NBR_DRMT_AC										      --睡眠户数量

				   -----------------------------End of 20091208---------------------------------			

					

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT   ;               				  				  				      	--

   END IF;--

ELSE  --非月初

	

  INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

    (OU_ID                                --机构号

	  ,CRD_TP_ID                            --卡类型

	  ,Is_CR_CRD_F                          --是否为贷记卡

	  ,CRD_Brand_TP_Id                      --卡品牌类型

	  ,CRD_PRVL_TP_ID                       --卡级别

	  ,PSBK_RLTD_F                          --卡折相关标识

	  ,IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

	  ,ENT_IDV_IND                          --卡对象

	  ,MST_CRD_IND                          --主/副卡标志

	  ,NGO_CRD_IND                          --协议卡类型

	  ,MULT_CCY_F                           --双币卡标志

	  ,PD_GRP_CD                            --产品类

	  ,PD_SUB_CD                            --产品子代码

	  ,BIZ_CGY_TP_ID                        --业务类别

	  ,CCY                                  --币种

	  ,ACG_DT                               --日期YYYY-MM-DD

	  ,CDR_YR                               --年份YYYY

	  ,CDR_MTH                              --月份MM

	  ,ACT_NBR_AC                           --实际账户数

	  ,NBR_EFF_CRD                          --正常卡数

	  ,NBR_UNATVD_CR_CRD                    --未启用信用卡数量

	  ,NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

	  ,NBR_EXP_CRD                          --过期卡数

	  ,NBR_DRMT_CRD                         --睡眠卡数量

	  ,NBR_CRD_CLECTD                       --已收卡数

	  ,NBR_NEW_CRD                          --新开卡数

	  ,NBR_CRD_CLD                          --销卡数

	  ,NBR_NEW_CST                          --新增客户数

	  ,NBR_CST_CLD                          --客户销户数

	  ,NBR_CRD_CHG                          --换卡数

	  ,NBR_AC_Opened                        --开户账户数

	  ,NBR_AC_CLS                           --销户账户数

	  ,TOT_MTD_NBR_NEW_CRD                  --累计新开卡数

	  ,TOT_MTD_NBR_CRD_CLD                  --累计销卡数

	  ,TOT_MTD_NBR_NEW_CST                  --累计新增客户数

	  ,TOT_MTD_NBR_CST_CLD                  --累计客户销户数

	  ,TOT_MTD_NBR_CRD_CHG                  --累计换卡数

	  ,TOT_MTD_NBR_AC_Opened                --累计开户账户数

	  ,TOT_MTD_NBR_AC_CLS                   --累计销户账户数

	  ,TOT_MTD_NBR_CRD_CLECTD               --累计已收卡数

	  ,TOT_QTD_NBR_NEW_CRD                  --累计新开卡数

	  ,TOT_QTD_NBR_CRD_CLD                  --累计销卡数

	  ,TOT_QTD_NBR_NEW_CST                  --累计新增客户数

	  ,TOT_QTD_NBR_CST_CLD                  --累计客户销户数

	  ,TOT_QTD_NBR_CRD_CHG                  --累计换卡数

	  ,TOT_QTD_NBR_AC_Opened                --累计开户账户数

	  ,TOT_QTD_NBR_AC_CLS                   --累计销户账户数

	  ,TOT_QTD_NBR_CRD_CLECTD               --累计已收卡数

	  ,TOT_YTD_NBR_NEW_CRD                  --累计新开卡数

	  ,TOT_YTD_NBR_CRD_CLD                  --累计销卡数

	  ,TOT_YTD_NBR_NEW_CST                  --累计新增客户数

	  ,TOT_YTD_NBR_CST_CLD                  --累计客户销户数

	  ,TOT_YTD_NBR_CRD_CHG                  --累计换卡数

	  ,TOT_YTD_NBR_AC_Opened                --累计开户账户数

	  ,TOT_YTD_NBR_AC_CLS                   --累计销户账户数

	  ,TOT_YTD_NBR_CRD_CLECTD               --累计已收卡数

	  -----------------------------Start of 20091203---------------------------------

 		,NBR_CST                              --客户数

		-----------------------------Start of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		 ,NBR_DRMT_AC										      --睡眠户数量

	   -----------------------------End of 20091208---------------------------------					

  )

  SELECT 

    a.OU_ID                                --机构号

	  ,a.CRD_TP_ID                            --卡类型

	  ,a.Is_CR_CRD_F                          --是否为贷记卡

	  ,a.CRD_Brand_TP_Id                      --卡品牌类型

	  ,a.CRD_PRVL_TP_ID                       --卡级别

	  ,a.PSBK_RLTD_F                          --卡折相关标识

	  ,a.IS_NONGXIN_CRD_F                     --丰收卡/农信卡标识

	  ,a.ENT_IDV_IND                          --卡对象

	  ,a.MST_CRD_IND                          --主/副卡标志

	  ,a.NGO_CRD_IND                          --协议卡类型

	  ,a.MULT_CCY_F                           --双币卡标志

	  ,a.PD_GRP_CD                            --产品类

	  ,a.PD_SUB_CD                            --产品子代码

	  ,a.BIZ_CGY_TP_ID                        --业务类别

	  ,a.CCY                                  --币种

	  ,a.ACG_DT                               --日期YYYY-MM-DD

	  ,a.CDR_YR                               --年份YYYY

	  ,a.CDR_MTH                              --月份MM

	  ,a.ACT_NBR_AC                           --实际账户数

	  ,a.NBR_EFF_CRD                          --正常卡数

	  ,a.NBR_UNATVD_CR_CRD                    --未启用信用卡数量

	  ,a.NBR_UNATVD_CHGD_CRD                  --已换卡未启用卡数

	  ,a.NBR_EXP_CRD                          --过期卡数

	  ,a.NBR_DRMT_CRD                         --睡眠卡数量

	  ,a.NBR_CRD_CLECTD                       --已收卡数

	  ,a.NBR_NEW_CRD                          --新开卡数

	  ,a.NBR_CRD_CLD                          --销卡数

	  ,a.NBR_NEW_CST                          --新增客户数

	  ,a.NBR_CST_CLD                          --客户销户数

	  ,a.NBR_CRD_CHG                          --换卡数

	  ,a.NBR_AC_Opened                        --开户账户数

	  ,a.NBR_AC_CLS                           --销户账户数

		,COALESCE(b.TOT_MTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)    	  --月累计新开卡数        																											        

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--月累计销卡数          																							

    ,COALESCE(b.TOT_MTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--月累计新增客户数      																							

    ,COALESCE(b.TOT_MTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--月累计客户销户数      																							

    ,COALESCE(b.TOT_MTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--月累计换卡数          																							

    ,COALESCE(b.TOT_MTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--月累计开户账户数  

    --------------------------Start on 20100114----------------------------------------------------    																							

    --,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--月累计销户账户数      																							

    --,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --月累计已收卡数  

    ,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--月累计销户账户数      																							

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --月累计已收卡数        

    --------------------------End on 20100114----------------------------------------------------

		,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--季累计新开卡数        																												        

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--季累计销卡数          																								

    ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--季累计新增客户数      																								

    ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--季累计客户销户数      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--季累计换卡数          																								

    ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--季累计开户账户数 

    --------------------------Start on 20100114----------------------------------------------------      																								

    --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--季累计销户账户数      																								

    --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --季累计已收卡数 

    ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--季累计销户账户数      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --季累计已收卡数 

    --------------------------End on 20100114----------------------------------------------------        

		,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--年累计新开卡数        																												        

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--年累计销卡数          																								

    ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--年累计新增客户数      																								

    ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--年累计客户销户数      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--年累计换卡数          																								

    ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--年累计开户账户数   

    --------------------------Start on 20100114----------------------------------------------------    																								

    --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--年累计销户账户数      																								

    --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --年累计已收卡数   

    ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--年累计销户账户数      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --年累计已收卡数      

    --------------------------End on 20100114----------------------------------------------------       

 	  -----------------------------Start of 20091203---------------------------------

 		,a.NBR_CST                              --客户数

		-----------------------------End of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		 ,a.NBR_DRMT_AC										      --睡眠户数量

	   -----------------------------End of 20091208---------------------------------					

  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

  ON a.OU_ID = b.OU_ID AND

     a.CRD_TP_ID = b.CRD_TP_ID AND

  	 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

  	 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

  	 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

  	 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

  	 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

  	 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

  	 a.MST_CRD_IND      = b.MST_CRD_IND AND

  	 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

  	 a.MULT_CCY_F       = b.MULT_CCY_F AND

  	 a.PD_GRP_CD        = b.PD_GRP_CD AND

  	 a.PD_SUB_CD        = b.PD_SUB_CD AND

  	 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

  	 a.CCY              = b.CCY AND

  	 a.ACG_DT - 1 day   = b.ACG_DT   ; 	--

	   

END IF  ;                           --

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

 

IF DAY(ACCOUNTING_DATE + 1 DAY ) = 1 THEN	

  SET SMY_STEPDESC = '将月底数据插入月表';--

  DELETE FROM SMY.CRD_PRFL_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--

	INSERT INTO SMY.CRD_PRFL_CRD_MTHLY_SMY

		(OU_ID

		,CRD_TP_ID

		,IS_CR_CRD_F

		,CRD_BRAND_TP_ID

		,CRD_PRVL_TP_ID

		,PSBK_RLTD_F

		,IS_NONGXIN_CRD_F

		,ENT_IDV_IND

		,MST_CRD_IND

		,NGO_CRD_IND

		,MULT_CCY_F

		,PD_GRP_CD

		,PD_SUB_CD

		,BIZ_CGY_TP_ID

		,CCY

		,CDR_YR

		,CDR_MTH

		,ACG_DT

		,ACT_NBR_AC

		,NBR_EFF_CRD

		,NBR_UNATVD_CR_CRD

		,NBR_UNATVD_CHGD_CRD

		,NBR_EXP_CRD

		,NBR_DRMT_CRD

		,NBR_CRD_CLECTD

		,NBR_NEW_CRD

		,NBR_CRD_CLD

		,NBR_NEW_CST

		,NBR_CST_CLD

		,NBR_CRD_CHG

		,NBR_AC_OPENED

		,NBR_AC_CLS

		,TOT_MTD_NBR_NEW_CRD

		,TOT_MTD_NBR_CRD_CLD

		,TOT_MTD_NBR_NEW_CST

		,TOT_MTD_NBR_CST_CLD

		,TOT_MTD_NBR_CRD_CHG

		,TOT_MTD_NBR_AC_OPENED

		,TOT_MTD_NBR_AC_CLS

		,TOT_MTD_NBR_CRD_CLECTD

		,TOT_QTD_NBR_NEW_CRD

		,TOT_QTD_NBR_CRD_CLD

		,TOT_QTD_NBR_NEW_CST

		,TOT_QTD_NBR_CST_CLD

		,TOT_QTD_NBR_CRD_CHG

		,TOT_QTD_NBR_AC_OPENED

		,TOT_QTD_NBR_AC_CLS

		,TOT_QTD_NBR_CRD_CLECTD

		,TOT_YTD_NBR_NEW_CRD

		,TOT_YTD_NBR_CRD_CLD

		,TOT_YTD_NBR_NEW_CST

		,TOT_YTD_NBR_CST_CLD

		,TOT_YTD_NBR_CRD_CHG

		,TOT_YTD_NBR_AC_OPENED

		,TOT_YTD_NBR_AC_CLS

		,TOT_YTD_NBR_CRD_CLECTD		 

	  -----------------------------Start of 20091203---------------------------------

 		,NBR_CST                              --客户数

		-----------------------------Start of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		 ,NBR_DRMT_AC										      --睡眠户数量

	   -----------------------------End of 20091208---------------------------------					

  )

	SELECT 

		 OU_ID

		,CRD_TP_ID

		,IS_CR_CRD_F

		,CRD_BRAND_TP_ID

		,CRD_PRVL_TP_ID

		,PSBK_RLTD_F

		,IS_NONGXIN_CRD_F

		,ENT_IDV_IND

		,MST_CRD_IND

		,NGO_CRD_IND

		,MULT_CCY_F

		,PD_GRP_CD

		,PD_SUB_CD

		,BIZ_CGY_TP_ID

		,CCY

		,CDR_YR

		,CDR_MTH

		,ACG_DT

		,ACT_NBR_AC

		,NBR_EFF_CRD

		,NBR_UNATVD_CR_CRD

		,NBR_UNATVD_CHGD_CRD

		,NBR_EXP_CRD

		,NBR_DRMT_CRD

		,NBR_CRD_CLECTD

		,NBR_NEW_CRD

		,NBR_CRD_CLD

		,NBR_NEW_CST

		,NBR_CST_CLD

		,NBR_CRD_CHG

		,NBR_AC_OPENED

		,NBR_AC_CLS

		,TOT_MTD_NBR_NEW_CRD

		,TOT_MTD_NBR_CRD_CLD

		,TOT_MTD_NBR_NEW_CST

		,TOT_MTD_NBR_CST_CLD

		,TOT_MTD_NBR_CRD_CHG

		,TOT_MTD_NBR_AC_OPENED

		,TOT_MTD_NBR_AC_CLS

		,TOT_MTD_NBR_CRD_CLECTD

		,TOT_QTD_NBR_NEW_CRD

		,TOT_QTD_NBR_CRD_CLD

		,TOT_QTD_NBR_NEW_CST

		,TOT_QTD_NBR_CST_CLD

		,TOT_QTD_NBR_CRD_CHG

		,TOT_QTD_NBR_AC_OPENED

		,TOT_QTD_NBR_AC_CLS

		,TOT_QTD_NBR_CRD_CLECTD

		,TOT_YTD_NBR_NEW_CRD

		,TOT_YTD_NBR_CRD_CLD

		,TOT_YTD_NBR_NEW_CST

		,TOT_YTD_NBR_CST_CLD

		,TOT_YTD_NBR_CRD_CHG

		,TOT_YTD_NBR_AC_OPENED

		,TOT_YTD_NBR_AC_CLS

		,TOT_YTD_NBR_CRD_CLECTD		

    -----------------------------Start of 20091203---------------------------------

 		,NBR_CST                              --客户数

		-----------------------------Start of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --余额小于10的睡眠卡数量

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --余额小于10的睡眠户数量

		 ,NBR_DRMT_AC										      --睡眠户数量

	   -----------------------------End of 20091208---------------------------------					

	FROM SMY.CRD_PRFL_CRD_DLY_SMY WHERE ACG_DT = SMY_DATE

;--



GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--





END IF;--



END
@