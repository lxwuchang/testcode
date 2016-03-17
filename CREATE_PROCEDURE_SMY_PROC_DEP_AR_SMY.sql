CREATE PROCEDURE SMY.PROC_DEP_AR_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_DEP_AR_SMY.sql
-- Procedure name: 			SMY.PROC_DEP_AR_SMY
-- Source Table:				SOR.DMD_DEP_SUB_AR, SOR.AC_AR, SOR.DMD_DEP_MN_AR, SMY.CST_INF
-- Target Table: 				SMY.LOAN_AR_SMY
-- Project:             ZJ RCCB EDW
-- Note                 Delete and Insert
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
-- 2009-11-05   Peng Jie     Create SP File	
-- 2009-11-25   SHANG        MODIFY SP	
-- 2009-12-03   Xu Yan       Updated 'TM_MAT_SEG_ID' for FT_DEP_AR
-- 2009-12-27   Xu Yan       Created a temporary table to handle MAT_SEG
-- 2010-03-04   Xu Yan       Filter the DEL_F = 1 
-- 2010-03-23   Xu Yan       Updated the filter condition to 'DEL_F = 0'
-- 2010-03-24   Xu Yan       Updated the special logic for '股金'
-- 2010-09-02   Wang Youbing Modify select MAT_SEG_ID from SMY.SMY_DT
-- 2010-09-03   Wang Youbing Modify select CST_NO,CST_NM from SOR.CST
-- 2011-05-29   Chen XiaoWen 将前面三步插入DEP_AR_SMY的公共关联表抽离,增加临时表存放中间数据,最后插入DEP_AR_SMY再统一关联。
-- 2011-08-05   Li ShuWen    在数据插入SMY.DEP_AR_SMY表前增加临时表TMPX，分步Join，避免不稳定情况发生
-- 2011-09-26   Li ShuWen    对表SMY.DEP_AR_SMY增加三个字段PLG_F,NGO_RT_F,OPN_BAL，并根据映射规则添加数据
-- 2011-10-31   Li Shenyu    add column INT_CLCN_EFF_DT to SMY.DEP_AR_SMY
-- 2012-02-27   Chen XiaoWen 拆分临时表TMP_RESULT、TMPX的数据，分部分直接插入目标表，以此减少中间结果。
-- 2012-03-31   Chen XiaoWen 修改为增量merge加载形式
-- 2012-06-05   Chen XiaoWen TM_MAT_SEG_ID字段置为-1、删除当日销户数据
-- 2012-07-10   Chen XiaoWen 修改股金账户的质押标志为-1
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
     /*声明异常处理使用变量*/
     DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
     DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
     DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
     DECLARE SMY_DATE DATE;                                 --临时日期变量
     DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
     DECLARE SMY_PROCNM VARCHAR(100);                        --存储过程名称
		 --DECLARE EMP_SQL VARCHAR(200); --
		 --DECLARE T_NUM INT;      --
		 ---Modified By Wang Youbing On 2010-09-02 Start----
     --DECLARE MAT_SEG_ID_1 INT DEFAULT 0;--
     --DECLARE MAT_SEG_ID_2 INT DEFAULT 0;--
     --DECLARE CNT INT DEFAULT 0;--
     ---Modified By Wang Youbing On 2010-09-02 End----
     
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
       SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
     END;--
     /*变量赋值*/
     
     ---Modified By Wang Youbing On 2010-09-02 Start----
     --SELECT MAT_SEG_ID INTO MAT_SEG_ID_1 FROM SMY.MAT_SEG WHERE LOW_VAL=-99999;--
     --SELECT MAT_SEG_ID INTO MAT_SEG_ID_2 FROM SMY.MAT_SEG WHERE MAX_VAL=99999;--
     --SELECT MAX(LOW_VAL)-1 INTO CNT FROM SMY.MAT_SEG;--
     ---Modified By Wang Youbing On 2010-09-02 End----
     
     SET SMY_PROCNM = 'PROC_DEP_AR_SMY';--
     SET SMY_DATE=ACCOUNTING_DATE;--
     
     /*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
     DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
     COMMIT;--
     INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '存储过程开始运行', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
     COMMIT;--
     
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --2
     SET SMY_STEPDESC = '缓存SOR.DMD_DEP_SUB_AR活期增量数据到临时表';
     
     DECLARE GLOBAL TEMPORARY TABLE TMP_DELTA_RESULT AS (
         SELECT
             A.DMD_DEP_AR_ID AS DEP_AR_ID    						            --存款账号               
         	  ,A.CSH_RMIT_IND_TP_ID	AS CSH_RMIT_IND_TP_ID					    --钞汇属性             
         	  ,A.DNMN_CCY_ID AS DNMN_CCY_ID      						          --币种                 
         	  ,COALESCE(B.ENT_IDV_IND     ,-1) AS ENT_IDV_IND         --企业、个人标志       
         	  ,COALESCE(B.STMT_DLV_TP_ID  ,-1) AS STMT_DLV_TP_ID      --发账户对账单标志     
         	  ,COALESCE(B.DEP_PSBK_TP_ID  ,-1) AS DEP_PSBK_TP_ID  		--存单折类型           
         	  ,COALESCE(B.DMD_DEP_AC_TP_ID, -1) AS DEP_AC_TP_ID       --账户类型             
         	  ,COALESCE(B.BIZ_TP_ID       ,-1) AS BIZ_TP_ID           --业务类别码           
         	  ,COALESCE(B.MG_TP_ID        ,-1) AS MG_TP_ID            --保证金种类           
         	  ,B.AR_NM AS AR_NM            						                --账户名称             
         	  ,COALESCE(B.AC_CHRCTR_TP_ID, -1) AS AC_CHRCTR_TP_ID     --账户性质             
         	  ,A.RPRG_OU_IP_ID AS RPRG_OU_IP_ID    						        --归属机构             
         	  ,B.CR_STA_SEQ_NO AS CR_STA_SEQ_NO    						        --信用站编号           
         	  ,A.ACG_SBJ_ID AS ACG_SBJ_ID       						          --科目号               
         	  ,A.OD_ACG_SBJ_ID AS OD_ACG_SBJ_ID    						        --透支科目号           
         	  ,A.AR_LCS_TP_ID AS AR_LCS_TP_ID     						        --账户生命周期         
         	  ,B.FX_CST_TP_ID AS FX_CST_TP_ID     						        --外汇客户类别         
         	  ,B.PD_GRP_CODE AS PD_GRP_CODE      						          --产品类别码           
         	  ,B.PD_SUB_CODE AS PD_SUB_CODE      						          --产品子代码           
         	  ,B.CRD_ASOCT_AC_F AS CRD_ASOCT_AC_F   						      --卡关联标志         
         	  ,A.PLG_F AS PLG_F 						    						          --质押标志 
         	  ,B.NGO_RT_F AS NGO_RT_F 						   						      --协定存款标志
         	  ,cast(0 as decimal(17,2)) AS OPN_BAL		 			 	 				--开户金额   
         	  ,A.INT_CLCN_EFF_DT AS INT_CLCN_EFF_DT                   --起息日期
         	  ,0 AS DEP_PRD_NOM                						            --定期存款存期         
         	  ,0 AS NTC_PRD_NOD                						            --提前通知存款通知天数 
         	  ,'1899-12-31' AS MAT_DT     						                --到期日期             
         	  ,B.PBC_APV_NO AS PBC_APV_NO       						          --央行开户核准书编号   
         	  ,A.BAL_AMT AS BAL_AMT          						              --账户余额             
         	  ,A.CRED_OD_LMT AS CRED_OD_LMT      						          --透支额度             
         	  ,A.FRZ_AMT AS FRZ_AMT          						              --冻结金额             
         	  ,A.RES_TRD_AMT AS RES_TRD_AMT      						          --账户当前保留金额     
         	  ,A.SUS_PAY_AMT AS SUS_PAY_AMT      						          --止付金额             
         	  ,A.EFF_DT AS EFF_DT           						              --开户日期             
         	  ,B.END_DT AS END_DT           						              --销户日期             
         	  ,B.PRIM_CST_ID AS CST_ID      						              --客户内码         
         	  ,A.STL_DT AS SETL_DT                                    --结清日期             
         	  ,-1 AS TM_MAT_SEG_ID                                    --期限
         	  ,A.LAST_ETL_ACG_DT AS ACG_DT
         FROM SOR.DMD_DEP_SUB_AR A LEFT JOIN SOR.DMD_DEP_MN_AR B ON A.DMD_DEP_AR_ID =B.DMD_DEP_AR_ID 
         where A.LAST_ETL_ACG_DT='2012-03-31' and A.DEL_F = 0
     ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(DEP_AR_ID);
     
     CREATE INDEX SESSION.TMP_DELTA_DEP_AR_ID ON SESSION.TMP_DELTA_RESULT(DEP_AR_ID);
     CREATE INDEX SESSION.TMP_DELTA_CST_ID ON SESSION.TMP_DELTA_RESULT(CST_ID);
     
     INSERT INTO SESSION.TMP_DELTA_RESULT
     SELECT 
         A.DMD_DEP_AR_ID     						       --存款账号               
     	  ,A.CSH_RMIT_IND_TP_ID						       --钞汇属性             
     	  ,A.DNMN_CCY_ID       						       --币种                 
     	  ,COALESCE(B.ENT_IDV_IND     ,-1)       --企业、个人标志       
     	  ,COALESCE(B.STMT_DLV_TP_ID  ,-1)       --发账户对账单标志     
     	  ,COALESCE(B.DEP_PSBK_TP_ID  ,-1)   		 --存单折类型           
     	  ,COALESCE(B.DMD_DEP_AC_TP_ID, -1)      --账户类型             
     	  ,COALESCE(B.BIZ_TP_ID       ,-1)       --业务类别码           
     	  ,COALESCE(B.MG_TP_ID        ,-1)       --保证金种类           
     	  ,B.AR_NM             						       --账户名称             
     	  ,COALESCE(B.AC_CHRCTR_TP_ID, -1)       --账户性质             
     	  ,A.RPRG_OU_IP_ID     						       --归属机构             
     	  ,B.CR_STA_SEQ_NO     						       --信用站编号           
     	  ,A.ACG_SBJ_ID        						       --科目号               
     	  ,A.OD_ACG_SBJ_ID     						       --透支科目号           
     	  ,A.AR_LCS_TP_ID      						       --账户生命周期         
     	  ,B.FX_CST_TP_ID      						       --外汇客户类别         
     	  ,B.PD_GRP_CODE       						       --产品类别码           
     	  ,B.PD_SUB_CODE       						       --产品子代码           
     	  ,B.CRD_ASOCT_AC_F    						       --卡关联标志         
     	  ,A.PLG_F  						    						 --质押标志 
     	  ,B.NGO_RT_F  						   						 --协定存款标志
     	  ,0			 			 	 											 --开户金额   
     	  ,A.INT_CLCN_EFF_DT                     --起息日期 -- add by Li Shenyu at 20111031
     	  ,0                 						         --定期存款存期         
     	  ,0                 						         --提前通知存款通知天数 
     	  ,'1899-12-31'      						         --到期日期             
     	  ,B.PBC_APV_NO        						       --央行开户核准书编号   
     	  ,A.BAL_AMT           						       --账户余额             
     	  ,A.CRED_OD_LMT       						       --透支额度             
     	  ,A.FRZ_AMT           						       --冻结金额             
     	  ,A.RES_TRD_AMT       						       --账户当前保留金额     
     	  ,A.SUS_PAY_AMT       						       --止付金额             
     	  ,A.EFF_DT            						       --开户日期             
     	  ,B.END_DT            						       --销户日期             
     	  ,B.PRIM_CST_ID       						       --客户内码         
     	  ,A.STL_DT                              --结清日期             
     	  ,-1                                    --期限
     	  ,SMY_DATE
     FROM SOR.DMD_DEP_SUB_AR A LEFT JOIN SOR.DMD_DEP_MN_AR B ON A.DMD_DEP_AR_ID =B.DMD_DEP_AR_ID 
     where A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F = 0;
     
     /** 收集操作信息 */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --3
     SET SMY_STEPDESC = '缓存SOR.FT_DEP_AR定期增量数据到临时表';
     
     INSERT INTO SESSION.TMP_DELTA_RESULT
     SELECT
          A.FT_DEP_AR_ID           --存款账号            
     	   ,A.CSH_RMIT_IND_TP_ID     --钞汇属性            
     	   ,A.DNMN_CCY_ID            --币种
     	   ,A.ENT_IDV_IND            --企业、个人标志      
     	   ,A.STMT_DLV_TP_ID         --发账户对账单标志    
     	   ,A.DEP_PSBK_TP_ID         --存单折类型          
     	   ,A.FT_DEP_AC_TP_ID        --账户类型            
     	   ,A.BIZ_TP_ID              --业务类别码          
     	   ,A.MG_TP_ID               --保证金种类          
     	   ,A.AR_NM                  --账户名称            
     	   ,-1                       --账户性质            
     	   ,A.RPRG_OU_IP_ID          --归属机构            
     	   ,A.CR_STA_SEQ_NO          --信用站编号          
     	   ,A.ACG_SBJ_ID             --科目号              
     	   ,''                       --透支科目号          
     	   ,A.AR_LCS_TP_ID           --账户生命周期        
     	   ,A.FX_CST_TP_ID           --外汇客户类别        
     	   ,A.PD_GRP_CODE            --产品类别码          
     	   ,A.PD_SUB_CODE            --产品子代码          
     	   ,0                        --卡关联标志
     	   ,A.PLG_F   						   --质押标志
				 ,A.NGO_RT_F					   	 --协定存款标志
     	   ,A.OPN_BAL  						   --开户金额           
     	   ,A.INT_CLCN_EFF_DT        --起息日期 -- add by Li Shenyu at 20111031
     	   ,A.DEP_PRD_NOM            --定期存款存期        
     	   ,A.NTC_PRD_NOD            --提前通知存款通知天数
     	   ,A.NTNL_MAT_DT            --到期日期            
     	   ,A.PBC_APV_NO             --央行开户核准书编号  
     	   ,A.BAL_AMT                --账户余额            
     	   ,0                        --透支额度            
     	   ,A.FRZ_AMT                --冻结金额            
     	   ,A.RES_TRD_AMT            --账户当前保留金额    
     	   ,A.SUS_PAY_AMT            --止付金额            
     	   ,A.EFF_DT                 --开户日期            
     	   ,A.END_DT                 --销户日期            
     	   ,A.PRIM_CST_ID            --客户内码            
     	   ,A.END_DT                 --结清日期            
         ,-1
         ,SMY_DATE
     FROM SOR.FT_DEP_AR A where A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F = 0;
     
     /** 收集操作信息 */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --4
     SET SMY_STEPDESC = '缓存SOR.INTRBNK_DEP_SUB_AR同业增量数据到临时表';
     
     INSERT INTO SESSION.TMP_DELTA_RESULT
     SELECT
          A.INTRBNK_DEP_AR_ID
     	   ,A.CSH_RMIT_IND_TP_ID
     	   ,A.DNMN_CCY_ID
     	   ,B.ENT_IDV_IND
     	   ,COALESCE(B.STMT_DLV_TP_ID, -1)
     	   ,B.DEP_PSBK_TP_ID
     	   ,COALESCE(A.INTRBNK_DEP_AC_TP_ID, -1)
     	   ,COALESCE(A.BIZ_TP_ID ,-1)
     	   ,-1
     	   ,B.AR_NM
     	   ,-1
     	   ,A.RPRG_OU_IP_ID
     	   ,B.CR_STA_SEQ_NO
     	   ,A.ACG_SBJ_ID
     	   ,''
     	   ,A.AR_LCS_TP_ID
     	   ,-1
     	   ,A.PD_GRP_CODE
     	   ,A.PD_SUB_CODE
     	   ,0
     	   ,A.PLG_F   						   --质押标志
				 ,B.NGO_RT_F					   	 --协定存款标志
     	   ,0				  						   --开户金额       
     	   ,A.INT_CLCN_EFF_DT        --起息日期 -- add by Li Shenyu at 20111031
     	   ,A.DEP_PRD_NOM
     	   ,0
     	   ,A.NTNL_MAT_DT
     	   ,B.PBC_APV_NO
     	   ,A.BAL_AMT
     	   ,0
     	   ,A.FRZ_AMT
     	   ,0
     	   ,0
     	   ,B.EFF_DT
     	   ,B.END_DT
     	   ,B.PRIM_CST_ID
     	   ,A.STL_DT
         ,-1
         ,SMY_DATE
     FROM SOR.INTRBNK_DEP_SUB_AR A LEFT JOIN SOR.INTRBNK_DEP_AR B ON A.INTRBNK_DEP_AR_ID = B.INTRBNK_DEP_AR_ID 
     where A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F = 0;
     
     /** 收集操作信息 */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --5
     SET SMY_STEPDESC = '缓存中间数据到临时表';
     
     DECLARE GLOBAL TEMPORARY TABLE TMP_DEP_AR_SMY LIKE SMY.DEP_AR_SMY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(DEP_AR_ID);
     CREATE INDEX SESSION.TMP_DEP_AR_SMY_IDX ON SESSION.TMP_DEP_AR_SMY(DEP_AR_ID,CSH_RMIT_IND_TP_ID,DNMN_CCY_ID);
         
     INSERT INTO SESSION.TMP_DEP_AR_SMY
     (
          DEP_AR_ID                   --存款账号
         ,CSH_RMIT_IND_TP_ID          --钞汇属性
         ,DNMN_CCY_ID                 --币种
         ,DEP_TP_ID                   --定期/活期账户标识
         ,ENT_IDV_IND                 --企业、个人标志
         ,STMT_DLV_TP_ID              --发账户对账单标志
         ,DEP_PSBK_TP_Id              --存单折类型
         ,DEP_AC_TP_ID                --账户类型
         ,BIZ_TP_ID                   --业务类别码
         ,MG_TP_ID                    --保证金种类
         ,AR_NM                       --账户名称
         ,AC_CHRCTR_TP_ID             --账户性质
         ,RPRG_OU_IP_ID               --归属机构
         ,CR_STA_SEQ_NO               --信用站编号
         ,ACG_SBJ_ID                  --科目号
         ,OD_ACG_SBJ_ID               --透支科目号
         ,AR_LCS_TP_ID                --账户生命周期
         ,FX_CST_TP_ID                --外汇客户类别
         ,PD_GRP_CODE                 --产品类别码
         ,PD_SUB_CODE                 --产品子代码
         ,CRD_ASOCT_AC_F              --卡关联标志
         ,PLG_F   						    		--质押标志 
     	   ,NGO_RT_F   						   		--协定存款标志
     	   ,OPN_BAL   						    	--开户金额 
     	   ,INT_CLCN_EFF_DT             --起息日期 -- add by Li Shenyu at 20111031
         ,DEP_PRD_NOM                 --定期存款存期
         ,NTC_PRD_NOD                 --提前通知存款通知天数
         ,MAT_DT                      --到期日期
         ,PBC_APV_NO                  --央行开户核准书编号
         ,BAL_AMT                     --账户余额
         ,CRED_OD_LMT                 --透支额度
         ,FRZ_AMT                     --冻结金额
         ,RES_TRD_AMT                 --账户当前保留金额
         ,SUS_PAY_AMT                 --止付金额
         ,EFF_DT                      --开户日期
         ,END_DT                      --销户日期
         ,CST_ID                      --客户内码
         ,CST_NO                      --客户号
         ,CST_NM                      --客户名称
         ,SETL_DT                     --结清日期
         ,TM_MAT_SEG_ID
         ,ACG_DT
     )
     SELECT 
         A.DEP_AR_ID     						           --存款账号               
     	  ,A.CSH_RMIT_IND_TP_ID						       --钞汇属性             
     	  ,A.DNMN_CCY_ID       						       --币种                 
     	  ,COALESCE(D.AC_AR_TP_ID  ,-1)					 --定期/活期账户标识    
     	  ,A.ENT_IDV_IND       						       --企业、个人标志       
     	  ,A.STMT_DLV_TP_ID                      --发账户对账单标志     
     	  ,A.DEP_PSBK_TP_ID   						       --存单折类型           
     	  ,A.DEP_AC_TP_ID                        --账户类型             
     	  ,A.BIZ_TP_ID                           --业务类别码           
     	  ,A.MG_TP_ID  						               --保证金种类           
     	  ,A.AR_NM             						       --账户名称             
     	  ,A.AC_CHRCTR_TP_ID                     --账户性质             
     	  ,A.RPRG_OU_IP_ID     						       --归属机构             
     	  ,A.CR_STA_SEQ_NO     						       --信用站编号           
     	  ,A.ACG_SBJ_ID        						       --科目号               
     	  ,A.OD_ACG_SBJ_ID     						       --透支科目号           
     	  ,A.AR_LCS_TP_ID      						       --账户生命周期         
     	  ,A.FX_CST_TP_ID      						       --外汇客户类别         
     	  ,A.PD_GRP_CODE       						       --产品类别码           
     	  ,A.PD_SUB_CODE       						       --产品子代码           
     	  ,A.CRD_ASOCT_AC_F    						       --卡关联标志
     	  ,A.PLG_F   						    						 --质押标志 
     	  ,A.NGO_RT_F  						   						 --协定存款标志
     	  ,A.OPN_BAL  						    					 --开户金额                
     	  ,A.INT_CLCN_EFF_DT                     --起息日期 -- add by Li Shenyu at 20111031
     	  ,A.DEP_PRD_NOM                 				 --定期存款存期         
     	  ,A.NTC_PRD_NOD                 				 --提前通知存款通知天数 
     	  ,A.MAT_DT      						             --到期日期             
     	  ,A.PBC_APV_NO        						       --央行开户核准书编号   
     	  ,A.BAL_AMT           						       --账户余额             
     	  ,A.CRED_OD_LMT       						       --透支额度             
     	  ,A.FRZ_AMT           						       --冻结金额             
     	  ,A.RES_TRD_AMT       						       --账户当前保留金额     
     	  ,A.SUS_PAY_AMT       						       --止付金额             
     	  ,A.EFF_DT            						       --开户日期             
     	  ,A.END_DT            						       --销户日期             
     	  ,A.CST_ID       						           --客户内码
     	  ,C.CST_NO            						       --客户号               
     	  ,C.CST_FUL_NM            						   --客户名称
     	  ,A.SETL_DT                             --结清日期             
     	  ,A.TM_MAT_SEG_ID                       --期限
     	  ,A.ACG_DT
     FROM SESSION.TMP_DELTA_RESULT A LEFT JOIN SOR.AC_AR D ON A.DEP_AR_ID = D.AC_AR_ID
     LEFT JOIN SOR.CST C ON A.CST_ID = C.CST_ID;
     
     /** 收集操作信息 */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --7
     SET SMY_STEPDESC = '从SOR.EQTY_AC_SUB_AR插入股金数据';
     
     INSERT INTO SESSION.TMP_DEP_AR_SMY
     (    DEP_AR_ID                   --存款账号
         ,CSH_RMIT_IND_TP_ID          --钞汇属性
         ,DNMN_CCY_ID                 --币种
         ,DEP_TP_ID                   --定期/活期账户标识
         ,ENT_IDV_IND                 --企业、个人标志
         ,STMT_DLV_TP_ID              --发账户对账单标志
         ,DEP_PSBK_TP_Id              --存单折类型
         ,DEP_AC_TP_ID                --账户类型
         ,BIZ_TP_ID                   --业务类别码
         ,MG_TP_ID                    --保证金种类
         ,AR_NM                       --账户名称
         ,AC_CHRCTR_TP_ID             --账户性质
         ,RPRG_OU_IP_ID               --归属机构
         ,CR_STA_SEQ_NO               --信用站编号
         ,ACG_SBJ_ID                  --科目号
         ,OD_ACG_SBJ_ID               --透支科目号
         ,AR_LCS_TP_ID                --账户生命周期
         ,FX_CST_TP_ID                --外汇客户类别
         ,PD_GRP_CODE                 --产品类别码
         ,PD_SUB_CODE                 --产品子代码
         ,CRD_ASOCT_AC_F              --卡关联标志
         ,PLG_F   						    		--质押标志 
     	   ,NGO_RT_F   						   		--协定存款标志
     	   ,OPN_BAL   						    	--开户金额 
     	   ,INT_CLCN_EFF_DT             --起息日期 -- add by Li Shenyu at 20111031
         ,DEP_PRD_NOM                 --定期存款存期
         ,NTC_PRD_NOD                 --提前通知存款通知天数
         ,MAT_DT                      --到期日期
         ,PBC_APV_NO                  --央行开户核准书编号
         ,BAL_AMT                     --账户余额
         ,CRED_OD_LMT                 --透支额度
         ,FRZ_AMT                     --冻结金额
         ,RES_TRD_AMT                 --账户当前保留金额
         ,SUS_PAY_AMT                 --止付金额
         ,EFF_DT                      --开户日期
         ,END_DT                      --销户日期
         ,CST_ID                      --客户内码
         ,CST_NO                      --客户号
         ,CST_NM                      --客户名称
         ,SETL_DT                     --结清日期
         ,TM_MAT_SEG_ID
         ,ACG_DT
         )
     SELECT 
          A.EQTY_AC_AR_ID                 --存款账号             
         ,A.CSH_RMIT_IND_TP_ID            --钞汇属性             
         ,A.DNMN_CCY_ID                   --币种                 
         ,COALESCE(B.AC_AR_TP_ID ,-1)     --定期/活期账户标识    
         ,COALESCE(C.ENT_IDV_IND ,-1)     --企业、个人标志       
         ,-1                              --发账户对账单标志     
         ,-1                              --存单折类型           
         ,-1                              --账户类型             
         ,COALESCE(C.BIZ_TP_ID, -1)       --业务类别码           
         ,-1                              --保证金种类           
         ,COALESCE(C.AR_NM ,'')           --账户名称             
         ,-1                              --账户性质             
         ,A.RPRG_OU_IP_ID                 --归属机构             
         ,''                              --信用站编号           
         ,A.ACG_SBJ_ID                    --科目号               
         ,''                              --透支科目号  
         ,min(A.AR_LCS_TP_ID)             --账户生命周期  
         ,-1                              --外汇客户类别         
         ,COALESCE(C.PD_GRP_CODE,'')      --产品类别码           
         ,COALESCE(C.PD_SUB_CODE,'')      --产品子代码           
         ,0                               --卡关联标志
         ,-1                              --质押标志
				 ,-1					  						      --协定存款标志
     	   ,0													      --开户金额            
     	   ,'9999-12-31'                    --起息日期
         ,0                               --定期存款存期         
         ,0                               --提前通知存款通知天数 
         ,'1899-12-31'                    --到期日期             
         ,''                              --央行开户核准书编号   
         ,SUM(A.BAL_AMT)                  --账户余额             
         ,0                               --透支额度             
         ,SUM(A.FRZ_AMT)                  --冻结金额             
         ,0                               --账户当前保留金额     
         ,0                               --止付金额             
         ,MIN(A.EFF_DT)                   --开户日期             
         ,MAX(A.END_DT)                   --销户日期             
         ,COALESCE(C.PRIM_CST_ID,'')      --客户内码             
         ,COALESCE(D.CST_NO,'')           --客户号               
         ,COALESCE(D.CST_FUL_NM ,'')      --客户名称          
         ,'1899-12-31'                    --结清日期             
         ,-1                              --期限
         ,SMY_DATE
     FROM SOR.EQTY_AC_SUB_AR A LEFT OUTER JOIN SOR.AC_AR B ON A.EQTY_AC_AR_ID = B.AC_AR_ID
                               LEFT OUTER JOIN SOR.EQTY_AC_AR C ON A.EQTY_AC_AR_ID = C.EQTY_AC_AR_ID
                               LEFT OUTER JOIN SOR.CST D ON C.PRIM_CST_ID =D.CST_ID
     where A.DEL_F = 0
     GROUP BY
          A.EQTY_AC_AR_ID
         ,A.CSH_RMIT_IND_TP_ID
         ,A.DNMN_CCY_ID
         ,B.AC_AR_TP_ID
         ,C.ENT_IDV_IND
         ,C.BIZ_TP_ID
         ,C.AR_NM
         ,A.RPRG_OU_IP_ID
         ,A.ACG_SBJ_ID
         ,C.PD_GRP_CODE
         ,C.PD_SUB_CODE
         ,A.PLG_F
         ,C.PRIM_CST_ID
         ,D.CST_NO
         ,D.CST_FUL_NM
     ;
     
     /** 收集操作信息 */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --6
     SET SMY_STEPDESC = '删除当日销户数据';
     
     DELETE FROM SMY.DEP_AR_SMY 
     WHERE (DEP_AR_ID,CSH_RMIT_IND_TP_ID,DNMN_CCY_ID) in (
         SELECT DMD_DEP_AR_ID,CSH_RMIT_IND_TP_ID,DNMN_CCY_ID 
         FROM SOR.DMD_DEP_SUB_AR A WHERE A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F <> 0
         union all 
         SELECT FT_DEP_AR_ID,CSH_RMIT_IND_TP_ID,DNMN_CCY_ID 
         FROM SOR.FT_DEP_AR A WHERE A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F <> 0
         union all
         SELECT INTRBNK_DEP_AR_ID,CSH_RMIT_IND_TP_ID,DNMN_CCY_ID 
         FROM SOR.INTRBNK_DEP_SUB_AR A WHERE A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F <> 0
         union all
         SELECT EQTY_AC_AR_ID,CSH_RMIT_IND_TP_ID,DNMN_CCY_ID 
         FROM SOR.EQTY_AC_SUB_AR A WHERE A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F <> 0
     );
     
     /** 收集操作信息 */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --7
     SET SMY_STEPDESC = '将临时表数据Merge到目标表SMY.DEP_AR_SMY';
     
     MERGE INTO SMY.DEP_AR_SMY AS T 
         USING SESSION.TMP_DEP_AR_SMY AS S
         ON
             T.DEP_AR_ID          = S.DEP_AR_ID
         AND T.CSH_RMIT_IND_TP_ID = S.CSH_RMIT_IND_TP_ID
         AND T.DNMN_CCY_ID        = S.DNMN_CCY_ID
     WHEN MATCHED THEN UPDATE SET
        DEP_TP_ID         =S.DEP_TP_ID
       ,ENT_IDV_IND       =S.ENT_IDV_IND
       ,STMT_DLV_TP_ID    =S.STMT_DLV_TP_ID
       ,DEP_PSBK_TP_ID    =S.DEP_PSBK_TP_ID
       ,DEP_AC_TP_ID      =S.DEP_AC_TP_ID
       ,BIZ_TP_ID         =S.BIZ_TP_ID
       ,MG_TP_ID          =S.MG_TP_ID
       ,AR_NM             =S.AR_NM
       ,AC_CHRCTR_TP_ID   =S.AC_CHRCTR_TP_ID
       ,RPRG_OU_IP_ID     =S.RPRG_OU_IP_ID
       ,CR_STA_SEQ_NO     =S.CR_STA_SEQ_NO
       ,ACG_SBJ_ID        =S.ACG_SBJ_ID
       ,OD_ACG_SBJ_ID     =S.OD_ACG_SBJ_ID
       ,AR_LCS_TP_ID      =S.AR_LCS_TP_ID
       ,FX_CST_TP_ID      =S.FX_CST_TP_ID
       ,PD_GRP_CODE       =S.PD_GRP_CODE
       ,PD_SUB_CODE       =S.PD_SUB_CODE
       ,CRD_ASOCT_AC_F    =S.CRD_ASOCT_AC_F
       ,PLG_F             =S.PLG_F
       ,NGO_RT_F          =S.NGO_RT_F
       ,OPN_BAL           =S.OPN_BAL
       ,INT_CLCN_EFF_DT   =S.INT_CLCN_EFF_DT
       ,DEP_PRD_NOM       =S.DEP_PRD_NOM
       ,NTC_PRD_NOD       =S.NTC_PRD_NOD
       ,MAT_DT            =S.MAT_DT
       ,BAL_AMT           =S.BAL_AMT
       ,CRED_OD_LMT       =S.CRED_OD_LMT
       ,FRZ_AMT           =S.FRZ_AMT
       ,RES_TRD_AMT       =S.RES_TRD_AMT
       ,SUS_PAY_AMT       =S.SUS_PAY_AMT
       ,EFF_DT            =S.EFF_DT
       ,END_DT            =S.END_DT
       ,CST_ID            =S.CST_ID
       ,CST_NO            =S.CST_NO
       ,CST_NM            =S.CST_NM
       ,TM_MAT_SEG_ID     =S.TM_MAT_SEG_ID
       ,SETL_DT           =S.SETL_DT
       ,ACG_DT            =S.ACG_DT
     WHEN NOT MATCHED THEN INSERT
     (
        DEP_AR_ID
       ,CSH_RMIT_IND_TP_ID
       ,DNMN_CCY_ID
       ,DEP_TP_ID
       ,ENT_IDV_IND
       ,STMT_DLV_TP_ID
       ,DEP_PSBK_TP_ID
       ,DEP_AC_TP_ID
       ,BIZ_TP_ID
       ,MG_TP_ID
       ,AR_NM
       ,AC_CHRCTR_TP_ID
       ,RPRG_OU_IP_ID
       ,CR_STA_SEQ_NO
       ,ACG_SBJ_ID
       ,OD_ACG_SBJ_ID
       ,AR_LCS_TP_ID
       ,FX_CST_TP_ID
       ,PD_GRP_CODE
       ,PD_SUB_CODE
       ,CRD_ASOCT_AC_F
       ,PLG_F
       ,NGO_RT_F
       ,OPN_BAL
       ,INT_CLCN_EFF_DT
       ,DEP_PRD_NOM
       ,NTC_PRD_NOD
       ,MAT_DT
       ,PBC_APV_NO
       ,BAL_AMT
       ,CRED_OD_LMT
       ,FRZ_AMT
       ,RES_TRD_AMT
       ,SUS_PAY_AMT
       ,EFF_DT
       ,END_DT
       ,CST_ID
       ,CST_NO
       ,CST_NM
       ,TM_MAT_SEG_ID
       ,SETL_DT
       ,ACG_DT
     )
     VALUES
     (
        S.DEP_AR_ID
       ,S.CSH_RMIT_IND_TP_ID
       ,S.DNMN_CCY_ID
       ,S.DEP_TP_ID
       ,S.ENT_IDV_IND
       ,S.STMT_DLV_TP_ID
       ,S.DEP_PSBK_TP_ID
       ,S.DEP_AC_TP_ID
       ,S.BIZ_TP_ID
       ,S.MG_TP_ID
       ,S.AR_NM
       ,S.AC_CHRCTR_TP_ID
       ,S.RPRG_OU_IP_ID
       ,S.CR_STA_SEQ_NO
       ,S.ACG_SBJ_ID
       ,S.OD_ACG_SBJ_ID
       ,S.AR_LCS_TP_ID
       ,S.FX_CST_TP_ID
       ,S.PD_GRP_CODE
       ,S.PD_SUB_CODE
       ,S.CRD_ASOCT_AC_F
       ,S.PLG_F
       ,S.NGO_RT_F
       ,S.OPN_BAL
       ,S.INT_CLCN_EFF_DT
       ,S.DEP_PRD_NOM
       ,S.NTC_PRD_NOD
       ,S.MAT_DT
       ,S.PBC_APV_NO
       ,S.BAL_AMT
       ,S.CRED_OD_LMT
       ,S.FRZ_AMT
       ,S.RES_TRD_AMT
       ,S.SUS_PAY_AMT
       ,S.EFF_DT
       ,S.END_DT
       ,S.CST_ID
       ,S.CST_NO
       ,S.CST_NM
       ,S.TM_MAT_SEG_ID
       ,S.SETL_DT
       ,S.ACG_DT
     )
;
     
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --8
     SET SMY_STEPDESC = '存储过程结束！' ;--
     SET SMY_RCOUNT =0 ; --
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP); --

COMMIT;
END@