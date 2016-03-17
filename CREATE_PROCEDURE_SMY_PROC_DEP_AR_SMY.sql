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
-- 2010-03-24   Xu Yan       Updated the special logic for '�ɽ�'
-- 2010-09-02   Wang Youbing Modify select MAT_SEG_ID from SMY.SMY_DT
-- 2010-09-03   Wang Youbing Modify select CST_NO,CST_NM from SOR.CST
-- 2011-05-29   Chen XiaoWen ��ǰ����������DEP_AR_SMY�Ĺ������������,������ʱ�����м�����,������DEP_AR_SMY��ͳһ������
-- 2011-08-05   Li ShuWen    �����ݲ���SMY.DEP_AR_SMY��ǰ������ʱ��TMPX���ֲ�Join�����ⲻ�ȶ��������
-- 2011-09-26   Li ShuWen    �Ա�SMY.DEP_AR_SMY���������ֶ�PLG_F,NGO_RT_F,OPN_BAL��������ӳ������������
-- 2011-10-31   Li Shenyu    add column INT_CLCN_EFF_DT to SMY.DEP_AR_SMY
-- 2012-02-27   Chen XiaoWen �����ʱ��TMP_RESULT��TMPX�����ݣ��ֲ���ֱ�Ӳ���Ŀ����Դ˼����м�����
-- 2012-03-31   Chen XiaoWen �޸�Ϊ����merge������ʽ
-- 2012-06-05   Chen XiaoWen TM_MAT_SEG_ID�ֶ���Ϊ-1��ɾ��������������
-- 2012-07-10   Chen XiaoWen �޸Ĺɽ��˻�����Ѻ��־Ϊ-1
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
     /*������ֵ*/
     
     ---Modified By Wang Youbing On 2010-09-02 Start----
     --SELECT MAT_SEG_ID INTO MAT_SEG_ID_1 FROM SMY.MAT_SEG WHERE LOW_VAL=-99999;--
     --SELECT MAT_SEG_ID INTO MAT_SEG_ID_2 FROM SMY.MAT_SEG WHERE MAX_VAL=99999;--
     --SELECT MAX(LOW_VAL)-1 INTO CNT FROM SMY.MAT_SEG;--
     ---Modified By Wang Youbing On 2010-09-02 End----
     
     SET SMY_PROCNM = 'PROC_DEP_AR_SMY';--
     SET SMY_DATE=ACCOUNTING_DATE;--
     
     /*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
     DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
     COMMIT;--
     INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '�洢���̿�ʼ����', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
     COMMIT;--
     
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --2
     SET SMY_STEPDESC = '����SOR.DMD_DEP_SUB_AR�����������ݵ���ʱ��';
     
     DECLARE GLOBAL TEMPORARY TABLE TMP_DELTA_RESULT AS (
         SELECT
             A.DMD_DEP_AR_ID AS DEP_AR_ID    						            --����˺�               
         	  ,A.CSH_RMIT_IND_TP_ID	AS CSH_RMIT_IND_TP_ID					    --��������             
         	  ,A.DNMN_CCY_ID AS DNMN_CCY_ID      						          --����                 
         	  ,COALESCE(B.ENT_IDV_IND     ,-1) AS ENT_IDV_IND         --��ҵ�����˱�־       
         	  ,COALESCE(B.STMT_DLV_TP_ID  ,-1) AS STMT_DLV_TP_ID      --���˻����˵���־     
         	  ,COALESCE(B.DEP_PSBK_TP_ID  ,-1) AS DEP_PSBK_TP_ID  		--�浥������           
         	  ,COALESCE(B.DMD_DEP_AC_TP_ID, -1) AS DEP_AC_TP_ID       --�˻�����             
         	  ,COALESCE(B.BIZ_TP_ID       ,-1) AS BIZ_TP_ID           --ҵ�������           
         	  ,COALESCE(B.MG_TP_ID        ,-1) AS MG_TP_ID            --��֤������           
         	  ,B.AR_NM AS AR_NM            						                --�˻�����             
         	  ,COALESCE(B.AC_CHRCTR_TP_ID, -1) AS AC_CHRCTR_TP_ID     --�˻�����             
         	  ,A.RPRG_OU_IP_ID AS RPRG_OU_IP_ID    						        --��������             
         	  ,B.CR_STA_SEQ_NO AS CR_STA_SEQ_NO    						        --����վ���           
         	  ,A.ACG_SBJ_ID AS ACG_SBJ_ID       						          --��Ŀ��               
         	  ,A.OD_ACG_SBJ_ID AS OD_ACG_SBJ_ID    						        --͸֧��Ŀ��           
         	  ,A.AR_LCS_TP_ID AS AR_LCS_TP_ID     						        --�˻���������         
         	  ,B.FX_CST_TP_ID AS FX_CST_TP_ID     						        --���ͻ����         
         	  ,B.PD_GRP_CODE AS PD_GRP_CODE      						          --��Ʒ�����           
         	  ,B.PD_SUB_CODE AS PD_SUB_CODE      						          --��Ʒ�Ӵ���           
         	  ,B.CRD_ASOCT_AC_F AS CRD_ASOCT_AC_F   						      --��������־         
         	  ,A.PLG_F AS PLG_F 						    						          --��Ѻ��־ 
         	  ,B.NGO_RT_F AS NGO_RT_F 						   						      --Э������־
         	  ,cast(0 as decimal(17,2)) AS OPN_BAL		 			 	 				--�������   
         	  ,A.INT_CLCN_EFF_DT AS INT_CLCN_EFF_DT                   --��Ϣ����
         	  ,0 AS DEP_PRD_NOM                						            --���ڴ�����         
         	  ,0 AS NTC_PRD_NOD                						            --��ǰ֪ͨ���֪ͨ���� 
         	  ,'1899-12-31' AS MAT_DT     						                --��������             
         	  ,B.PBC_APV_NO AS PBC_APV_NO       						          --���п�����׼����   
         	  ,A.BAL_AMT AS BAL_AMT          						              --�˻����             
         	  ,A.CRED_OD_LMT AS CRED_OD_LMT      						          --͸֧���             
         	  ,A.FRZ_AMT AS FRZ_AMT          						              --������             
         	  ,A.RES_TRD_AMT AS RES_TRD_AMT      						          --�˻���ǰ�������     
         	  ,A.SUS_PAY_AMT AS SUS_PAY_AMT      						          --ֹ�����             
         	  ,A.EFF_DT AS EFF_DT           						              --��������             
         	  ,B.END_DT AS END_DT           						              --��������             
         	  ,B.PRIM_CST_ID AS CST_ID      						              --�ͻ�����         
         	  ,A.STL_DT AS SETL_DT                                    --��������             
         	  ,-1 AS TM_MAT_SEG_ID                                    --����
         	  ,A.LAST_ETL_ACG_DT AS ACG_DT
         FROM SOR.DMD_DEP_SUB_AR A LEFT JOIN SOR.DMD_DEP_MN_AR B ON A.DMD_DEP_AR_ID =B.DMD_DEP_AR_ID 
         where A.LAST_ETL_ACG_DT='2012-03-31' and A.DEL_F = 0
     ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(DEP_AR_ID);
     
     CREATE INDEX SESSION.TMP_DELTA_DEP_AR_ID ON SESSION.TMP_DELTA_RESULT(DEP_AR_ID);
     CREATE INDEX SESSION.TMP_DELTA_CST_ID ON SESSION.TMP_DELTA_RESULT(CST_ID);
     
     INSERT INTO SESSION.TMP_DELTA_RESULT
     SELECT 
         A.DMD_DEP_AR_ID     						       --����˺�               
     	  ,A.CSH_RMIT_IND_TP_ID						       --��������             
     	  ,A.DNMN_CCY_ID       						       --����                 
     	  ,COALESCE(B.ENT_IDV_IND     ,-1)       --��ҵ�����˱�־       
     	  ,COALESCE(B.STMT_DLV_TP_ID  ,-1)       --���˻����˵���־     
     	  ,COALESCE(B.DEP_PSBK_TP_ID  ,-1)   		 --�浥������           
     	  ,COALESCE(B.DMD_DEP_AC_TP_ID, -1)      --�˻�����             
     	  ,COALESCE(B.BIZ_TP_ID       ,-1)       --ҵ�������           
     	  ,COALESCE(B.MG_TP_ID        ,-1)       --��֤������           
     	  ,B.AR_NM             						       --�˻�����             
     	  ,COALESCE(B.AC_CHRCTR_TP_ID, -1)       --�˻�����             
     	  ,A.RPRG_OU_IP_ID     						       --��������             
     	  ,B.CR_STA_SEQ_NO     						       --����վ���           
     	  ,A.ACG_SBJ_ID        						       --��Ŀ��               
     	  ,A.OD_ACG_SBJ_ID     						       --͸֧��Ŀ��           
     	  ,A.AR_LCS_TP_ID      						       --�˻���������         
     	  ,B.FX_CST_TP_ID      						       --���ͻ����         
     	  ,B.PD_GRP_CODE       						       --��Ʒ�����           
     	  ,B.PD_SUB_CODE       						       --��Ʒ�Ӵ���           
     	  ,B.CRD_ASOCT_AC_F    						       --��������־         
     	  ,A.PLG_F  						    						 --��Ѻ��־ 
     	  ,B.NGO_RT_F  						   						 --Э������־
     	  ,0			 			 	 											 --�������   
     	  ,A.INT_CLCN_EFF_DT                     --��Ϣ���� -- add by Li Shenyu at 20111031
     	  ,0                 						         --���ڴ�����         
     	  ,0                 						         --��ǰ֪ͨ���֪ͨ���� 
     	  ,'1899-12-31'      						         --��������             
     	  ,B.PBC_APV_NO        						       --���п�����׼����   
     	  ,A.BAL_AMT           						       --�˻����             
     	  ,A.CRED_OD_LMT       						       --͸֧���             
     	  ,A.FRZ_AMT           						       --������             
     	  ,A.RES_TRD_AMT       						       --�˻���ǰ�������     
     	  ,A.SUS_PAY_AMT       						       --ֹ�����             
     	  ,A.EFF_DT            						       --��������             
     	  ,B.END_DT            						       --��������             
     	  ,B.PRIM_CST_ID       						       --�ͻ�����         
     	  ,A.STL_DT                              --��������             
     	  ,-1                                    --����
     	  ,SMY_DATE
     FROM SOR.DMD_DEP_SUB_AR A LEFT JOIN SOR.DMD_DEP_MN_AR B ON A.DMD_DEP_AR_ID =B.DMD_DEP_AR_ID 
     where A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F = 0;
     
     /** �ռ�������Ϣ */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --3
     SET SMY_STEPDESC = '����SOR.FT_DEP_AR�����������ݵ���ʱ��';
     
     INSERT INTO SESSION.TMP_DELTA_RESULT
     SELECT
          A.FT_DEP_AR_ID           --����˺�            
     	   ,A.CSH_RMIT_IND_TP_ID     --��������            
     	   ,A.DNMN_CCY_ID            --����
     	   ,A.ENT_IDV_IND            --��ҵ�����˱�־      
     	   ,A.STMT_DLV_TP_ID         --���˻����˵���־    
     	   ,A.DEP_PSBK_TP_ID         --�浥������          
     	   ,A.FT_DEP_AC_TP_ID        --�˻�����            
     	   ,A.BIZ_TP_ID              --ҵ�������          
     	   ,A.MG_TP_ID               --��֤������          
     	   ,A.AR_NM                  --�˻�����            
     	   ,-1                       --�˻�����            
     	   ,A.RPRG_OU_IP_ID          --��������            
     	   ,A.CR_STA_SEQ_NO          --����վ���          
     	   ,A.ACG_SBJ_ID             --��Ŀ��              
     	   ,''                       --͸֧��Ŀ��          
     	   ,A.AR_LCS_TP_ID           --�˻���������        
     	   ,A.FX_CST_TP_ID           --���ͻ����        
     	   ,A.PD_GRP_CODE            --��Ʒ�����          
     	   ,A.PD_SUB_CODE            --��Ʒ�Ӵ���          
     	   ,0                        --��������־
     	   ,A.PLG_F   						   --��Ѻ��־
				 ,A.NGO_RT_F					   	 --Э������־
     	   ,A.OPN_BAL  						   --�������           
     	   ,A.INT_CLCN_EFF_DT        --��Ϣ���� -- add by Li Shenyu at 20111031
     	   ,A.DEP_PRD_NOM            --���ڴ�����        
     	   ,A.NTC_PRD_NOD            --��ǰ֪ͨ���֪ͨ����
     	   ,A.NTNL_MAT_DT            --��������            
     	   ,A.PBC_APV_NO             --���п�����׼����  
     	   ,A.BAL_AMT                --�˻����            
     	   ,0                        --͸֧���            
     	   ,A.FRZ_AMT                --������            
     	   ,A.RES_TRD_AMT            --�˻���ǰ�������    
     	   ,A.SUS_PAY_AMT            --ֹ�����            
     	   ,A.EFF_DT                 --��������            
     	   ,A.END_DT                 --��������            
     	   ,A.PRIM_CST_ID            --�ͻ�����            
     	   ,A.END_DT                 --��������            
         ,-1
         ,SMY_DATE
     FROM SOR.FT_DEP_AR A where A.LAST_ETL_ACG_DT=SMY_DATE and A.DEL_F = 0;
     
     /** �ռ�������Ϣ */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --4
     SET SMY_STEPDESC = '����SOR.INTRBNK_DEP_SUB_ARͬҵ�������ݵ���ʱ��';
     
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
     	   ,A.PLG_F   						   --��Ѻ��־
				 ,B.NGO_RT_F					   	 --Э������־
     	   ,0				  						   --�������       
     	   ,A.INT_CLCN_EFF_DT        --��Ϣ���� -- add by Li Shenyu at 20111031
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
     
     /** �ռ�������Ϣ */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --5
     SET SMY_STEPDESC = '�����м����ݵ���ʱ��';
     
     DECLARE GLOBAL TEMPORARY TABLE TMP_DEP_AR_SMY LIKE SMY.DEP_AR_SMY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(DEP_AR_ID);
     CREATE INDEX SESSION.TMP_DEP_AR_SMY_IDX ON SESSION.TMP_DEP_AR_SMY(DEP_AR_ID,CSH_RMIT_IND_TP_ID,DNMN_CCY_ID);
         
     INSERT INTO SESSION.TMP_DEP_AR_SMY
     (
          DEP_AR_ID                   --����˺�
         ,CSH_RMIT_IND_TP_ID          --��������
         ,DNMN_CCY_ID                 --����
         ,DEP_TP_ID                   --����/�����˻���ʶ
         ,ENT_IDV_IND                 --��ҵ�����˱�־
         ,STMT_DLV_TP_ID              --���˻����˵���־
         ,DEP_PSBK_TP_Id              --�浥������
         ,DEP_AC_TP_ID                --�˻�����
         ,BIZ_TP_ID                   --ҵ�������
         ,MG_TP_ID                    --��֤������
         ,AR_NM                       --�˻�����
         ,AC_CHRCTR_TP_ID             --�˻�����
         ,RPRG_OU_IP_ID               --��������
         ,CR_STA_SEQ_NO               --����վ���
         ,ACG_SBJ_ID                  --��Ŀ��
         ,OD_ACG_SBJ_ID               --͸֧��Ŀ��
         ,AR_LCS_TP_ID                --�˻���������
         ,FX_CST_TP_ID                --���ͻ����
         ,PD_GRP_CODE                 --��Ʒ�����
         ,PD_SUB_CODE                 --��Ʒ�Ӵ���
         ,CRD_ASOCT_AC_F              --��������־
         ,PLG_F   						    		--��Ѻ��־ 
     	   ,NGO_RT_F   						   		--Э������־
     	   ,OPN_BAL   						    	--������� 
     	   ,INT_CLCN_EFF_DT             --��Ϣ���� -- add by Li Shenyu at 20111031
         ,DEP_PRD_NOM                 --���ڴ�����
         ,NTC_PRD_NOD                 --��ǰ֪ͨ���֪ͨ����
         ,MAT_DT                      --��������
         ,PBC_APV_NO                  --���п�����׼����
         ,BAL_AMT                     --�˻����
         ,CRED_OD_LMT                 --͸֧���
         ,FRZ_AMT                     --������
         ,RES_TRD_AMT                 --�˻���ǰ�������
         ,SUS_PAY_AMT                 --ֹ�����
         ,EFF_DT                      --��������
         ,END_DT                      --��������
         ,CST_ID                      --�ͻ�����
         ,CST_NO                      --�ͻ���
         ,CST_NM                      --�ͻ�����
         ,SETL_DT                     --��������
         ,TM_MAT_SEG_ID
         ,ACG_DT
     )
     SELECT 
         A.DEP_AR_ID     						           --����˺�               
     	  ,A.CSH_RMIT_IND_TP_ID						       --��������             
     	  ,A.DNMN_CCY_ID       						       --����                 
     	  ,COALESCE(D.AC_AR_TP_ID  ,-1)					 --����/�����˻���ʶ    
     	  ,A.ENT_IDV_IND       						       --��ҵ�����˱�־       
     	  ,A.STMT_DLV_TP_ID                      --���˻����˵���־     
     	  ,A.DEP_PSBK_TP_ID   						       --�浥������           
     	  ,A.DEP_AC_TP_ID                        --�˻�����             
     	  ,A.BIZ_TP_ID                           --ҵ�������           
     	  ,A.MG_TP_ID  						               --��֤������           
     	  ,A.AR_NM             						       --�˻�����             
     	  ,A.AC_CHRCTR_TP_ID                     --�˻�����             
     	  ,A.RPRG_OU_IP_ID     						       --��������             
     	  ,A.CR_STA_SEQ_NO     						       --����վ���           
     	  ,A.ACG_SBJ_ID        						       --��Ŀ��               
     	  ,A.OD_ACG_SBJ_ID     						       --͸֧��Ŀ��           
     	  ,A.AR_LCS_TP_ID      						       --�˻���������         
     	  ,A.FX_CST_TP_ID      						       --���ͻ����         
     	  ,A.PD_GRP_CODE       						       --��Ʒ�����           
     	  ,A.PD_SUB_CODE       						       --��Ʒ�Ӵ���           
     	  ,A.CRD_ASOCT_AC_F    						       --��������־
     	  ,A.PLG_F   						    						 --��Ѻ��־ 
     	  ,A.NGO_RT_F  						   						 --Э������־
     	  ,A.OPN_BAL  						    					 --�������                
     	  ,A.INT_CLCN_EFF_DT                     --��Ϣ���� -- add by Li Shenyu at 20111031
     	  ,A.DEP_PRD_NOM                 				 --���ڴ�����         
     	  ,A.NTC_PRD_NOD                 				 --��ǰ֪ͨ���֪ͨ���� 
     	  ,A.MAT_DT      						             --��������             
     	  ,A.PBC_APV_NO        						       --���п�����׼����   
     	  ,A.BAL_AMT           						       --�˻����             
     	  ,A.CRED_OD_LMT       						       --͸֧���             
     	  ,A.FRZ_AMT           						       --������             
     	  ,A.RES_TRD_AMT       						       --�˻���ǰ�������     
     	  ,A.SUS_PAY_AMT       						       --ֹ�����             
     	  ,A.EFF_DT            						       --��������             
     	  ,A.END_DT            						       --��������             
     	  ,A.CST_ID       						           --�ͻ�����
     	  ,C.CST_NO            						       --�ͻ���               
     	  ,C.CST_FUL_NM            						   --�ͻ�����
     	  ,A.SETL_DT                             --��������             
     	  ,A.TM_MAT_SEG_ID                       --����
     	  ,A.ACG_DT
     FROM SESSION.TMP_DELTA_RESULT A LEFT JOIN SOR.AC_AR D ON A.DEP_AR_ID = D.AC_AR_ID
     LEFT JOIN SOR.CST C ON A.CST_ID = C.CST_ID;
     
     /** �ռ�������Ϣ */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --7
     SET SMY_STEPDESC = '��SOR.EQTY_AC_SUB_AR����ɽ�����';
     
     INSERT INTO SESSION.TMP_DEP_AR_SMY
     (    DEP_AR_ID                   --����˺�
         ,CSH_RMIT_IND_TP_ID          --��������
         ,DNMN_CCY_ID                 --����
         ,DEP_TP_ID                   --����/�����˻���ʶ
         ,ENT_IDV_IND                 --��ҵ�����˱�־
         ,STMT_DLV_TP_ID              --���˻����˵���־
         ,DEP_PSBK_TP_Id              --�浥������
         ,DEP_AC_TP_ID                --�˻�����
         ,BIZ_TP_ID                   --ҵ�������
         ,MG_TP_ID                    --��֤������
         ,AR_NM                       --�˻�����
         ,AC_CHRCTR_TP_ID             --�˻�����
         ,RPRG_OU_IP_ID               --��������
         ,CR_STA_SEQ_NO               --����վ���
         ,ACG_SBJ_ID                  --��Ŀ��
         ,OD_ACG_SBJ_ID               --͸֧��Ŀ��
         ,AR_LCS_TP_ID                --�˻���������
         ,FX_CST_TP_ID                --���ͻ����
         ,PD_GRP_CODE                 --��Ʒ�����
         ,PD_SUB_CODE                 --��Ʒ�Ӵ���
         ,CRD_ASOCT_AC_F              --��������־
         ,PLG_F   						    		--��Ѻ��־ 
     	   ,NGO_RT_F   						   		--Э������־
     	   ,OPN_BAL   						    	--������� 
     	   ,INT_CLCN_EFF_DT             --��Ϣ���� -- add by Li Shenyu at 20111031
         ,DEP_PRD_NOM                 --���ڴ�����
         ,NTC_PRD_NOD                 --��ǰ֪ͨ���֪ͨ����
         ,MAT_DT                      --��������
         ,PBC_APV_NO                  --���п�����׼����
         ,BAL_AMT                     --�˻����
         ,CRED_OD_LMT                 --͸֧���
         ,FRZ_AMT                     --������
         ,RES_TRD_AMT                 --�˻���ǰ�������
         ,SUS_PAY_AMT                 --ֹ�����
         ,EFF_DT                      --��������
         ,END_DT                      --��������
         ,CST_ID                      --�ͻ�����
         ,CST_NO                      --�ͻ���
         ,CST_NM                      --�ͻ�����
         ,SETL_DT                     --��������
         ,TM_MAT_SEG_ID
         ,ACG_DT
         )
     SELECT 
          A.EQTY_AC_AR_ID                 --����˺�             
         ,A.CSH_RMIT_IND_TP_ID            --��������             
         ,A.DNMN_CCY_ID                   --����                 
         ,COALESCE(B.AC_AR_TP_ID ,-1)     --����/�����˻���ʶ    
         ,COALESCE(C.ENT_IDV_IND ,-1)     --��ҵ�����˱�־       
         ,-1                              --���˻����˵���־     
         ,-1                              --�浥������           
         ,-1                              --�˻�����             
         ,COALESCE(C.BIZ_TP_ID, -1)       --ҵ�������           
         ,-1                              --��֤������           
         ,COALESCE(C.AR_NM ,'')           --�˻�����             
         ,-1                              --�˻�����             
         ,A.RPRG_OU_IP_ID                 --��������             
         ,''                              --����վ���           
         ,A.ACG_SBJ_ID                    --��Ŀ��               
         ,''                              --͸֧��Ŀ��  
         ,min(A.AR_LCS_TP_ID)             --�˻���������  
         ,-1                              --���ͻ����         
         ,COALESCE(C.PD_GRP_CODE,'')      --��Ʒ�����           
         ,COALESCE(C.PD_SUB_CODE,'')      --��Ʒ�Ӵ���           
         ,0                               --��������־
         ,-1                              --��Ѻ��־
				 ,-1					  						      --Э������־
     	   ,0													      --�������            
     	   ,'9999-12-31'                    --��Ϣ����
         ,0                               --���ڴ�����         
         ,0                               --��ǰ֪ͨ���֪ͨ���� 
         ,'1899-12-31'                    --��������             
         ,''                              --���п�����׼����   
         ,SUM(A.BAL_AMT)                  --�˻����             
         ,0                               --͸֧���             
         ,SUM(A.FRZ_AMT)                  --������             
         ,0                               --�˻���ǰ�������     
         ,0                               --ֹ�����             
         ,MIN(A.EFF_DT)                   --��������             
         ,MAX(A.END_DT)                   --��������             
         ,COALESCE(C.PRIM_CST_ID,'')      --�ͻ�����             
         ,COALESCE(D.CST_NO,'')           --�ͻ���               
         ,COALESCE(D.CST_FUL_NM ,'')      --�ͻ�����          
         ,'1899-12-31'                    --��������             
         ,-1                              --����
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
     
     /** �ռ�������Ϣ */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --6
     SET SMY_STEPDESC = 'ɾ��������������';
     
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
     
     /** �ռ�������Ϣ */
     GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
     INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --7
     SET SMY_STEPDESC = '����ʱ������Merge��Ŀ���SMY.DEP_AR_SMY';
     
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
     
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --8
     SET SMY_STEPDESC = '�洢���̽�����' ;--
     SET SMY_RCOUNT =0 ; --
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP); --

COMMIT;
END@