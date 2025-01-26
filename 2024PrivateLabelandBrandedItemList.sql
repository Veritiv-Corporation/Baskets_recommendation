WITH tmp_variables AS (
   select cast(max(snap_mo_puma) as char(6)) as SnapDate from veritiv.edm.snap_mo
)
select
	case when DSL_IM.PROD_SEG_NAME in ('Print','Facility Solutions','Packaging') then DSL_IM.PROD_SEG_NAME else
        case when not(DSL_I.LEGACY_PROD_SEG_NAME in ('Print','Facility Solutions','Packaging')) then 'Other'
        else DSL_I.LEGACY_PROD_SEG_NAME end
    end as PRODUCT_SEGMENT,
    DSL_IM.PROD_CAT_1_NAME as prod_cat_1,
    DSL_IM.PROD_CAT_2_NAME as prod_cat_2,
    DSL_IM.PROD_CAT_3_NAME as prod_cat_3,
    DSL_IM.PROD_CAT_1_NAME || ' | ' || DSL_IM.PROD_CAT_2_NAME || ' | ' || DSL_IM.PROD_CAT_3_NAME as full_prod_cat,
    DSL_IM.BRAND,
    dsl_vm.vend_name,
	DIM.active_ingr,
    DIM.adhesive,
    DIM.amphere,
    DIM.basic_mat as material,
    DIM.basic_size_desc,
    DIM.basis_wgt,
    DIM.box_style,
    DIM.brightness,
    DIM.burst_strength,
    DIM.caliper,
    DIM.cap as capacity,
    DIM.coating,
    DIM.coil_size,
    DIM.collation,
    DIM.color,
    DIM.conc,
    DIM.contains_wood_fiber,
    DIM.core_item,
    DIM.core_size,
    DIM.corr_test,
    DIM.cotton_content,
    DIM.cradle_to_cradle,
    DIM.ctn_skid_packing as outer_packing,
    DIM.dangerous_goods,
    DIM.density,
    DIM.dilu_ratio,
    DIM.dot,
    DIM.env_closure,
    DIM.env_dimensions,
    DIM.env_flap,
    DIM.env_seam,
    DIM.env_seam_desc,
    DIM.env_tint_color,
    DIM.env_type,
    DIM.env_window_pos,
    DIM.env_window_size,
    DIM.env_window_type,
    DIM.envr_levy,
    DIM.equip_sw,
    DIM.fair_trd,
    DIM.finish,
    DIM.flash_pnt,
    DIM.flute,
    DIM.fold,
    DIM.form,
    DIM.gauge,
    DIM.grade,
    DIM.green_seal,
    DIM.gsm,
    DIM.hp,
    DIM.image,
    DIM.inner_packing,
    DIM.innov,
    DIM.item_size,
    DIM.liner_mat,
    DIM.logical_del,
    DIM.m_wgt,
    DIM.mfg_item_nbr,
    DIM.model,
    DIM.nbr_of_parts,
    DIM.nbr_of_punch_holes,
    DIM.opacity,
    DIM.oper_rate,
    DIM.pack_meth,
    DIM.packing_grp,
    DIM.packing_qty,
    DIM.pallet_qty,
    DIM.perf_type,
    DIM.ph,
    DIM.ply,
    DIM.ppi,
    DIM.private_label_sw,
    DIM.prod as product_type,
    DIM.quality_type,
    DIM.rainforest_alliance,
    DIM.rcy,
    DIM.rcy_resin,
    DIM.rcyd_resin,
    DIM.ready_to_use,
    DIM.ream_marked,
    DIM.recy_post_cnsmr,
    DIM.recy_ttl_content,
    DIM.reuse,
    DIM.roll_dia,
    DIM.roll_wgt,
    DIM.rpa_100pct,
    DIM.rpm,
    DIM.rpt_grp,
    DIM.rspct_id,
    DIM.rspct_qual,
    DIM.scent,
    DIM.scoring,
    DIM.sds,
    DIM.series_nbr,
    DIM.sheffield_smoothness,
    DIM.shelf_life,
    DIM.sht_layout,
    DIM.shts_per_pkg_box,
    DIM.size_ind,
    DIM.sqn,
    DIM.storage_restriction,
    DIM.stretch_film_mfg_proc,
    DIM.subgrade,
    DIM.sustainable,
    DIM.tensile,
    DIM.test,
    DIM.thickness,
    DIM.trade_name,
    DIM.vol,
    DIM.voltage,
    DIM.web_create,
    DIM.wmk,
    case when f.dist_chnl_cde = 'D' then 'D' else 'W' end as order_dist_chnl,
    case when (DSL_IM.PROD_CAT_4_NAME = 'Private Brand' 
    		or (case when DSL_IM.PROD_SEG_NAME in ('Print','Facility Solutions','Packaging') then DSL_IM.PROD_SEG_NAME else
            		case when not(DSL_I.LEGACY_PROD_SEG_NAME in ('Print','Facility Solutions','Packaging')) then 'Other'
            	else DSL_I.LEGACY_PROD_SEG_NAME end end = 'Print' 
        	and (upper(DSL_IM.brand) like '%ENDURANCE%' 
    		or upper(DSL_IM.brand) like '%STARBRITE%' 
    		or upper(DSL_IM.brand) like '%SEVILLE%'
    		or upper(DSL_IM.brand) like '%ECONOSOURCE%'
    		or upper(DSL_IM.brand) like '%COMET%'
    		or upper(DSL_IM.brand) like '%POLIPRINT%'
    		or upper(DSL_IM.brand) like '%GALAXY%'
    		or upper(DSL_IM.brand) like '%NORDIC%'
   			or upper(DSL_IM.brand) like '%SHOWCASE%'
    		or upper(DSL_IM.brand) like '%VIV%'))) then 'Private Brand' else 'Branded' end as Private_Label,
	case when DSL_IM.mfg_item_nbr in ('','MFG') then 'Y' else 'N' end as MFG_Item,
	DSL_IM.item_cde AS item_mst_cde,
	DSL_IM.prod_desc,
	SUM(nvl(f.SLS,0)+nvl(f.CUST_TERM_STATED_CST,0)+nvl(f.CUST_TERM_CST_ADJ,0)+nvl(f.CUST_TERM_VARIANCE,0)+nvl(f.CUST_RBTE_CST,0)+nvl(f.LOCAL_RBTE_CST,0)) as NET_SALES,
	-1*sum(nvl(f.TRANS_COGS,0)+nvl(f.TRANS_COGS_ADJ,0)) AS trans_cogs,
	SUM(nvl(f.SLS,0)+nvl(f.CUST_TERM_STATED_CST,0)+nvl(f.CUST_TERM_CST_ADJ,0)+nvl(f.CUST_TERM_VARIANCE,0)+nvl(f.CUST_RBTE_CST,0)+nvl(f.LOCAL_RBTE_CST,0)+nvl(f.TRANS_COGS,0)+nvl(f.TRANS_COGS_ADJ,0)) as sell_profit,
	SUM(nvl(f.SLS,0)+nvl(f.TRANS_COGS,0)+nvl(f.TRANS_COGS_ADJ,0)+nvl(f.CUST_TERM_STATED_CST,0)+nvl(f.CUST_TERM_CST_ADJ,0)+nvl(f.CUST_TERM_VARIANCE,0)+nvl(f.LOCAL_RBTE_CST,0)+nvl(f.CUST_RBTE_CST,0)+nvl(f.VEND_TERM_CST,0)+nvl(f.INV_ADJ_CST,0)+nvl(f.VEND_INCENTIVE_CST,0)+nvl(f.VERIMILL_CST,0)+nvl(f.DEAD_EXCESS_CST,0)) AS ADJ_GROSS_PROFIT
FROM
	veritiv.edm.F_SLS_ITEM_VPR f INNER JOIN veritiv.edm.DS_CUST_SHIP_TO DSL_CST
		ON (f.CUST_SHIP_TO_KEY=DSL_CST.CUST_SHIP_TO_KEY
		and to_char(DSL_CST.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
	inner join edm.d_so so
		on (f.so_key = so.so_key)
    left outer join veritiv.edm.mktg_vert vert on
		(dsl_cst.cust_ship_to_key = vert.cust_ship_to_key
		and vert.process_time = (select max(process_time) from edm.mktg_vert))
	INNER JOIN veritiv.edm.DS_CUST DSL_C on
        (DSL_CST.CUST_KEY = DSL_C.CUST_KEY
        and to_char(DSL_C.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
	INNER JOIN veritiv.edm.DS_SLS_REP SR on    
        (DSL_CST.CUST_SHIP_TO_SLS_REP_KEY = SR.SLS_REP_KEY
        and to_char(sr.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
    INNER JOIN veritiv.edm.DS_EMP e on
        (SR.CURR_EMP_KEY = e.EMP_KEY
        and to_char(e.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
    INNER JOIN veritiv.edm.DS_ITEM  DSL_I 
		ON (f.ITEM_KEY=DSL_I.ITEM_KEY
		and to_char(DSL_I.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
    INNER JOIN veritiv.edm.DS_ITEM_MST  DSL_IM 
		ON DSL_I.ITEM_MST_KEY = DSL_IM.ITEM_MST_KEY
		and to_char(DSL_IM.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables)
    INNER JOIN veritiv.edm.D_ITEM_MST_V DIM
		ON DSL_I.ITEM_MST_KEY = DIM.ITEM_MST_KEY
		and to_char(DSL_IM.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables)
    INNER JOIN veritiv.edm.DS_VEND  DSL_V 
		ON (f.VEND_KEY=DSL_V.VEND_KEY
		and to_char(DSL_V.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
    INNER JOIN veritiv.edm.DS_VEND_MST  DSL_VM 
		ON (DSL_V.VEND_MST_KEY =DSL_VM.VEND_MST_KEY
		and to_char(DSL_VM.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
	INNER JOIN veritiv.edm.DS_LOC LOC 
		ON (f.LOC_KEY = LOC.LOC_KEY
		and to_char(LOC.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
	INNER JOIN veritiv.edm.DS_HFM_CC HFM
		ON (f.HFM_CC_KEY = HFM.HFM_CC_KEY
		and to_char(HFM.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
	INNER JOIN veritiv.edm.DS_NATL_ACCT DSL_CST_NA
        ON (DSL_C.NATL_ACCT_KEY=DSL_CST_NA.NATL_ACCT_KEY
        and to_char(DSL_CST_NA.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))         
    INNER JOIN veritiv.edm.DS_EMP e1 on
        (e1.EMP_KEY=DSL_CST_NA.NATL_ACCT_MGR_1_EMP_KEY
        and to_char(e1.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
    INNER JOIN veritiv.edm.DS_EMP e2 on
        (e2.EMP_KEY = DSL_CST_NA.NATL_ACCT_MGR_2_EMP_KEY
        and to_char(e2.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
    INNER JOIN veritiv.edm.DS_EMP e3 on
        (e3.EMP_KEY = DSL_CST_NA.DIRECTOR_EMP_KEY
        and to_char(e3.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))   
    INNER JOIN veritiv.edm.DS_B_SLS_REP_SLS_ORG B
        ON (f.hfm_cc_key = b.hfm_cc_key
        and DSL_CST.CUST_SHIP_TO_SLS_REP_KEY = b.SLS_REP_KEY
        and to_char(b.fiscal_mo,'YYYYMM') = (select snapdate from tmp_variables))
WHERE
    F.SYS_CDE in ('03','MA','NU','PC','X')
    and f.invc_date >= add_months(current_date, -18)
    and case when DSL_IM.PROD_SEG_NAME in ('Print','Facility Solutions','Packaging') then DSL_IM.PROD_SEG_NAME else
        case when not(DSL_I.LEGACY_PROD_SEG_NAME in ('Print','Facility Solutions','Packaging')) then 'Other'
        else DSL_I.LEGACY_PROD_SEG_NAME end
    end in ('Facility Solutions','Packaging','Print')
GROUP by    
	case when DSL_IM.PROD_SEG_NAME in ('Print','Facility Solutions','Packaging') then DSL_IM.PROD_SEG_NAME else
        case when not(DSL_I.LEGACY_PROD_SEG_NAME in ('Print','Facility Solutions','Packaging')) then 'Other'
        else DSL_I.LEGACY_PROD_SEG_NAME end
    end,
	DSL_IM.PROD_CAT_1_NAME,
    DSL_IM.PROD_CAT_2_NAME,
    DSL_IM.PROD_CAT_3_NAME,
    DSL_IM.PROD_CAT_1_NAME || ' | ' || DSL_IM.PROD_CAT_2_NAME || ' | ' || DSL_IM.PROD_CAT_3_NAME,
    DSL_IM.BRAND,
    dsl_vm.vend_name,
	DIM.active_ingr,
    DIM.adhesive,
    DIM.amphere,
    DIM.basic_mat,
    DIM.basic_size_desc,
    DIM.basis_wgt,
    DIM.box_style,
    DIM.brightness,
    DIM.burst_strength,
    DIM.caliper,
    DIM.cap,
    DIM.coating,
    DIM.coil_size,
    DIM.collation,
    DIM.color,
    DIM.conc,
    DIM.contains_wood_fiber,
    DIM.core_item,
    DIM.core_size,
    DIM.corr_test,
    DIM.cotton_content,
    DIM.cradle_to_cradle,
    DIM.ctn_skid_packing,
    DIM.dangerous_goods,
    DIM.density,
    DIM.dilu_ratio,
    DIM.dot,
    DIM.env_closure,
    DIM.env_dimensions,
    DIM.env_flap,
    DIM.env_seam,
    DIM.env_seam_desc,
    DIM.env_tint_color,
    DIM.env_type,
    DIM.env_window_pos,
    DIM.env_window_size,
    DIM.env_window_type,
    DIM.envr_levy,
    DIM.equip_sw,
    DIM.fair_trd,
    DIM.finish,
    DIM.flash_pnt,
    DIM.flute,
    DIM.fold,
    DIM.form,
    DIM.gauge,
    DIM.grade,
    DIM.green_seal,
    DIM.gsm,
    DIM.hp,
    DIM.image,
    DIM.inner_packing,
    DIM.innov,
    DIM.item_size,
    DIM.liner_mat,
    DIM.logical_del,
    DIM.m_wgt,
    DIM.mfg_item_nbr,
    DIM.model,
    DIM.nbr_of_parts,
    DIM.nbr_of_punch_holes,
    DIM.opacity,
    DIM.oper_rate,
    DIM.pack_meth,
    DIM.packing_grp,
    DIM.packing_qty,
    DIM.pallet_qty,
    DIM.perf_type,
    DIM.ph,
    DIM.ply,
    DIM.ppi,
    DIM.private_label_sw,
    DIM.prod,
    DIM.quality_type,
    DIM.rainforest_alliance,
    DIM.rcy,
    DIM.rcy_resin,
    DIM.rcyd_resin,
    DIM.ready_to_use,
    DIM.ream_marked,
    DIM.recy_post_cnsmr,
    DIM.recy_ttl_content,
    DIM.reuse,
    DIM.roll_dia,
    DIM.roll_wgt,
    DIM.rpa_100pct,
    DIM.rpm,
    DIM.rpt_grp,
    DIM.rspct_id,
    DIM.rspct_qual,
    DIM.scent,
    DIM.scoring,
    DIM.sds,
    DIM.series_nbr,
    DIM.sheffield_smoothness,
    DIM.shelf_life,
    DIM.sht_layout,
    DIM.shts_per_pkg_box,
    DIM.size_ind,
    DIM.sqn,
    DIM.storage_restriction,
    DIM.stretch_film_mfg_proc,
    DIM.subgrade,
    DIM.sustainable,
    DIM.tensile,
    DIM.test,
    DIM.thickness,
    DIM.trade_name,
    DIM.vol,
    DIM.voltage,
    DIM.web_create,
    DIM.wmk,
    case when f.dist_chnl_cde = 'D' then 'D' else 'W' end,
    case when (DSL_IM.PROD_CAT_4_NAME = 'Private Brand' 
    		or (case when DSL_IM.PROD_SEG_NAME in ('Print','Facility Solutions','Packaging') then DSL_IM.PROD_SEG_NAME else
            		case when not(DSL_I.LEGACY_PROD_SEG_NAME in ('Print','Facility Solutions','Packaging')) then 'Other'
            	else DSL_I.LEGACY_PROD_SEG_NAME end end = 'Print' 
        	and (upper(DSL_IM.brand) like '%ENDURANCE%' 
    		or upper(DSL_IM.brand) like '%STARBRITE%' 
    		or upper(DSL_IM.brand) like '%SEVILLE%'
    		or upper(DSL_IM.brand) like '%ECONOSOURCE%'
    		or upper(DSL_IM.brand) like '%COMET%'
    		or upper(DSL_IM.brand) like '%POLIPRINT%'
    		or upper(DSL_IM.brand) like '%GALAXY%'
    		or upper(DSL_IM.brand) like '%NORDIC%'
   			or upper(DSL_IM.brand) like '%SHOWCASE%'
    		or upper(DSL_IM.brand) like '%VIV%'))) then 'Private Brand' else 'Branded' end,
	case when DSL_IM.mfg_item_nbr in ('','MFG') then 'Y' else 'N' end,
	DSL_IM.item_cde,
	DSL_IM.prod_desc