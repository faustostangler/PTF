# PATHWAY
## STRUCTURE
General structure of multipage dash app

## DATA
### COMPANIES
load local
load online
merge
save local



## DASH PAGES


## PLOTLY


## PORTFOLIO


# LEGACY PATHWAY

* LOAD DATABASE

1. OK stk_get_composicao_acionaria
    nsd_get_nsd_content
        nsd_nsd_range
        nsd_nsd_dates
        nsd_get_nsd
    stk_synchronize_nsd_and_acoes
    sys_load_browser
    stk_get_acoes
    stk_fix_stock_values
        stk_stock_values_units
        stk_stock_values_magnitude_values

2. OK b3 - get companies info
    b3_get_companies
        b3_get_b3_tickers
            b3_get_ticker_keywords
    b3_get_company_info
        b3_get_company_full_info
            b3_get_company_extra_cnpj_info

3.1. cvm_get_databases_from_cvm
    # web
    cvm_get_database_filelist
        cvm_get_database_filelist_links
    cvm_download_csv_files_from_cvm_web
    cvm_group_dataframes_by_year
        cvm_group_dataframes_by_year_yearly
    cvm_clean_dataframe
    cvm_get_metadados
    cvm_get_categories
    # updated
    cvm_updated_rows
        cvm_extract_updated_rows

3.2. cvm_web: math_local + cvm_web->math_web -> math_local = merge(math_local, math_web)
    3.1. get cvm_new
    cvm_get_databases_from_cvm
        cvm_get_web_database
        cvm_get_database_filelist
        cvm_get_database_filelist_links
        cvm_get_metadados
        cvm_get_categories
    cvm_download_csv_files_from_cvm_web
    cvm_group_dataframes_by_year
    cvm_clean_dataframe
    cvm_calculate_math
    cvm_wrapper_apply
    cvm_math_calculations_adjustments

N. FUND
    Need intelacoes
        need intel_b3
            need b3_cvm
                need b3_grab (company)
                companies__local



* nsd
nsd_get_nsd_content
nsd_nsd_range
nsd_nsd_dates
sys_get_nsd

* SYS
sys_load_browser
sys_remaining_time
sys_clean_text
sys_gather_links

sys_read_or_create_dataframe
sys_save_and_pickle
sys_load_pkl
sys_save_pkl

sys_download_from_gcs
sys_upload_to_gcs