/******/
/* I M P O R T A N T E  A  L E E R*/
/******/
/******/
/* EN ESTA VERSIÓN del generador NO HAY EXCHANGE YA QUE TODO SE HACE EN EL ln_ */
/******/
/******/

declare

cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE"
    FROM
      MTDT_TC_SCENARIO
    WHERE TABLE_TYPE in ('D')
    --WHERE TABLE_TYPE in ('I')
    and trim(TABLE_NAME) in 
    (
    --'TRN_PDUSG_ACCOUNT_DIM'   -- Generada
    --,'TRN_SALES_CNL_DIM'      -- Generada
    -- 'TRN_CSTMR_DVC_DIM'     -- No se ha generado porque no esta especificado
    --, 'TRN_SALES_EMPE_DIM'    -- Generada
    --, 'TRN_REFER_GEO_AREA_DIM' -- Generada
    --, 'TRN_SALES_ROLE_DIM'      --No se ha generado porque no esta especificado
    --, 'TRN_INSEC_SRC_STM_DIM' -- Generado
    --'TRN_PDSVC_SVC_DIM'     -- Error
    --, 'TRN_CSTMR_CSTMR_DIM'     -- Generado
    --, 'TRN_PDUSG_PYMT_ENT_DIM'  -- Generado
    --'TRN_CSTMR_CSTMR_CLSS_DIM'  --No se ha generado porque no esta especificado SE HA CONVERTIDO EN CARGA UNICA
    --, 'TRN_CSTMR_CSTMR_GRP_DIM' -- Generado
    --, 'TRN_CSTMR_CSTMR_HLDG_DIM'  -- Generado
    -- 'TRN_CSTMR_IP_DIM'          -- Generado
    --, 'TRN_INSEC_SRC_OBJ_DIM'   -- Error en line 3370
    --, 'TRN_CSTMR_AR_CTC_PRFL_DIM'  -- Generado
    --'TRN_PDSVC_BANTRN_PDSVCTH_DIM'  --No se ha generado porque no esta especificado
    --'TRN_PDUSG_COLL_PYMT_MTH_DIM'   --No se ha generado porque no esta especificado
    --'TRN_NTWRK_NTW_CMPT_DIM'    --No se ha generado porque no esta especificado
    --, 'TRN_FINAN_ORG_DIM'   -- Generado
    --, 'TRN_PDSVC_PD_DIM' --Generado
    --'TRN_PDSVC_PD_OFRG_DIM' --No se ha generado porque no esta especificado
    --, 'TRN_SALES_RTLR_DIM'    -- Generado
    --, 'TRN_INSEC_BTCH_DIM'  -- Error file write error
    'AR_CTC_PRFL_DIM'
    --'DWD_GEO_AREA', 'DWD_ORG', 'DWD_AR_CTC_PRFL', 'DWD_CSTMR', 'DWD_CSTMR_GRP'
    --, 'DWD_CSTMR_HLDG', 'DWD_EMPE', 'DWD_CNL', 'DWD_SRC_STM', 'DWD_SRC_OBJ'
    --, 'DWD_RTLR', 'DWD_PD', 'DWD_BTCH', 'DWD_ACCOUNT', 'DWD_SUBSCRIBER'
    --, 'DWD_HH' -- falla porque hay reglas no definidas
    --, 'DWD_SVC', 'DWD_PRVN_SVC_AR', 'DWD_CSTMR_DOC_TP', 'DWD_IP'
    --, 'DWD_PYMT_ENT'
    --, 'DWD_INV_PRD', 'INV_PRD_DIM', 'DWD_INV_TP', 'DWD_INV_ITM', 'DWD_INV_ITM_TP'
    --, 'GEO_AREA_DIM' 
    --, 'AR_DIM' -- falla por un error raro
    --, 'ORG_DIM'
    --, 'AR_CTC_PRFL_DIM'
    --, 'CSTMR_GRP_DIM'
    --, 'CSTMR_HLDG_DIM'
    --, 'EMPE_DIM'
    --, 'CNL_DIM', 'SRC_STM_DIM', 'SRC_OBJ_DIM', 'RTLR_DIM', 'PD_DIM', 'BTCH_DIM', 'CSTMR_CLSS_DIM'
    --, 'INV_TP_DIM'
    --, 'INV_ITM_DIM'
    --, 'PRVN_SVC_AR_DIM', 'HH_DIM', 'PYMT_ENT_DIM'
    )
    --(
    --'SA_SOLAPES_DESC', 'SA_DESHACE_SOLAPES_DESC'
    --)
    --('NGD_PRIMARY_OFFER')
    order by
    TABLE_NAME;
    --and TRIM(TABLE_NAME) not in;
    --and 
    --TRIM(TABLE_NAME) in ('DMD_CAUSA_TERMINACION_LLAMADA', 'DMD_EMPRESA');
    --TRIM(TABLE_NAME) not in ('DMD_ESTADO_CELDA', 'DMD_FINALIZACION_LLAMADA', 'DMD_EMPRESA', 'DMD_POSICION_TRAZO_LLAMADA', 'DMD_TRONCAL', 'DMD_TIPO_REGISTRO', 'DMD_MSC');
    --and
    --TABLE_NAME = 'DMD_OPERADOR_VIRTUAL';
      
      
  cursor MTDT_SCENARIO (table_name_in IN VARCHAR2)
  is
    SELECT 
      trim(TABLE_NAME) "TABLE_NAME",
      trim(TABLE_TYPE) "TABLE_TYPE",
      TABLE_COLUMNS,
      trim(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      FILTER,
      INTERFACE_COLUMNS,
      trim(SCENARIO) "SCENARIO",
      trim(REINYECTION) "REINYECTION",
      DATE_CREATE,
      DATE_MODIFY
    FROM 
      MTDT_TC_SCENARIO
    WHERE
      TABLE_NAME = table_name_in
      ORDER BY SCENARIO;
      
  CURSOR MTDT_TC_DETAIL (table_name_in IN VARCHAR2, scenario_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_TC_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_TC_DETAIL.TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(MTDT_TC_DETAIL.TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(MTDT_TC_DETAIL.SCENARIO) "SCENARIO",
      TRIM(MTDT_TC_DETAIL.OUTER) "OUTER",
      MTDT_TC_DETAIL.SEVERIDAD,
      TRIM(MTDT_TC_DETAIL.TABLE_LKUP) "TABLE_LKUP",
      TRIM(MTDT_TC_DETAIL.TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(MTDT_TC_DETAIL.TABLE_LKUP_COND) "TABLE_LKUP_COND",      
      TRIM(MTDT_TC_DETAIL.IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      TRIM(MTDT_TC_DETAIL.LKUP_COM_RULE) "LKUP_COM_RULE",
      MTDT_TC_DETAIL.VALUE,
      MTDT_TC_DETAIL.RUL,
      MTDT_TC_DETAIL.DATE_CREATE,
      MTDT_TC_DETAIL.DATE_MODIFY
  FROM
      MTDT_TC_DETAIL, MTDT_MODELO_DETAIL
  WHERE
      trim(MTDT_TC_DETAIL.TABLE_NAME) = table_name_in and
      trim(MTDT_TC_DETAIL.SCENARIO) = scenario_in and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_NAME)) = UPPER(trim(MTDT_MODELO_DETAIL.TABLE_NAME)) and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_COLUMN)) = UPPER(trim(MTDT_MODELO_DETAIL.COLUMN_NAME))
  ORDER BY MTDT_MODELO_DETAIL.POSITION ASC;
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      CONCEPT_NAME,
      SOURCE,
      COLUMNA,
      KEY,
      TYPE,
      LENGTH,
      NULABLE,
      POSITION
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      CONCEPT_NAME = concep_name_in and
      SOURCE = source_in
    ORDER BY POSITION;
      
  CURSOR MTDT_TC_LOOKUP (table_name_in IN VARCHAR2)
  IS
    SELECT
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TRIM(TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      (trim(RUL) = 'LKUP' or trim(RUL) = 'LKUPC') and
      trim(TABLE_NAME) = table_name_in;

  CURSOR MTDT_TC_LKUPD (table_name_in IN VARCHAR2)
  IS
    SELECT
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TRIM(TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      trim(RUL) = 'LKUPD' and
      trim(TABLE_NAME) = table_name_in;


--CURSOR MTDT_TC_FUNCTION (table_name_in IN VARCHAR2)
--  IS
--    SELECT
--      DISTINCT
--      TRIM(TABLE_LKUP) "TABLE_LKUP",
--      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
--      TABLE_LKUP_COND "TABLE_LKUP_COND",
--      IE_COLUMN_LKUP "IE_COLUMN_LKUP",
--      TRIM("VALUE") "VALUE"
--    FROM
--      MTDT_TC_DETAIL
--  WHERE
--      trim(RUL) = 'FUNCTION' and
--      TRIM(TABLE_NAME) = table_name_in;
  
      
  reg_tabla MTDT_TABLA%rowtype;
      
  reg_scenario MTDT_SCENARIO%rowtype;
  
  reg_detail MTDT_TC_DETAIL%rowtype;
  
  reg_interface_detail dtd_interfaz_detail%rowtype;
  
  reg_lookup MTDT_TC_LOOKUP%rowtype;
  reg_lookupd MTDT_TC_LKUPD%rowtype;
  
  --reg_function MTDT_TC_FUNCTION%rowtype;
  
  

  v_nombre_particion VARCHAR2(30);
  v_interface_summary MTDT_INTERFACE_SUMMARY%ROWTYPE;
  v_existe_escenario_HF varchar2(1):='N';   /* (20151113) Angel Ruiz. NF.: REINYECCION */ 
  v_existe_reinyeccion varchar2(1):='N';  /* (20151120) Angel Ruiz. NF: Algun escenario posee el FLAG R activo */
  
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  TYPE list_strings  IS TABLE OF VARCHAR(500);
  type lista_tablas_from is table of varchar(2000);
  type lista_condi_where is table of varchar(500);

  l_FROM                                      lista_tablas_from := lista_tablas_from();
  l_FROM_solo_tablas                               lista_tablas_from := lista_tablas_from();
  l_WHERE                                   lista_condi_where := lista_condi_where();
  l_WHERE_ON_clause                         lista_condi_where := lista_condi_where();

  
  lista_pk                               list_columns_primary := list_columns_primary (); 
  where_interface_columns                list_strings := list_strings();
  where_table_columns                      list_strings := list_strings();
  lista_scenarios_presentes                                    list_strings := list_strings();
  lista_variables_rownumber              list_strings := list_strings();
  lista_lkup                                    list_strings := list_strings();
  lista_lkupd                                  list_strings := list_strings();
  lista_tablas_base                                  list_strings := list_strings();
  nombre_tabla_T                        VARCHAR2(30);

  
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(2000);
  prototipo_fun                             VARCHAR2(500);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_hist                         UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(60);
  nombre_fich_pkg                      VARCHAR2(60);
  nombre_fich_hist                      VARCHAR2(60);
  nombre_fich_exchange            VARCHAR2(60);
  nombre_tabla_reducido           VARCHAR2(30);
  campo_filter                                VARCHAR2(500);
  nombre_proceso                        VARCHAR2(80);
  nombre_tabla_base_redu        VARCHAR2(30);
  nombre_tabla_base_sp_redu  VARCHAR2(30);
  num_sce_integra number(2) := 0;
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR(60);
  PAIS                              VARCHAR2(60);
  OWNER_TC                              VARCHAR(60);
  PREFIJO_DM                            VARCHAR2(60);
  ESQUEMA_DM  VARCHAR2(60);
  BD_SID VARCHAR2(60);
  NAME_DM_FULL VARCHAR2(60);
  nombre_funcion                   VARCHAR2(100);
  v_encontrado											VARCHAR2(1):= 'N';
  v_contador                        PLS_INTEGER:=0;
  v_concept_name                MTDT_INTERFACE_SUMMARY.CONCEPT_NAME%TYPE;
  TABLESPACE_SA                  VARCHAR2(60);
  v_num_meses                          VARCHAR2(2);
  v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;

  v_hay_look_up                           VARCHAR2(1):='N';
  v_bandera                                   VARCHAR2(1):='S';
  v_nombre_tabla_agr                VARCHAR2(30):='No Existe';
  v_nombre_tabla_agr_redu           VARCHAR2(30):='No Existe';
  v_nombre_proceso_agr              VARCHAR2(30);
  nombre_tabla_T_agr                VARCHAR2(30);
  v_existen_retrasados              VARCHAR2(1) := 'N';
  v_numero_indices                  PLS_INTEGER:=0;
  v_MULTIPLICADOR_PROC                   VARCHAR2(60);
  v_alias_dim_table_base_name             VARCHAR2(40);
  v_hay_regla_seq                   BOOLEAN:=false; /*(20170110) Angel Ruiz. NF: reglas SEQ */
  v_nombre_seq                      VARCHAR2(50); /*(20170110) Angel Ruiz. NF: reglas SEQ */
  v_nombre_campo_seq                VARCHAR2(50); /*(20170110) Angel Ruiz. NF: reglas SEQ */
  v_num_sce_NUEVOS              PLS_INTEGER:=0; /* (20170315) Angel Ruiz. NF: numeros de escenarios del mismo tipo NUEVO*/
  v_num_sce_EXISTENTES              PLS_INTEGER:=0; /* (20170315) Angel Ruiz. NF: numeros de escenarios del mismo tipo NUEVO*/
  v_num_sce_HISTORICOS              PLS_INTEGER:=0; /* (20170315) Angel Ruiz. NF: numeros de escenarios del mismo tipo NUEVO*/
  v_variables_sesion BOOLEAN;
  v_row_number VARCHAR2(70);
  v_encontrado_var_row_number BOOLEAN;
  V_EXISTE_ESCENARIO_I BOOLEAN:=false;  /* (20200117)*/
  v_TABLE_BASE_NAME_SCENARIO_I reg_scenario.TABLE_BASE_NAME%type; /* (20200117)*/
  v_TABLE_NAME_SCENARIO_I reg_scenario.TABLE_NAME%type; /* (20200117)*/
  
  
  
  function procesa_condicion_lookup (cadena_in in varchar2, v_alias_in varchar2 := NULL) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  cadena_resul varchar(1000);
  begin
    dbms_output.put_line ('Entro en procesa_condicion_lookup');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco el signo = */
      sustituto := ' (+)= ';
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, '=', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length ('='));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + (length (' (+)= '));
        dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
        pos := pos_ant;
      end loop;
      /* Busco LA COMILLA */
      --pos := 0;
      --posicion_ant := 0;
      --sustituto := '''''';
      --loop
        --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
        --pos := instr(cadena_resul, '''', pos+1);
        --exit when pos = 0;
        --dbms_output.put_line ('Pos es mayor que 0');
        --dbms_output.put_line ('Primer valor de Pos: ' || pos);
        --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        --dbms_output.put_line ('La cabeza es: ' || cabeza);
        --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        --cola := substr(cadena_resul, pos + length (''''));
        --dbms_output.put_line ('La cola es: ' || cola);
        --cadena_resul := cabeza || sustituto || cola;
        --pos_ant := pos + length ('''''');
        --pos := pos_ant;
      --end loop;
      /* Sustituyo el nombre de Tabla generico por el nombre que le paso como parametro */
      if (v_alias_in is not null) then
        /* Existe un alias que sustituir */
        pos := 0;
        posicion_ant := 0;
        sustituto := v_alias_in;
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup para sustituir el ALIAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#TABLE_OWNER#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#TABLE_OWNER#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length ('''''');
          --pos := pos_ant;
        end loop;

      end if;
    end if;
    
    return cadena_resul;
  end;

  function proceso_campo_value (cadena_in in varchar2, alias_in in varchar) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  v_pos_ini_corchete_ab PLS_integer;
  v_pos_fin_corchete_ce PLS_integer;
  v_cadena_a_buscar varchar2(100);
  cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      v_pos_ini_corchete_ab := instr(cadena_in, '[');
      v_pos_fin_corchete_ce := instr(cadena_in, ']');
      v_cadena_a_buscar := substr(cadena_in, v_pos_ini_corchete_ab, (v_pos_fin_corchete_ce - v_pos_ini_corchete_ab) + 1);
      sustituto := alias_in || '.' || substr (cadena_in, v_pos_ini_corchete_ab + 1, (v_pos_fin_corchete_ce - v_pos_ini_corchete_ab) - 1);
      loop
        pos := instr(cadena_resul, v_cadena_a_buscar, pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (v_cadena_a_buscar));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
      end loop;
    end if;
    return cadena_resul;
  end;

  function procesa_COM_RULE_lookup (cadena_in in varchar2, v_alias_in varchar2 := NULL) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  cadena_resul varchar(1000);
  begin
    dbms_output.put_line ('Entro en procesa_COM_RULE_lookup');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco LA COMILLA */
      pos := 0;
      posicion_ant := 0;
      sustituto := '''''';
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, '''', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (''''));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + length ('''''');
        pos := pos_ant;
      end loop;
      /* Sustituyo el nombre de Tabla generico por el nombre que le paso como parametro */
      if (v_alias_in is not null) then
        /* Existe un alias que sustituir */
        pos := 0;
        posicion_ant := 0;
        sustituto := v_alias_in;
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup para sustituir el ALIAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#TABLE_OWNER#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#TABLE_OWNER#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length ('''''');
          --pos := pos_ant;
        end loop;

      end if;
    end if;
    
    return cadena_resul;
  end;

  
  function cambio_puntoYcoma_por_coma (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(1000);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      /* Busco VAR_FUN_NAME_LOOKUP */
      sustituto := ',';
      loop
        dbms_output.put_line ('Entro en el LOOP de cambio_puntoYcoma_por_coma. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, ';', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (';'));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
        --pos := pos_ant;
      end loop;
    end if;  
    return cadena_resul;
  end cambio_puntoYcoma_por_coma;
  
  function split_string_punto_coma ( cadena_in in varchar2) return list_strings
  is
  lon_cadena integer;
  elemento varchar2 (100);
  pos integer;
  pos_ant integer;
  lista_elementos                                      list_strings := list_strings (); 
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    if lon_cadena > 0 then
      loop
              dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_in);
              if pos < lon_cadena then
                pos := instr(cadena_in, ';', pos+1);
              else
                pos := 0;
              end if;
              dbms_output.put_line ('Primer valor de Pos: ' || pos);
              if pos > 0 then
                dbms_output.put_line ('Pos es mayor que 0');
                dbms_output.put_line ('Pos es:' || pos);
                dbms_output.put_line ('Pos_ant es:' || pos_ant);
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant) -1);
                dbms_output.put_line ('El elemento es: ' || elemento);
                lista_elementos.EXTEND;
                lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (elemento)));
                pos_ant := pos;
              end if;
       exit when pos = 0;
      end loop;
      lista_elementos.EXTEND;
      lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena))));
      dbms_output.put_line ('El ultimo elemento es: ' || UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena)))));
    end if;
    dbms_output.put_line('Antes del return');
    return lista_elementos;
  end split_string_punto_coma;

  function split_string_coma ( cadena_in in varchar2) return list_strings
  is
  lon_cadena integer;
  elemento varchar2 (50);
  pos integer;
  pos_ant integer;
  lista_elementos                                      list_strings := list_strings (); 
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    if lon_cadena > 0 then
      loop
              dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_in);
              if pos < lon_cadena then
                pos := instr(cadena_in, ',', pos+1);
              else
                pos := 0;
              end if;
              dbms_output.put_line ('Primer valor de Pos: ' || pos);
              if pos > 0 then
                dbms_output.put_line ('Pos es mayor que 0');
                dbms_output.put_line ('Pos es:' || pos);
                dbms_output.put_line ('Pos_ant es:' || pos_ant);
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant) -1);
                dbms_output.put_line ('El elemento es: ' || elemento);
                lista_elementos.EXTEND;
                lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (elemento)));
                pos_ant := pos;
              end if;
       exit when pos = 0;
      end loop;
      lista_elementos.EXTEND;
      lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena))));
      dbms_output.put_line ('El ultimo elemento es: ' || UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena)))));
    end if;
    return lista_elementos;
  end split_string_coma;

  /* (20161122) Angel Ruiz. Transforma el campo de la funcion poniendole el alias*/
  function transformo_funcion (cadena_in in varchar2, alias_in in varchar2) return varchar2
  is
    v_campo varchar2(200);
    v_cadena_temp varchar2(200);
    v_cadena_result varchar2(200);
  begin
    /* Detecto si existen funciones SQL en el campo */
    if (regexp_instr(cadena_in, '[Nn][Vv][Ll]') > 0) then
      /* Se trata de que el campo de join posee la funcion NVL */
      if (regexp_instr(cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z_]+ *,') > 0) then
        /* trasformamos el primer operador del NVL */
        v_cadena_temp := regexp_replace (cadena_in, ' *([Nn][Vv][Ll]) *\( *([A-Za-z_]+) *,', '\1(' || alias_in || '.' || '\2' || ',');
        /* trasformamos el segundo operador del NVL, en caso de que sea un campo y no un literal */
        if (regexp_instr(v_cadena_temp, '[Cc][Uu][Rr][Rr][Ee][Nn][Tt]') = 0) then
          /* si aunque no es un literal si es un current_date entonces no se anyade alias */
          v_cadena_temp := regexp_replace (v_cadena_temp, ', *([A-Za-z_]+) *\)', ', ' || alias_in || '.' || '\1' || ')');
        end if;
        v_cadena_result := v_cadena_temp; /* retorno el resultado */
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
      /* Se trata de que el campo de join posee la funcion UPPER */
      if (regexp_instr(cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)') > 0) then
        v_cadena_temp := regexp_replace (cadena_in, ' *([Uu][Pp][Pp][Ee][Rr]) *\( *([A-Za-z_]+) *\)', '\1(' || alias_in || '.' || '\2' || ')');
        v_cadena_result := v_cadena_temp;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
      /* Se trata de que el campo de join posee la funcion REPLACE */
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *') > 0) then
        v_cadena_temp := regexp_replace (cadena_in, ' *([Rr][Ee][Pp][Ll][Aa][Cc][Ee]) *\( *([A-Za-z_]+) *,', '\1(' || alias_in || '.' || '\2,');
        v_cadena_result := v_cadena_temp;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Ii][Ff] *\(') > 0) then
      /* Se trata de que el campo de join posee la funcion IF */
        v_cadena_temp := regexp_replace (cadena_in, ' *([Ii][Ff]) *\( *([A-Za-z_\.]+) *', ' \1(' || alias_in || '.' || '\2');
        v_cadena_temp := regexp_replace (v_cadena_temp, ', *([A-Za-z_\.]+) *,', ', ' || alias_in || '.' || '\1,');
        v_cadena_temp := regexp_replace (v_cadena_temp, ', *([A-Za-z_\.'']+) *\)', ', ' || alias_in || '.' || '\1' || ')');
        v_cadena_result := v_cadena_temp;
    else
      v_cadena_result := alias_in || '.' || cadena_in;
    end if;
    return v_cadena_result;
  end;



  /* (20161117) Angel Ruiz. Extrae el campo de una cedena donde hay funciones*/
  function extrae_campo (cadena_in in varchar2) return varchar2
  is
    v_campo varchar2(200);
    v_cadena_temp varchar2(200);
    v_cadena_result varchar2(200);
  begin
    /* Detecto si existen funciones SQL en el campo */
    if (regexp_instr(cadena_in, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 ) then
      if (regexp_instr(cadena_in, ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\( *[A-Za-z_]+ *,') > 0) then
        /* Se trata de un decode normal y corriente */
        v_cadena_temp := regexp_substr (cadena_in, ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\( *[A-Za-z_]+ *,'); 
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Nn][Vv][Ll]') > 0) then
      /* Se trata de que el campo de join posee la funcion NVL */
      if (regexp_instr(cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z_]+ *,') > 0) then
        v_cadena_temp := regexp_substr (cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z_]+ *,');
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
      /* Se trata de que el campo de join posee la funcion UPPER */
      if (regexp_instr(cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)') > 0) then
        v_cadena_temp := regexp_substr (cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)');
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
      /* Se trata de que el campo de join posee la funcion REPLACE */
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *') > 0) then
        v_cadena_temp := regexp_substr (cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *,');
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Ii][Ff] *\(') > 0) then
      v_cadena_temp := regexp_substr (cadena_in,' *[Ii][Ff] *\( *[A-Za-z_\.]+ *');
      dbms_output.put_line('Estoy en el extrae_campo. Entro en el IF. El primer substr es : ' || v_cadena_temp);
      
      if (instr(v_cadena_temp, '.') > 0 )then
        /* El campo viene calificado */
        v_campo := regexp_substr (v_cadena_temp,'\.[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_campo := substr(v_campo, 2); /* ME QUITO EL PUNTO */
        v_cadena_result := v_campo;
      else
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      end if;
    else
      v_cadena_result := cadena_in;
    end if;
    return v_cadena_result;
  end;
  
  function extrae_campo_decode (cadena_in in varchar2) return varchar2
  is
    lista_elementos list_strings := list_strings (); 

  begin
    lista_elementos := split_string_coma(cadena_in);
    return lista_elementos(lista_elementos.count - 1);
  
  end extrae_campo_decode;

  function extrae_campo_decode_sin_tabla (cadena_in in varchar2) return varchar2
  is
    lista_elementos list_strings := list_strings (); 

  begin
    lista_elementos := split_string_coma(cadena_in);
    if instr(lista_elementos((lista_elementos.count) - 1), '.') > 0 then
      return substr(lista_elementos(lista_elementos.count - 1), instr(lista_elementos((lista_elementos.count) - 1), '.') + 1);
    else
      return lista_elementos(lista_elementos.count - 1);
    end if;
  end extrae_campo_decode_sin_tabla;
  
  /* (20161118) Angel Ruiz. Nueva version de la funcion que transforma los decodes*/
  function transformo_decode(cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
  is
    parte_1 varchar2(100);
    parte_2 varchar2(100);
    parte_3 varchar2(100);
    parte_4 varchar2(100);
    decode_out varchar2(500);
    lista_elementos list_strings := list_strings ();
    v_cadena_temp VARCHAR2(500):='';

  begin
    /* Ejemplo de Decode que analizo DECODE (ID_FUENTE,'SER', ID_CANAL,'1') */
    if (regexp_instr(cadena_in, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
      lista_elementos := split_string_coma(cadena_in);
      if (lista_elementos.COUNT > 0) then
        FOR indx IN lista_elementos.FIRST .. lista_elementos.LAST
        LOOP
          if (indx = 1) then
            /* Se trata del primer elemento: DECODE (ID_FUENTE */
            v_cadena_temp := trim(regexp_substr(lista_elementos(indx), ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\('));  /* Me quedo con DECODE ( */
            parte_1 := trim(substr(lista_elementos(indx), instr(lista_elementos(indx), '(') +1)); /* DETECTO EL ( */
            if (instr(parte_1, '.') = 0) then /* (20170221) Angel Ruiz. BUG. Si no viene ALIAS */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.' || '\1' || ' (+)'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              else
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.' || '\1'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              end if;
            else /* (20170221) Angel Ruiz. BUG. Si viene ALIAS */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_.]+) *', '\1' || ' (+)'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              else
                v_cadena_temp := v_cadena_temp || parte_1; /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              end if;
            end if;
            v_cadena_temp := v_cadena_temp || ', '; /* Tengo LA CADENA: "DECODE (alias_in.ID_FUENTE (+), " */
          elsif (indx = lista_elementos.LAST) then
            /* Se trata del ultimo elemento '1') */
            if (instr(lista_elementos(indx), '''') = 0) then
              /* Se trata de un elemnto tipo ID_CANAL pero situado al final del DECODE */
              if (instr(lista_elementos(indx), '.') = 0) then /* (20170221) Angel Ruiz. BUG. Si no viene ALIAS */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.' || '\1' || ' (+))'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                else
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.' || '\1' || ')'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                end if;
              else  /* (20170221) Angel Ruiz. BUG. Si viene ALIAS */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_.]+) *\)', '\1' || ' (+))'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                else
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', '\1' || ')'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                end if;
              end if;
            else
              /* Se trata de un elemento literal situado como ultimo elemento del decode, tipo '1' */
              /* Le ponemos doble comillas ya que estamos generando una query deinamica */
              v_cadena_temp := v_cadena_temp || lista_elementos(indx) || ')';
            end if;
          else
            /* Se trata del resto de elmentos 'SER', ID_CANAL*/
            if (instr(lista_elementos(indx), '''') = 0) then
              /* Se trata de un elemento que no es un literal, tipo ID_CANAL */
              if (instr(lista_elementos(indx), '.') = 0) then /* (20170221) Angel Ruiz. BUG. Si no viene ALIAS */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *', alias_in || '.' || '\1' || ' (+)');
                else
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *', alias_in || '.' || '\1');
                end if;
              else
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_.]+) *', '\1' || ' (+)');
                else
                  v_cadena_temp := v_cadena_temp || lista_elementos(indx);
                end if;
              end if;
              v_cadena_temp := v_cadena_temp || ', '; /* Tengo LA CADENA: "DECODE (alias_in.ID_FUENTE (+), ..., alias_in.ID_CANAL, ... "*/
            else
              /* Se trata de un elemento que es un literal, tipo 'SER' */
              /* Le ponemos doble comillas ya que estamos generando una query deinamica */
              v_cadena_temp := v_cadena_temp || lista_elementos(indx) || ', ';
            end if; 
          end if;
        END LOOP;
      end if;
    else
      if (outer_in = 1) then
        v_cadena_temp := alias_in || '.' || cadena_in || ' (+)';
      else
        v_cadena_temp := alias_in || '.' || cadena_in;
      end if;
    end if;
    return v_cadena_temp;
  end;
  
  
  function proc_campo_value_condicion (cadena_in in varchar2, nombre_funcion_lookup in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(1000);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      /* Busco VAR_FUN_NAME_LOOKUP */
      sustituto := nombre_funcion_lookup;
      loop
        dbms_output.put_line ('Entro en el LOOP de proc_campo_value_condicion. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, 'VAR_FUN_NAME_LOOKUP', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length ('VAR_FUN_NAME_LOOKUP'));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
        --pos := pos_ant;
      end loop;
    end if;  
    return cadena_resul;
  end;
  function procesa_campo_filter (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (25000);
    sustituto              varchar2(100);
    cola                      varchar2(25000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(25000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco VAR_FCH_CARGA */
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_CARGA#', 'fch_datos_in');
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_DATOS#', 'fch_datos_in');
        /* Busco VAR_FCH_INICIO */
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_INICIO#', 'var_fch_inicio');

        /* (20170522) Angel Ruiz. Incñuyo una nueva variable VAR_FCH_INI_MES que contiene el primer dia del mes */
        /* segun la fecha de carga */
        --cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_INI_MES#', ' date_format(fch_datos_in, ''yyyy-MM-01'') ');
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_INI_MES#', ' to_date(fch_datos_in, ''YYYYMM01'') ');
        
        /* Busco VAR_PROFUNDIDAD_BAJAS */
        cadena_resul := regexp_replace(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', ' 90 ');
        
        /* Busco OWNER_DM */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_DM#', ESQUEMA_DM);
        /* Busco OWNER_SA */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_SA#', OWNER_SA);
        /* Busco OWNER_T */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_T#', OWNER_T);
        
        /* Busco OWNER_MTDT */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_MTDT#', OWNER_MTDT);
        /* Busco VAR_MARGEN_COMISION */
        cadena_resul := regexp_replace(cadena_resul, '#VAR_MARGEN_COMISION#', ' 0.3 ');
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FIN_DEFAULT#', ' ''9999-12-31'' ');

      end if;
      return cadena_resul;
    end;
  
/************/
/*************/
  function procesa_campo_filter_dinam (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (3000);
    sustituto              varchar2(100);
    cola                      varchar2(3000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(3000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco VAR_FCH_CARGA */
        --sustituto := ' to_date ('''''' ||  fch_datos_in || '''''', ''''yyyymmdd'''') ';
        sustituto := ' to_date ('''''' ||  #VAR_FCH_DATOS# || '''''', ''''yyyymmdd'''') '; /* (20161208) Angel Ruiz */
        loop
          dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_FCH_CARGA', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_FCH_CARGA'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
          --pos := pos_ant;
        end loop;
        /* Busco VAR_PROFUNDIDAD_BAJAS */
        sustituto := ' 90 ';  /* Temporalmente pongo 90 dias */
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de VAR_PROFUNDIDAD_BAJAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_PROFUNDIDAD_BAJAS'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_DM */
        sustituto := OWNER_DM;
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_DM#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_DM#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_SA */
        sustituto := OWNER_SA; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_SA#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_SA#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_T */
        sustituto := OWNER_T; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_T#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_T#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_MTDT */
        sustituto := OWNER_MTDT; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_MTDT#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_MTDT#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
      end if;
      return cadena_resul;
    end;


  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (2000);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(500);
    cad_seg         VARCHAR(500);
    cadena            VARCHAR(500);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(500);
    condicion_pro         VARCHAR2(200);
    posicion_ant    PLS_integer;
    pos                    PLS_integer;
    cadena_resul  VARCHAR(1000);
    sustituto           VARCHAR(30);
    lon_cadena     PLS_integer;
    cabeza             VARCHAR2(500);
    cola                   VARCHAR2(500);
    pos_ant            PLS_integer;
    v_encontrado  VARCHAR2(1);
    v_alias             VARCHAR2(40);
    table_columns_lkup  list_strings := list_strings();
    ie_column_lkup    list_strings := list_strings();
    tipo_columna  VARCHAR2(30);
    mitabla_look_up VARCHAR2(2000);
    l_registro          MTDT_INTERFACE_DETAIL%rowtype;
    l_registro1         MTDT_MODELO_DETAIL%rowtype;
    l_registro2         v_MTDT_CAMPOS_DETAIL%rowtype;
    v_value VARCHAR(200);
    nombre_campo  VARCHAR2(100);
    v_alias_incluido PLS_Integer:=0;
    v_alias_incluido_table_base PLS_Integer:=0;
    v_es_query_table_base PLS_Integer:=0;
    v_table_base_name varchar2(100);
    v_alias_table_base_name varchar2(100);
    mi_tabla_base_name VARCHAR2(50);
    mi_tabla_base_name_alias VARCHAR2(50);
    v_tipo_campo  VARCHAR2(30);
    v_alias_table_base  VARCHAR2(40);
    v_tabla_base_name VARCHAR2(4000);
    constante         VARCHAR2(500);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_paquete                    VARCHAR2(40);
    v_nombre_tabla_reducido         VARCHAR2(40);
    v_IE_COLUMN_LKUP              VARCHAR(800);
    v_LKUP_COM_RULE               VARCHAR2(1000);
    v_prototipo_func                        VARCHAR2(500);
    v_table_look_up varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_reg_table_lkup varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_alias_table_look_up varchar2(10000);  /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_existe_valor  BOOLEAN;
    --v_table_lkup_prima varchar2(10000);  /*(20170109) Angel Ruiz. BUG.*/
    v_no_se_generara_case             BOOLEAN:=false;
    v_temporal varchar2(500);


    
    
  begin
      dbms_output.put_line ('REGLA RUL: #' || reg_detalle_in.RUL || '#');
      case trim(reg_detalle_in.RUL)
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        --valor_retorno := '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;
        valor_retorno := '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;
      when 'LKUPC' then
        /* (20150626) Angel Ruiz.  Se trata de hacer el LOOK UP con la tabla dimension de manera condicional */
        l_FROM.extend;
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0 or instr (reg_detalle_in.TABLE_LKUP,'select') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$')) then
          /* (20160629) Angel Ruiz. NF: Se aceptan tablas de LKUP que son SELECT que ademas tienen un ALIAS */
            v_alias := trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            mitabla_look_up := reg_detalle_in.TABLE_LKUP;
            v_alias_incluido := 1;
          else
            v_alias := 'LKUP_' || l_FROM.count;
            mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
            v_alias_incluido := 0;
          end if;
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else
        
          /* (20161111) Angel Ruiz. NF. Puede haber ALIAS EN LA TABLA DE LOOKUP */
          dbms_output.put_line('Dentro del ELSE del SELECT');
          /* (20160401) Detectamos si la tabla de LookUp posee Alias */
          v_reg_table_lkup := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
          if (REGEXP_LIKE(trim(v_reg_table_lkup), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_incluido := 1;
            dbms_output.put_line('La tabla de LKUP posee alias');
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), '^+[a-zA-Z_0-9\.#&]+ '));
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_table_look_up := v_table_look_up;
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              --v_table_look_up := OWNER_DM || '.' || v_table_look_up;
              v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
            v_alias := v_alias_table_look_up;
          else    /* La tabla de LKUP no posee Alias */
            v_alias_incluido := 0;
            dbms_output.put_line('La tabla de LKUP no posee alias');
            --v_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := v_reg_table_lkup;            
            --if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              dbms_output.put_line('La tabla de LKUP esta calificado');
              --v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9]+'), 2);
              v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              --v_table_look_up := OWNER_DM || '.' || v_table_look_up;
              v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
          end if;
          
          /* (20161111) Angel Ruiz. NF FIN. Puede haber ALIAS EN LA TABLA DE LOOKUP */
        
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        table_columns_lkup := split_string_punto_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_punto_coma (reg_detalle_in.IE_COLUMN_LKUP);
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          if (instr (cadena, 'SI') > 0) then
          /* (20160705) Se trata de un condicion del tipo SI ...THEN ...ELSE */
            pos_del_si := instr(cadena, 'SI');
            pos_del_then := instr(cadena, 'THEN');
            pos_del_else := instr(cadena, 'ELSE');
            pos_del_end := instr(cadena, 'END');  
            condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
            condicion_pro := procesa_COM_RULE_lookup(condicion);
            constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
            valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
          else
            /* (20160705) Angel Ruiz. se trata de una condicion normal */
            /*Puede ocurrir que en el campo VALUE de la llamada a LOOKUP se use la variable VAR_FCH_CARGA */
            v_IE_COLUMN_LKUP := procesa_campo_filter (reg_detalle_in.IE_COLUMN_LKUP);
            /* (20160115) ANGEL RUIZ. Puede ocurrir que en el campo LKUP_COM_RULE se use la variable VAR_FCH_CARGA */
            v_LKUP_COM_RULE := procesa_campo_filter (reg_detalle_in.LKUP_COM_RULE);
            /* (20160705) Angel Ruiz. Sustituyo VAR_FUN_NAME_LOOKUP por lo que aparece en VALUE */
            valor_retorno := proc_campo_value_condicion (v_LKUP_COM_RULE, reg_detalle_in.TABLE_LKUP || '.' || reg_detalle_in.value);
          end if;
        end if;
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            --SELECT * INTO l_registro2
            --FROM MTDT_INTERFACE_DETAIL
            --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
            --UPPER(TRIM(COLUMNA)) = UPPER(TRIM(ie_column_lkup(indx)));
            SELECT * INTO l_registro2
            FROM v_MTDT_CAMPOS_DETAIL
            WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
            UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
            if (l_WHERE.count = 1) then
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  l_WHERE(l_WHERE.last) :=  'COALESCE(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            else
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE.count = 1) then
              l_WHERE(l_WHERE.last) := v_IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || v_IE_COLUMN_LKUP || ' < ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            else
              l_WHERE(l_WHERE.last) := ' AND ' || v_IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || v_IE_COLUMN_LKUP || ' < ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            --SELECT * INTO l_registro
            --FROM MTDT_INTERFACE_DETAIL
            --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
            --UPPER(TRIM(COLUMNA)) = UPPER(reg_detalle_in.IE_COLUMN_LKUP);
            SELECT * INTO l_registro2
            FROM v_MTDT_CAMPOS_DETAIL
            WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
            UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.IE_COLUMN_LKUP);
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  l_WHERE(l_WHERE.last) :=  ' AND COALESCE(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE.extend;
          l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
        end if;
        
      when 'LKUP' then
        /* Se trata de hacer el LOOK UP con la tabla dimension */
        /* (20150126) Angel Ruiz. Primero recojo la tabla del modelo con la que se hace LookUp. NO puede ser tablas T_* sino su equivalesnte del modelo */
        dbms_output.put_line('ESTOY EN EL LOOKUP. Al principio');
        l_FROM.extend;
        l_FROM_solo_tablas.extend;  /*(20161202) Angel Ruiz */
        /* (20150130) Angel Ruiz */
        /* Nueva incidencia. */
        if (regexp_instr (reg_detalle_in.TABLE_LKUP,'[Ss][Ee][Ll][Ee][Cc][Tt] ') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$')) then
          /* (20160629) Angel Ruiz. NF: Se aceptan tablas de LKUP que son SELECT que ademas tienen un ALIAS */
            v_alias := trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            mitabla_look_up := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
            v_alias_incluido := 1;
          else
            v_alias := 'LKUP_' || l_FROM.count;
            --mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
            mitabla_look_up := '(' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ') LKUP_' || l_FROM.count;
            v_alias_incluido := 0;
          end if;
          --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
          l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
          /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
          --l_FROM (l_FROM.last) := ' LEFT OUTER JOIN ' || mitabla_look_up || ' ';
          if (reg_detalle_in.OUTER is null) then
            l_FROM (l_FROM.last) := 'INNER JOIN ' || mitabla_look_up || ' ';
          else
            l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
          end if;
        else
          /* (20161111) Angel Ruiz. NF. Puede haber ALIAS EN LA TABLA DE LOOKUP */
          dbms_output.put_line('Dentro del ELSE del SELECT');
          /* (20160401) Detectamos si la tabla de LookUp posee Alias */
          v_reg_table_lkup := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
          if (REGEXP_LIKE(trim(v_reg_table_lkup), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_incluido := 1;
            dbms_output.put_line('La tabla de LKUP posee alias');
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), '^+[a-zA-Z_0-9\.#&]+ '));
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_table_look_up := v_table_look_up;
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
              --v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM_solo_tablas.FIRST .. l_FROM_solo_tablas.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM_solo_tablas(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
              --l_FROM (l_FROM.last) := ' LEFT OUTER JOIN ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' ';
              if (reg_detalle_in.OUTER is null) then
                l_FROM (l_FROM.last) := ' INNER JOIN ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' ';
              else
                l_FROM (l_FROM.last) := ' LEFT OUTER JOIN ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' ';
              end if;

            end if;
            v_alias := v_alias_table_look_up;
          else    /* La tabla de LKUP no posee Alias */
            v_alias_incluido := 0;
            dbms_output.put_line('La tabla de LKUP no posee alias');
            --v_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := v_reg_table_lkup;            
            --if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              dbms_output.put_line('La tabla de LKUP esta calificado');
              --v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9]+'), 2);
              v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
              --v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM_solo_tablas.FIRST .. l_FROM_solo_tablas.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM_solo_tablas(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
              /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
              --l_FROM (l_FROM.last) := ' LEFT OUTER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              if (reg_detalle_in.OUTER is null) then
                l_FROM (l_FROM.last) := ' INNER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              else
                l_FROM (l_FROM.last) := ' LEFT OUTER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              end if;
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
              /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
              --l_FROM (l_FROM.last) := ' LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              if (reg_detalle_in.OUTER is null) then
                l_FROM (l_FROM.last) := ' INNER JOIN ' || mitabla_look_up || ' ';
              else
                l_FROM (l_FROM.last) := ' LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              end if;
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
            end if;
          end if;
          
        end if;
        /*********************************/
        /* (20161227) Angel Ruiz. Ocurre que pueden venir Queries en la columna TABLE_BASE_NAME */
        /*********************************/
        if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt] ') > 0) then
          /* Tenemos una query en TABLE_BASE_NAME DEL SCENARIO */
          v_es_query_table_base:=1;
          /* Calculo el TABLE_BASE_NAME a partir del Scenario mejor que a partir del detail */
          if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$')) then
            v_alias_table_base := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$'), 2));
            v_tabla_base_name := procesa_campo_filter(reg_scenario.TABLE_BASE_NAME);
            v_alias_incluido_table_base:=1;
          end if;
        else
          /* NO es una query lo que viene en TABLE_BASE_NAME*/
          v_es_query_table_base:=0;
          if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* Posee un alias */
            v_alias_incluido_table_base:=1;
            v_alias_table_base := trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, ' +[a-zA-Z_0-9]+$'));
            v_tabla_base_name := trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, '^+[a-zA-Z_0-9\.#&]+ '));
            if (REGEXP_LIKE(v_tabla_base_name, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* TABLE_BASE_NAME esta calificada */
              /* me quedo solo con el nombre de la tabla base name */
              v_tabla_base_name := substr(trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, '\.+[a-zA-Z_0-9]+')),2);
            end if;
          else
            /* No posee un alias */
            v_alias_incluido_table_base:=0;
            /* NO viene ALIAS en TABLE_BASE_NAME */
            if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* TABLE_BASE_NAME esta calificada */
              /* me quedo solo con el nombre de la tabla base name */
              v_tabla_base_name := substr(trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, '\.+[a-zA-Z_0-9]+')),2);
            else
              v_tabla_base_name:=reg_scenario.TABLE_BASE_NAME;
            end if;
          end if;
        end if;
        if (v_alias_incluido_table_base = 1) then
          /* TABLE_BASE_NAME viene con alias */
          if (v_es_query_table_base = 1) then
            mi_tabla_base_name := v_alias_table_base;
            mi_tabla_base_name_alias := v_alias_table_base;
          else
            mi_tabla_base_name := v_tabla_base_name;
            mi_tabla_base_name_alias := v_alias_table_base;
          end if;
        else
          mi_tabla_base_name := v_tabla_base_name;
          mi_tabla_base_name_alias := v_tabla_base_name;
        end if;
        /* (20161227) Angel Ruiz. */

        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        /* (20160302) Angel Ruiz. NF: Campos separados por ; */
        --table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        --ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);
        dbms_output.put_line('Antes del split_string_punto_coma');
        table_columns_lkup := split_string_punto_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        dbms_output.put_line('Despues del split_string_punto_coma para table_columns_lkup');
        ie_column_lkup := split_string_punto_coma (reg_detalle_in.IE_COLUMN_LKUP);
        dbms_output.put_line('===El valor del número de columnas es: ');
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/

        /*************************************************************************/
        /* (20170110) Angel Ruiz. BUG. Existen ocasiones en las que no es posible */
        /* hacer el CASE WHEN para comprobar si los campos vienen NO INFORMADO */
        /* porque las columnas por las que se hacen JOIN poseen muchas funciones */
        /* Compruebo antes si sera posible generar un CASE WHEN */
        /*************************************************************************/
        v_no_se_generara_case:=false;
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0)
            then
              nombre_campo := extrae_campo (ie_column_lkup(indx));
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=false) then
                v_no_se_generara_case:=true;
              end if;
            else
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx))))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=false) then
                v_no_se_generara_case:=true;
              end if;
            end if;
          END LOOP;
          /* (20170709) Angel Ruiz. UNa excepcion para el campo ID_TRAZABILIDAD DE NGA_PARQUE_ABO_MES */
          if (reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD' or reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD_ANT') then
            v_no_se_generara_case:=true;
          end if;
        else
          dbms_output.put_line('El valor del numero de columna es 1');
          v_existe_valor:=false;
          for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
          WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
          UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN)))
          loop
            v_existe_valor:=true;
          end loop;
          if (v_existe_valor=false) then
            v_no_se_generara_case:=true;
          end if;
          /* (20170709) Angel Ruiz. UNa excepcion para el campo ID_TRAZABILIDAD DE NGA_PARQUE_ABO_MES */
          if (reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD' or reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD_ANT') then
            v_no_se_generara_case:=true;
          end if;
        end if;
        /* (20170109) Angel Ruiz. FIN BUG.*/
        dbms_output.put_line('===El valor del número de columnas es: ');
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          --condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          --valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
          --valor_retorno := 'CASE WHEN ' || trim(condicion) || ' THEN NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
          valor_retorno := 'CASE WHEN ' || trim(condicion) || ' THEN coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
        else
          if (v_no_se_generara_case = false) then /*(20170109) Angel Ruiz. BUG: Hay campos con JOIN en los que no se va a generar CASE WHEN */
            /* Construyo el campo de SELECT */
            if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
              valor_retorno := 'CASE WHEN (';
              FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
              LOOP
                /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                --if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0)
                then
                  nombre_campo := extrae_campo (ie_column_lkup(indx));
                  --SELECT * INTO l_registro
                  --FROM MTDT_INTERFACE_DETAIL
                  --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(substr(reg_detalle_in.TABLE_BASE_NAME, 4)) and
                  --UPPER(TRIM(COLUMNA)) = UPPER(TRIM(nombre_campo));
                  SELECT * INTO l_registro2
                  FROM v_MTDT_CAMPOS_DETAIL
                  WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                  UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
                else
                  dbms_output.put_line ('El campo por el que voy a hacer LookUp de la TABLE_BASE es: ' || TRIM(ie_column_lkup(indx)));
                  --SELECT * INTO l_registro
                  --FROM MTDT_INTERFACE_DETAIL
                  --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(substr(reg_detalle_in.TABLE_BASE_NAME, 4)) and
                  --UPPER(trim(COLUMNA)) = UPPER(TRIM(ie_column_lkup(indx)));
                  SELECT * INTO l_registro2
                  FROM V_MTDT_CAMPOS_DETAIL
                  WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                  UPPER(trim(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
                end if;
              
                if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then  /* se trata de un campo VARCHAR */
                  if (indx = 1) then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      --valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IN (''NI#'', ''NO INFORMADO'') ';
                      valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IN (''NI#'', ''NO INFORMADO'') ';
                    else
                      --valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IN (''NI#'', ''NO INFORMADO'') ';
                      valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IN (''NI#'', ''NO INFORMADO'') ';
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      --valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IN (''NI#'', ''NO INFORMADO'') ';
                      valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IN (''NI#'', ''NO INFORMADO'') ';
                    else
                      --valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IN (''NI#'', ''NO INFORMADO'') ';
                      valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IN (''NI#'', ''NO INFORMADO'') ';
                    end if;
                  end if;
                else 
                  if (indx = 1) then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      --valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' = -3 ';
                      --valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' = -3 ';
                      valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' = -1 ';
                    else
                      --valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -3 ';
                      --valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -3 ';
                      valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -1 ';
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      --valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' = -3 ';
                      --valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' = -3 ';
                      valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' = -1 ';
                    else
                      --valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -3 ';
                      --valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -3 ';
                      valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -1 ';
                    end if;
                  end if;
                end if;
              END LOOP;
              /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
              --SELECT * INTO l_registro1
              --FROM MTDT_MODELO_DETAIL
              --WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
              --UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.TABLE_COLUMN);
              SELECT * INTO l_registro2
              FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.TABLE_COLUMN);
              dbms_output.put_line ('Estoy donde quiero.');
              dbms_output.put_line ('El nombre de TABLE_NAME ES: ' || reg_detalle_in.TABLE_NAME);
              dbms_output.put_line ('El nombre de TABLE_COLUMN ES: ' || reg_detalle_in.TABLE_COLUMN);
              dbms_output.put_line ('El tipo de DATOS es: ' || l_registro2.TYPE);
              if (INSTR(trim(l_registro2.TYPE), 'NUMBER') > 0) then
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                  /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                    --valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || procesa_campo_filter (reg_detalle_in.VALUE) || ', -2) END';
                    valor_retorno := valor_retorno || ') THEN -1 ELSE ' || 'coalesce(' || procesa_campo_filter (reg_detalle_in.VALUE) || ', -3) END';
                  else
                    --valor_retorno := valor_retorno || ') THEN 0 ELSE ' || 'NVL(' || procesa_campo_filter (reg_detalle_in.VALUE) || ', 0) END';
                    valor_retorno := valor_retorno || ') THEN 0 ELSE ' || 'coalesce(' || procesa_campo_filter (reg_detalle_in.VALUE) || ', 0) END';
                  end if;
                else
                  if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                  /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                    --valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || v_alias || '.' || procesa_campo_filter (reg_detalle_in.VALUE) || ', -2) END';
                    valor_retorno := valor_retorno || ') THEN -1 ELSE ' || 'coalesce(' || v_alias || '.' || procesa_campo_filter (reg_detalle_in.VALUE) || ', -3) END';
                  else
                    --valor_retorno := valor_retorno || ') THEN 0 ELSE ' || 'NVL(' || v_alias || '.' || procesa_campo_filter (reg_detalle_in.VALUE) || ', 0) END';
                    valor_retorno := valor_retorno || ') THEN 0 ELSE ' || 'coalesce(' || v_alias || '.' || procesa_campo_filter (reg_detalle_in.VALUE) || ', 0) END';
                  end if;
                end if;
              elsif (UPPER(TRIM(l_registro2.TYPE)) = 'DATE') then
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  --valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'NVL(' || procesa_campo_filter (reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE)) END';
                  valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'coalesce(' || procesa_campo_filter (reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE)) END';
                else
                  --valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'NVL(' || v_alias || '.' || procesa_campo_filter (reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE)) END';
                  valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'coalesce(' || v_alias || '.' || procesa_campo_filter (reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE)) END';
                end if;
              else
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  --valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'') END';
                  valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'') END';
                else
                  --valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'') END';
                  valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'') END';
                end if;
              end if;
            else  /* if (table_columns_lkup.COUNT > 1) then */
              /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
              --SELECT * INTO l_registro1
              --FROM MTDT_MODELO_DETAIL
              --WHERE UPPER(TRIM(TABLE_NAME)) =  reg_detalle_in.TABLE_NAME and
              --UPPER(TRIM(COLUMN_NAME)) = reg_detalle_in.TABLE_COLUMN;
              SELECT * INTO l_registro2
              FROM V_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  reg_detalle_in.TABLE_NAME and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN));
              if (instr(l_registro2.TYPE, 'NUMBER') > 0) then
                if (v_alias_incluido = 1) then
                  if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                  /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                    --valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -2)';
                    valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -3)';
                  else
                    --valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                    valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                  end if;
                else
                  if (regexp_instr(reg_detalle_in.VALUE, '[A-Za-z0-9_]') = 0) then
                    /* (20170627) Angel Ruiz. Compruebo VALUE sólo sea un campo */
                    if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                    /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                      --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)';
                      valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', -3)';
                    else
                      --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', 0)';
                      valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', 0)';
                    end if;
                  else
                    if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                    /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                      --valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -2)';
                      valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -3)';
                    else
                      --valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                      valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                    end if;
                  end if;
                end if;
              elsif (UPPER(trim(l_registro2.TYPE)) = 'DATE') then
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE))';
                  valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE))';
                else
                  --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', CAST(''2000-01-01'' AS DATE))';
                  valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', CAST(''2000-01-01'' AS DATE))';
                end if;
              else
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'')';
                  valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'')';
                else
                  --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'')';
                  valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'')';
                end if;
              end if;
            end if;   /* end if if (table_columns_lkup.COUNT > 1) then */
          else  /* (20170109) Angel Ruiz. if (v_no_se_generara_case = false) then */
            /*(20170109) Angel Ruiz. BUG: Hay campos con JOIN en los que no se va a generar CASE WHEN */
            valor_retorno :=  '    ' || procesa_campo_filter(reg_detalle_in.VALUE);
          end if;   /* Fin del if (v_no_se_generara_case = false) then */

        end if;   /* fin del if (reg_detalle_in.LKUP_COM_RULE is not null) then */
        dbms_output.put_line('===Llego hasta la parte dle where');
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        l_WHERE_ON_clause.delete;   /* (20161204) Angel Ruiz */
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE_ON_clause.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            --if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
            if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0)
            then
            
              --nombre_campo := extrae_campo_decode (ie_column_lkup(indx));
              nombre_campo := extrae_campo (ie_column_lkup(indx));
              --SELECT * INTO l_registro
              --FROM MTDT_INTERFACE_DETAIL
              --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
              --UPPER(TRIM(COLUMNA)) = UPPER(TRIM(nombre_campo));
              /****************************************/
              /* (20170109) Angel Ruiz. BUG. Hay campos de los q no se puede hayar su tipo pq tienen muchas funciones */
              /****************************************/
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor = true) then
                SELECT * INTO l_registro2
                FROM V_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
              end if;
            else
              --SELECT * INTO l_registro
              --FROM MTDT_INTERFACE_DETAIL
              --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
              --UPPER(TRIM(COLUMNA)) = UPPER(TRIM(ie_column_lkup(indx)));
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx))))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor = true) then
                SELECT * INTO l_registro2
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
              end if;
            end if;
            if (l_WHERE_ON_clause.count = 1) then
              if (v_existe_valor = true) then
                if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'coalesce(' || transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ', ''NI#'')' || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'coalesce(' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                  if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                  end if;
                end if;
              else    /* if (v_existe_valor = true) then */
                /* No podemos encontar el campo en el diccionario de datos */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
                elsif (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || ' = ' || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                elsif (instr(ie_column_lkup(indx), '.') = 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                end if;
              end if; /* fin del if (v_existe_valor = true) then */
            else  /* else del if (l_WHERE_ON_clause.count = 1) then */
              if (v_existe_valor = true) then /* (20170110) Angel Ruiz */
                if (UPPER(TRIM(l_registro2.TYPE))='VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ', ''NI#'')' || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode (ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  end if;
                else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */                
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                end if;
              else /* else del if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
                elsif (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || ie_column_lkup(indx) || ' = ' || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                elsif (instr(ie_column_lkup(indx), '.') = 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ' = ' || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                end if;                  
              end if;
            end if; /* Fin del if (l_WHERE_ON_clause.count = 1) then */
          END LOOP;
        else    /* Solo hay un campo condicion */
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE_ON_clause.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE_ON_clause.count = 1) then
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
              /* (20161204) Angel Ruiz.*/
              --l_WHERE.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := l_WHERE_ON_clause(l_WHERE_ON_clause.last) || ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' < ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            else
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP;
              /* (20161204) Angel Ruiz.*/
              --l_WHERE.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := l_WHERE_ON_clause(l_WHERE_ON_clause.last) || ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' < ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            end if;
          else /* if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then */
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            --if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
            if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0)
            then
              --nombre_campo := extrae_campo_decode (reg_detalle_in.IE_COLUMN_LKUP);
              nombre_campo := extrae_campo (reg_detalle_in.IE_COLUMN_LKUP);
              dbms_output.put_line('Estoy dentro del if DE FUNCIONES. el valor de nombre_campo es: $' || nombre_campo || '$');
              --SELECT * INTO l_registro
              --FROM MTDT_INTERFACE_DETAIL
              --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
              --UPPER(TRIM(COLUMNA)) = UPPER(trim(nombre_campo));
              /* (20170109) Angel Ruiz. BUG: Hay campos que no se encuentran en el diccionario de datos */
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                /* podemos encontrar el campo en el diccionario de datos */
                SELECT * INTO l_registro2
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(trim(nombre_campo));
              end if;
            else
              --SELECT * INTO l_registro
              --FROM MTDT_INTERFACE_DETAIL
              --WHERE UPPER(TRIM((CONCEPT_NAME))) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
              --UPPER(TRIM(COLUMNA)) = UPPER(reg_detalle_in.IE_COLUMN_LKUP);
              /* (20170109) Angel Ruiz. BUG: Hay campos que no se encuentran en el diccionario de datos */
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.IE_COLUMN_LKUP)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                SELECT * INTO l_registro2
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM((TABLE_NAME))) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.IE_COLUMN_LKUP);
              end if;
            end if;
            if (l_WHERE_ON_clause.count = 1) then /* si es el primer campo del WHERE */
              if (v_existe_valor = true) then /* (20170110) Angel Ruiz. BUG. Hay columnas que no se encuentran en el metadato */
                if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || ', ''NI#'')' ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 and regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 and regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                  --l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;                  
              end if;
            else  /* sino es el primer campo del Where  */
              if (v_existe_valor = true) then
                if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || ', ''NI#'')' || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[D][E][C][O][D][E]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[D][E][C][O][D][E]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff][Nn][Uu][Ll][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              else  /* else del if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              end if;
            end if; /* (20170110) Angel Ruiz.FIN DEL IF if (l_WHERE_ON_clause.count = 1) */
          end if;   /* END IF de if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then */
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE_ON_clause.extend;
          l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || procesa_campo_filter(procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias));
        end if;
        /* (20161202) Angel Ruiz */
        /* Modifico esta parte para HIVE */
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || ' ON (';
        FOR indx IN l_WHERE_ON_clause.FIRST .. l_WHERE_ON_clause.LAST
        LOOP
          l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || l_WHERE_ON_clause(indx);
        END LOOP;
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || ')';
      when 'FUNCTION' then
        /* se trata de la regla FUNCTION */
        /* (20150306) ANGEL RUIZ. Hay un error que corrijo */
        v_nombre_tabla_reducido := substr(reg_detalle_in.TABLE_NAME, instr(reg_detalle_in.TABLE_NAME, '_')+1);
        if (length(reg_detalle_in.TABLE_NAME) < 25) then
        v_nombre_paquete := reg_detalle_in.TABLE_NAME;
        else
        v_nombre_paquete := v_nombre_tabla_reducido;
        end if;        
        valor_retorno :=  '    ' || 'PKG_' || v_nombre_paquete || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
      when 'DLOAD' then
        --valor_retorno := '    ' || 'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
        valor_retorno := '    ' || 'date_format (''#VAR_FCH_CARGA#'', ''yyyy-MM-dd'')'; /* (20161208) Angel Ruiz */
      when 'DSYS' then
        --valor_retorno := '    ' || 'SYSDATE';
        valor_retorno := '    ' || 'current_date'; /* (20161208) Angel Ruiz */
      when 'CODE' then
        pos := 0;
        posicion_ant := 0;
        cadena_resul:= trim(reg_detalle_in.VALUE);
        lon_cadena := length (cadena_resul);
        if lon_cadena > 0 then
          valor_retorno := procesa_campo_filter (cadena_resul);
        
        --posicion := instr(reg_detalle_in.VALUE, 'VAR_IVA');
        --if (posicion >0) then
          --cad_pri := substr(reg_detalle_in.VALUE, 1, posicion-1);
          --cad_seg := substr(reg_detalle_in.VALUE, posicion + length('VAR_IVA'));
          --valor_retorno :=  '    ' || cad_pri || '21' || cad_seg;
        --else
          --valor_retorno :=  '    ' || reg_detalle_in.VALUE;
        --end if;
        --posicion := instr(valor_retorno, '#VAR_FCH_CARGA#');
        --if (posicion >0) then
          --cad_pri := substr(valor_retorno, 1, posicion-1);
          --cad_seg := substr(valor_retorno, posicion + length('#VAR_FCH_CARGA#'));
          --valor_retorno :=  '    ' || cad_pri || ' to_date(fch_datos_in, ''yyyymmdd'') ' || cad_seg;
          --valor_retorno :=  '    ' || cad_pri || ' date_format(''#VAR_FCH_DATOS#'', ''yyyy-MM-dd'') ' || cad_seg; /* (20161208) Angel Ruiz */
        end if;
      when 'HARDC' then
        /* (20170105) Angel Ruiz */
        if reg_detalle_in.VALUE <> 'NULL' then
          SELECT * INTO l_registro2
          FROM v_MTDT_CAMPOS_DETAIL
          WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
          UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN));
          if (l_registro2.type <> 'NUMBER') then
            valor_retorno := '    ''' || reg_detalle_in.VALUE || '''';
          else
            valor_retorno := '    ' || reg_detalle_in.VALUE;
          end if;
        else
            valor_retorno := '    ' || reg_detalle_in.VALUE;
        end if;
      when 'SEQ' then
        valor_retorno := '     default';
        
        dbms_output.put_line('ESTOY EN LA REGLA SEQ.');
        v_hay_regla_seq := true;
        --v_nombre_seq := 'SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5);
        --v_nombre_seq := 'SEQ_' || substr(regexp_substr(reg_detalle_in.TABLE_COLUMN, '_[A-Za-z_]+$'), 2);
        v_nombre_seq := reg_detalle_in.VALUE;
        v_nombre_campo_seq := reg_detalle_in.TABLE_COLUMN;
        
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        /* (20170605) Angel Ruiz. BUG. Si ya lleva punto es que se le ha puesto el propietario */
        /* por lo que no se le pone */
        if (instr(reg_detalle_in.VALUE, '.') = 0) then
        
          /* Solo si el campo ya no esta calificado lo calificamos */
          if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            /* La tabla esta calificada */
            v_temporal := procesa_campo_filter(trim(reg_detalle_in.TABLE_BASE_NAME));
            if (REGEXP_LIKE(trim(v_temporal), '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+ +[a-zA-Z0-9_]+$') = true) then
              /* (20160329) Angel Ruiz. Detectamos si TABLE_BASE_NAME posee ALIAS */
              v_alias_table_base_name := trim(REGEXP_SUBSTR(TRIM(v_temporal), ' +[a-zA-Z_0-9]+$'));
              v_table_base_name := substr(trim(REGEXP_SUBSTR(TRIM(v_temporal), '\.[a-zA-Z_0-9]+ ')),2);
            else
              v_alias_table_base_name := substr(trim(REGEXP_SUBSTR(TRIM(v_temporal), '\.[a-zA-Z_0-9]+')),2);
              v_table_base_name := substr(trim(REGEXP_SUBSTR(TRIM(v_temporal), '\.[a-zA-Z_0-9]+')),2);
            end if;
          else
            /* La tabla no esta calificada */
            --if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9]+ +[a-zA-Z_0-9]+$') = true) then
            if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9]+\[*[a-zA-Z_0-9]+\]* +[a-zA-Z_0-9]+$') = true) then
              /* (20160329) Angel Ruiz. Detectamos si TABLE_BASE_NAME posee ALIAS */
              v_alias_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_BASE_NAME), ' +[a-zA-Z_0-9]+$'));
              v_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_BASE_NAME), '^+[a-zA-Z_0-9]+ '));
            else
              v_alias_table_base_name := reg_detalle_in.TABLE_BASE_NAME;
              v_table_base_name := reg_detalle_in.TABLE_BASE_NAME;
            end if;
          end if;
          --valor_retorno := '    ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;
          --valor_retorno := v_alias_table_base_name || '.' || reg_detalle_in.VALUE;
          valor_retorno := v_alias_table_base_name || '.' || reg_detalle_in.VALUE;
        else
          --valor_retorno := '    ' || reg_detalle_in.VALUE;
          valor_retorno := '    ' || reg_detalle_in.VALUE;
        end if;
      when 'VAR_FCH_INICIO' then
        --valor_retorno := '    ' || 'var_fch_inicio';
        /* (20161208) Angel Ruiz. Ocurre que esta regla la podemos usar tanto en */
        /* campos DATE como en campos DATETIME, por lo que hay que saber de que tipo de campo se trata */
        select trim(data_type) into v_tipo_campo from mtdt_modelo_detail where trim(table_name) = reg_detalle_in.TABLE_NAME and trim(column_name) = reg_detalle_in.TABLE_COLUMN;
        if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
          --valor_retorno := '    ' || 'date_format(var_fch_inicio, ''yyyy-MM-dd hh:mm:ss'')';
          valor_retorno := '    ' || 'var_fch_inicio';
        else
          valor_retorno := '    ' || 'var_fch_inicio';
        end if;
      when 'VAR_FCH_CARGA' then
          valor_retorno := '     ' || 'fch_datos_in'; /* (20161219) Angel Ruiz */
      when 'VAR_FCH_INI_MES' then
        /* (20170522) Angel Ruiz. Incñuyo una nueva variable VAR_FCH_INI_MES que contiene el primer dia del mes */
        --valor_retorno := '     ' || 'date_format(fch_datos_in, ''yyyy-MM-01'')';          
        valor_retorno := '     ' || 'to_char((to_date(fch_datos_in, ''YYYYMMDD''), ''YYYYMM01''';          
      when 'VAR_FIN_DEFAULT' then
          valor_retorno := '    ' || '''99991231''';
      when 'VAR_USER' then
          /* (20191010) Angel Ruiz. BUG.*/
          --valor_retorno := '    ' || '''#VAR_USER#''';
          valor_retorno := '    ' || 'current_user';
      when 'VAR' then
        /* Se toma el valor de una variable de entorno */
        if reg_detalle_in.VALUE =  'VAR_FCH_DATOS' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '     ' || 'fch_datos_in';        
          --valor_retorno := '     ' || '''#VAR_FCH_DATOS#'''; /* (20161208) Angel Ruiz */
          valor_retorno := '     ' || 'to_date(fch_datos_in, ''YYYYMMDD'')'; /* (20161208) Angel Ruiz */
        end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno := '     ' || 'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '     ' || 'fch_datos_in';        
          --valor_retorno := '     ' || '''#VAR_FCH_DATOS#''';  /* (20161208) Angel Ruiz */      
          valor_retorno := '     ' || 'to_date(fch_carga_in, ''YYYYMMDD'')';  /* (20210805) Angel Ruiz */      
        end if;
        if reg_detalle_in.VALUE =  'VAR_PAIS_TM' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          valor_retorno := '    ' || '1';
        end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_INICIO' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          /* (20161208) Angel Ruiz. Ocurre que esta regla la podemos usar tanto en */
          /* campos DATE como en campos TIMESTAMP, por lo que hay que saber de que tipo de campo se trata */
          select type into v_tipo_campo from v_MTDT_CAMPOS_DETAIL where table_name = reg_detalle_in.TABLE_NAME and column_name = reg_detalle_in.TABLE_COLUMN;
          if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
            --valor_retorno := '    ' || 'date_format(var_fch_inicio, ''yyyy-MM-dd hh:mm:ss'')';
            valor_retorno := '    ' || 'var_fch_inicio';
          else
            --valor_retorno := '    ' || 'date_format(var_fch_inicio, ''yyyy-MM-dd'')';
            valor_retorno := '    ' || 'var_fch_inicio';
          end if;
        end if;
      when 'LKUPN' then
        /* (20150824) ANGEL RUIZ. Nueva Regla. Permite rescatar un campo numerico de la tabla de look up y hacer operaciones con el */
        l_FROM.extend;
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          v_alias := 'LKUP_' || l_FROM.count;
          mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else
          /* (20161111) Angel Ruiz. NF. Puede haber ALIAS EN LA TABLA DE LOOUP */
          dbms_output.put_line('Dentro del ELSE del SELECT');
          /* (20160401) Detectamos si la tabla de LookUp posee Alias */
          v_reg_table_lkup := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
          if (REGEXP_LIKE(trim(v_reg_table_lkup), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_incluido := 1;
            dbms_output.put_line('La tabla de LKUP posee alias');
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), '^+[a-zA-Z_0-9\.#&]+ '));
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_table_look_up := v_table_look_up;
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              --v_table_look_up := OWNER_DM || '.' || v_table_look_up;
              v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
            v_alias := v_alias_table_look_up;
          else    /* La tabla de LKUP no posee Alias */
            v_alias_incluido := 0;
            dbms_output.put_line('La tabla de LKUP no posee alias');
            --v_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := v_reg_table_lkup;            
            --if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              dbms_output.put_line('La tabla de LKUP esta calificado');
              --v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9]+'), 2);
              v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              --v_table_look_up := OWNER_DM || '.' || v_table_look_up;
              v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
          end if;
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);
        /* Le añadimos al nombre del campo de la tabla de LookUp su Alias */
        v_value := proceso_campo_value (reg_detalle_in.VALUE, v_alias);
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_value || ', -2) ELSE ' || trim(constante) || ' END';
        else
          /* Construyo el campo de SELECT */
          if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
            valor_retorno := 'CASE WHEN (';
            FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
            LOOP
              --SELECT * INTO l_registro
              --FROM MTDT_INTERFACE_DETAIL
              --WHERE UPPER(TRIM(CONCEPT_NAME)) =  SUBSTR(reg_detalle_in.TABLE_BASE_NAME,4) and
              --UPPER(TRIM(COLUMNA)) = UPPER(TRIM(ie_column_lkup(indx)));
              SELECT * INTO l_registro2
              FROM V_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
            
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then  /* se trata de un campo VARCHAR */
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                end if;
              else 
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -3 ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || TRIM(l_registro2.COLUMN_NAME) || ' = -3 ';
                end if;
              end if;
            END LOOP;
            valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || v_value || ', -2) END';
          else
            valor_retorno :=  '    NVL(' || v_value || ', -2)';
          end if;

        end if;

        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/

        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            --SELECT * INTO l_registro
            --FROM MTDT_INTERFACE_DETAIL
            --WHERE UPPER(TRIM((CONCEPT_NAME))) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
            --UPPER(TRIM(COLUMNA)) = UPPER(TRIM(ie_column_lkup(indx)));
            SELECT * INTO l_registro2
            FROM v_MTDT_CAMPOS_DETAIL
            WHERE UPPER(TRIM((TABLE_NAME))) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
            UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
            if (l_WHERE.count = 1) then
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            else
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE.count = 1) then
              l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            else
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            --SELECT * INTO l_registro
            --FROM MTDT_INTERFACE_DETAIL
            --WHERE UPPER(TRIM(CONCEPT_NAME)) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
            --UPPER(TRIM(COLUMNA)) = UPPER(reg_detalle_in.IE_COLUMN_LKUP);
            SELECT * INTO l_registro2
            FROM v_MTDT_CAMPOS_DETAIL
            WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(SUBSTR(reg_detalle_in.TABLE_BASE_NAME, 4)) and
            UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.IE_COLUMN_LKUP);
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (UPPER(TRIM(l_registro2.TYPE)) = 'VARCHAR2') then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (TO_NUMBER(TRIM(l_registro2.LENGTH)) < 3 and l_registro2.NULABLE IS NULL) then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE.extend;
          l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
        end if;
        when 'LKUPD' then
          if (reg_detalle_in.LKUP_COM_RULE is not null) then
            /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
            cadena := trim(reg_detalle_in.LKUP_COM_RULE);
            pos_del_si := instr(cadena, 'SI');
            pos_del_then := instr(cadena, 'THEN');
            pos_del_else := instr(cadena, 'ELSE');
            pos_del_end := instr(cadena, 'END');  
            condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
            condicion_pro := procesa_COM_RULE_lookup(condicion);
            constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
            valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', '' '') ELSE ' || trim(constante) || ' END';
          else
            valor_retorno := procesa_campo_filter(reg_detalle_in.VALUE);
          end if;
      end case;
    return valor_retorno;
  end;

  function genera_encabezado_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
    ie_lkup_columns                list_strings := list_strings();
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_tabla                          VARCHAR2(30);
    
  begin
    /* (20150130) Angel Ruiz . Nueva incidencia. */
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then
      /* Aparecen queries en lugar de tablas para LookUp */
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.VALUE;  /* Llamo a mi funcion de LookUp esta concatenacion con el nombre del campo resultado del LookUp */
      v_nombre_tabla := reg_lookup_in.TABLE_BASE_NAME;  /* Si lo que tengo es una SELECT tengo que recurrir al nombre de la tabla BASE para posteriormente saber el tipo de campo  */
    else
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_LKUP;  /* Llamo a mi funcion de LookUp esta concatenacion */
      v_nombre_tabla := reg_lookup_in.TABLE_LKUP;     /* Como no tengo una SELECT uso la tabla de LookUp para posteriormente saber el tipo de campo  */
    end if;
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then    /* (20150102) Angel Ruiz . Nueva incidencia. Hay una SELECT en lugar de una tabla para hacer LookUp */
      /* Para hacer el prototipo de la funcion he de usar la tabla base y los campos ie_olumn_lookup ya que no tenemos los campos de LookUp al ser una select */
      lkup_columns := split_string_punto_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      ie_lkup_columns := split_string_punto_coma (reg_lookup_in.IE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
        FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
        LOOP
          if indx = 1 then
              valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          else
            valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          end if;
        END LOOP;
        valor_retorno := valor_retorno || ') return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
      else        
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || reg_lookup_in.IE_COLUMN_LKUP || '%TYPE) return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
      end if;
        
    else  /* (20150102) Angel Ruiz . Nueva incidencia. Hay una tabla de LookUp normal. No SELECT */
    
      lkup_columns := split_string_punto_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
        FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
        LOOP
          if indx = 1 then
            valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
          else
            valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
          end if;
        END LOOP;
        valor_retorno := valor_retorno || ') return ' || reg_lookup_in.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
      else        
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE) return ' || reg_lookup_in.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
      end if;
    end if;
    return valor_retorno;
  end;

  function gen_enca_funcion_LKUPD (reg_lookup_in in MTDT_TC_LKUPD%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
    ie_lkup_columns                list_strings := list_strings();
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_tabla                          VARCHAR2(30);
    
  begin
    /* (20150430) Angel Ruiz .  */
    v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion */
    v_nombre_tabla := reg_lookup_in.TABLE_LKUP;
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    lkup_columns := split_string_punto_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
    if (lkup_columns.COUNT > 1)
    then
      valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        else
          valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        end if;
      END LOOP;
      valor_retorno := valor_retorno || ') return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE RESULT_CACHE;';
    else        
      valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE) return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE RESULT_CACHE;';
    end if;
    return valor_retorno;
  end;

  procedure genera_cuerpo_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
    ie_lkup_columns                list_strings := list_strings();
    v_alias             VARCHAR2(40);
    mitabla_look_up VARCHAR2(200);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_tabla                          VARCHAR2(30);
    l_registro          ALL_TAB_COLUMNS%rowtype;

  begin
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* (20150130) Angel Ruiz . Nueva incidencia. */
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then
      /* Aparecen queries en lugar de tablas para LookUp */
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion con el nombre del campo destino del LookUp */
      v_nombre_tabla := reg_lookup_in.TABLE_BASE_NAME;  /* Si lo que tengo es una SELECT tengo que recurrir al nombre de la tabla BASE para posteriormente saber el tipo de campo  */
    else
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_LKUP;  /* Llamo a mi funcion de LookUp esta concatenacion */
      v_nombre_tabla := reg_lookup_in.TABLE_LKUP;     /* Como no tengo una SELECT uso la tabla de LookUp para posteriormente saber el tipo de campo  */
    end if;
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then    /* (20150102) Angel Ruiz . Nueva incidencia. Hay una SELECT en lugar de una tabla para hacer LookUp */
      /* Para hacer el prototipo de la funcion he de usar la tabla base y los campos ie_olumn_lookup ya que no tenemos los campos de LookUp al ser una select */
      /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
      lkup_columns := split_string_punto_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      ie_lkup_columns := split_string_punto_coma (reg_lookup_in.IE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
        FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
        LOOP
          if indx = 1 then
              valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          else
            valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          end if;
        END LOOP;
        valor_retorno := valor_retorno || ') return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE';
        UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
      else        
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || reg_lookup_in.IE_COLUMN_LKUP || '%TYPE) return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE';
        UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
      end if;
    else
      /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
      lkup_columns := split_string_punto_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (';
        FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
        LOOP
          if indx = 1 then
            valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
          else
            valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
          end if;
        END LOOP;
        valor_retorno := valor_retorno || ') ';
        UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
      else        
        UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE)'); 
      end if;
      UTL_FILE.put_line (fich_salida_pkg, '    return ' || reg_lookup_in.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE');
      UTL_FILE.put_line (fich_salida_pkg, '    RESULT_CACHE RELIES_ON (' || reg_lookup_in.TABLE_LKUP || ')');
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    /* (20150130) Angel Ruiz . Nueva incidencia. */
    --if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then
    --  UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE;');
    --else
    --  UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE;');
    --end if;
    /* (20150619) Angel Ruiz. Nueva funcionalidad. Cambio la obtencion del tipo de dato de retorno para que sea mas coherente*/
    UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE;');
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    /**********************************************************/
    /* (20150217) Angel Ruiz. Incidencia debido a que no esta retornando bien el valor de LookUp cuando se hace LookUp por varios campos */
    if (lkup_columns.COUNT > 1) then
      UTL_FILE.put_line (fich_salida_pkg, '');
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        dbms_output.put_line ('#### El campo que busco en el LookUp es: ' || lkup_columns(indx));
        SELECT * INTO l_registro
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME =  reg_lookup_in.TABLE_LKUP and
        COLUMN_NAME = trim(lkup_columns(indx));

        if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
          if (indx = 1) then
            UTL_FILE.put_line (fich_salida_pkg, '  if (' || lkup_columns(indx) || '_IN ' || 'IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO''');
          else
            if (indx = lkup_columns.LAST) then
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' ||lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO'') then');
            else
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' ||lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO''');
            end if;
          end if;
        else
          if (indx = 1) then
            UTL_FILE.put_line (fich_salida_pkg, '  if (' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3');
          else
            if (indx = lkup_columns.LAST) then
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3) then');
            else
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3');
            end if;
          end if;
        end if;
      END LOOP;
      UTL_FILE.put_line (fich_salida_pkg, '      l_row := -3;');
      UTL_FILE.put_line (fich_salida_pkg, '  else');
    end if;

    UTL_FILE.put_line (fich_salida_pkg, '');

    /*********************************************************/
    
    UTL_FILE.put_line (fich_salida_pkg, '    SELECT ifnull(' || reg_lookup_in.VALUE || ', -2) INTO l_row'); 
    UTL_FILE.put_line (fich_salida_pkg, '    FROM ' || reg_lookup_in.TABLE_LKUP);
    
    if (lkup_columns.COUNT > 1) then
      valor_retorno := '    WHERE ' ;
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        else
          valor_retorno := valor_retorno || ' and ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        end if;
      END LOOP;
      if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        valor_retorno := valor_retorno || ';';
      else
        valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP_COND || ';';
      end if;
      UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
    else 
      /* 20141204 Angel Ruiz - Añadido para las tablas de LOOK UP que son un rango */
      if (instr (reg_lookup_in.TABLE_LKUP,'RANGO') > 0) then
        /* Se trata de una tabla de Rango y la trato diferente */
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ';' );
        else
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ' and ' || reg_lookup_in.TABLE_LKUP_COND || ';');
        end if;
      else 
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in;' );
        else
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in and ' || reg_lookup_in.TABLE_LKUP_COND || ';' );
        end if;
      end if;
      --if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
      --  UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in;' );
      --else
      --  UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in and ' || reg_lookup_in.TABLE_LKUP_COND || ';' );
      --end if;
    end if;
    /* (20150217) Angel Ruiz. Incidencia debido a que no esta retornando bien el valor de LookUp cuando se hace LookUp por varios campos */
    if (lkup_columns.COUNT > 1) then
      UTL_FILE.put_line (fich_salida_pkg, '');
      UTL_FILE.put_line (fich_salida_pkg, '  end if;');
    end if;
    /***********************************/
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN l_row;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  exception');
    UTL_FILE.put_line (fich_salida_pkg, '  when NO_DATA_FOUND then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '  when others then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '');
 
  end genera_cuerpo_funcion_pkg;

/************/


  procedure gen_cuer_funcion_LKUPD (reg_lookup_in in MTDT_TC_LKUPD%rowtype) is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
    ie_lkup_columns                list_strings := list_strings();
    v_alias             VARCHAR2(40);
    mitabla_look_up VARCHAR2(200);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_tabla                          VARCHAR2(30);
    l_registro          ALL_TAB_COLUMNS%rowtype;

  begin
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* (20150430) Angel Ruiz . */
    v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion */
    v_nombre_tabla := reg_lookup_in.TABLE_LKUP;
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    lkup_columns := split_string_punto_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
    if (lkup_columns.COUNT > 1)
    then
      valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        else
          valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        end if;
      END LOOP;
      valor_retorno := valor_retorno || ') ';
      UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
    else        
      UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE)'); 
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '    return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE');
    UTL_FILE.put_line (fich_salida_pkg, '    RESULT_CACHE RELIES_ON (' || reg_lookup_in.TABLE_LKUP || ')');
    UTL_FILE.put_line (fich_salida_pkg, '  IS');

    UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE;');
    
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    /**********************************************************/
    if (lkup_columns.COUNT > 1) then
      UTL_FILE.put_line (fich_salida_pkg, '');
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        SELECT * INTO l_registro
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME =  reg_lookup_in.TABLE_LKUP and
        COLUMN_NAME = trim(lkup_columns(indx));

        if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
          if (indx = 1) then
            UTL_FILE.put_line (fich_salida_pkg, '  if (' || lkup_columns(indx) || '_IN ' || 'IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO''');
          else
            if (indx = lkup_columns.LAST) then
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' ||lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO'') then');
            else
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' ||lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO''');
            end if;
          end if;
        else
          if (indx = 1) then
            UTL_FILE.put_line (fich_salida_pkg, '  if (' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3');
          else
            if (indx = lkup_columns.LAST) then
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3) then');
            else
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3');
            end if;
          end if;
        end if;
      END LOOP;
      UTL_FILE.put_line (fich_salida_pkg, '      l_row := ''NI#'';');
      UTL_FILE.put_line (fich_salida_pkg, '  else');
    end if;

    UTL_FILE.put_line (fich_salida_pkg, '');

    /*********************************************************/
    
    UTL_FILE.put_line (fich_salida_pkg, '    SELECT ifnull(' || reg_lookup_in.VALUE || ', ''GE#'') INTO l_row'); 
    UTL_FILE.put_line (fich_salida_pkg, '    FROM ' || reg_lookup_in.TABLE_LKUP);
    
    if (lkup_columns.COUNT > 1) then
      valor_retorno := '    WHERE ' ;
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        else
          valor_retorno := valor_retorno || ' and ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        end if;
      END LOOP;
      if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        valor_retorno := valor_retorno || ';';
      else
        valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP_COND || ';';
      end if;
      UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
    else 
      /* 20141204 Angel Ruiz - Añadido para las tablas de LOOK UP que son un rango */
      if (instr (reg_lookup_in.TABLE_LKUP,'RANGO') > 0) then
        /* Se trata de una tabla de Rango y la trato diferente */
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ';' );
        else
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ' and ' || reg_lookup_in.TABLE_LKUP_COND || ';');
        end if;
      else 
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in;' );
        else
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in and ' || reg_lookup_in.TABLE_LKUP_COND || ';' );
        end if;
      end if;
    end if;
    /* (20150217) Angel Ruiz. Incidencia debido a que no esta retornando bien el valor de LookUp cuando se hace LookUp por varios campos */
    if (lkup_columns.COUNT > 1) then
      UTL_FILE.put_line (fich_salida_pkg, '');
      UTL_FILE.put_line (fich_salida_pkg, '  end if;');
    end if;
    /***********************************/
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN l_row;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  exception');
    UTL_FILE.put_line (fich_salida_pkg, '  when NO_DATA_FOUND then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN ''GE#'';');
    UTL_FILE.put_line (fich_salida_pkg, '  when others then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN ''GE#'';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || v_nombre_func_lookup || ';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '');
 
  end gen_cuer_funcion_LKUPD;

/************/


begin
  /* (20141222) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO v_REQ_NUMER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  SELECT VALOR INTO ESQUEMA_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'ESQUEMA_DM';
  SELECT VALOR INTO PAIS FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PAIS_DM';
  SELECT VALOR INTO BD_SID FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'BD_SID';
  SELECT VALOR INTO NAME_DM_FULL FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM_FULL';
  --SELECT VALOR INTO v_MULTIPLICADOR_PROC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'MULTIPLICADOR_PROC';

  /* (20141222) FIN*/

  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    if (upper(reg_tabla.TABLE_TYPE) = 'D') 
    then
      dbms_output.put_line ('Estoy en el primer LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME);
      nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, instr(reg_tabla.TABLE_NAME, '_')+1); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
      /* Angel Ruiz (20141201) Hecho porque hay paquetes que no compilan */
      if (length(reg_tabla.TABLE_NAME) < 25) then
        nombre_proceso := reg_tabla.TABLE_NAME;
      else
        nombre_proceso := nombre_tabla_reducido;
      end if;
      nombre_proceso := reg_tabla.TABLE_NAME; /* (20241003) Angel Ruiz. NF: Para que el nombre del paquete sea el mismo que el de la tabla */
      --nombre_fich_carga := 'load_ne_' || reg_tabla.TABLE_NAME || '.sh';
      nombre_fich_carga := 'load_ne_' || reg_tabla.TABLE_NAME || '.sh';
      --nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql';
      nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql'; /* Angel Ruiz (20141201) Hecho porque hay paquetes que no compilan */
      nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
      --nombre_fich_hist := 'load_dh_' || reg_tabla.TABLE_NAME || '.sh';
      fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
      fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W', 32767);
      --fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
      --fich_salida_hist := UTL_FILE.FOPEN ('SALIDA',nombre_fich_hist,'W');
      dbms_output.put_line ('El nombre del PAQUETE es: ' || '.pkg_' || nombre_proceso);

      --UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
      lista_scenarios_presentes.delete;
      /******/
      /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
      /******/
      
      /* Tercero genero los metodos para los escenarios */
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
      loop
        fetch MTDT_SCENARIO
        into reg_scenario;
        exit when MTDT_SCENARIO%NOTFOUND;
        dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
        /* Elaboramos los prototipos de la funciones que cargaran los distintos escenarios */
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        --lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'N';
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := reg_scenario.SCENARIO;
      end loop;   /* Fin del loop MTDT_SCENARIO */
      close MTDT_SCENARIO;
  
      
      /* Tercero genero los cuerpos de los metodos que implementan los escenarios */
     
      /***************************************************/
      /***************************************************/
      v_num_sce_NUEVOS := 0;  /* (20170315) Angel Ruiz. NF: numeros de escenarios del mismo tipo NUEVO */
      v_num_sce_EXISTENTES := 0;  /* (20170315) Angel Ruiz. NF: numeros de escenarios del mismo tipo EXISTENTE */
      v_num_sce_HISTORICOS := 0;  /* (20170315) Angel Ruiz. NF: numeros de escenarios del mismo tipo HISTORICOS */
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
      loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      if (reg_scenario.TABLE_COLUMNS is not null and reg_scenario.INTERFACE_COLUMNS is not null) then
        dbms_output.put_line ('Estoy en el segundo LOOP MTDT_SCENARIO. El escenario es: ' || reg_scenario.SCENARIO);
        --if (reg_scenario.SCENARIO like 'N%')
        if ((reg_scenario.SCENARIO = 'N' or regexp_instr(reg_scenario.SCENARIO, '^N_.+') > 0 or regexp_instr(reg_scenario.SCENARIO, '.+_N$') > 0))
        then
          /* (20160701) Angel Ruiz. BUG: Debo borrar en cada escenario las listas de */
          /* componentes del From y del Where */
          l_FROM.delete;
          l_WHERE.delete;
          l_FROM_solo_tablas.delete;
          lista_variables_rownumber.delete;
          
          v_hay_regla_seq:=false;
          v_num_sce_NUEVOS := v_num_sce_NUEVOS + 1; /* (20170315) Angel Ruiz. NF: Pueden venir varios escenarios nuevos */
          /* ESCENARIO NUEVO */
          dbms_output.put_line ('Estoy en el escenario: ' || reg_scenario.SCENARIO);
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, '-- ### ESCENARIO NUEVO: ' || reg_scenario.SCENARIO || ' ###');
          UTL_FILE.put_line(fich_salida_pkg, '');
          --UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_DM || '.nreg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR(8), fch_datos_in IN VARCHAR(8)) return DECIMAL');
          UTL_FILE.put_line(fich_salida_pkg, 'DROP FUNCTION IF EXISTS ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8))');
          UTL_FILE.put_line(fich_salida_pkg, 'returns integer as $$');
          UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas INTEGER := 0;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio TIMESTAMP := CURRENT_TIMESTAMP;');
          UTL_FILE.put_line(fich_salida_pkg, '  v_error_code text;');
          UTL_FILE.put_line(fich_salida_pkg, '  v_error_msg text;');
          UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'  INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'  INTO ' || OWNER_DM || '.' || NOMBRE_PROCESO || '_T');
          UTL_FILE.put_line(fich_salida_pkg,'  (');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
            if (upper(trim(reg_detail.RUL)) != 'AUTO') then
              if primera_col = 1 then
                UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
                primera_col := 0;
              else
                UTL_FILE.put_line(fich_salida_pkg, '    ,' || reg_detail.TABLE_COLUMN);
              end if;        
            end if;
            /* (20200303) Angel Ruiz. FIN NF: Nueva regla AUTO para no tener que usar las secuencias*/
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,'  )');
          dbms_output.put_line ('He pasado la parte del INTO');
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          UTL_FILE.put_line(fich_salida_pkg,'  SELECT');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
            if (upper(trim(reg_detail.RUL)) != 'AUTO') then
            
              dbms_output.put_line ('Antes de la llamada a la funcion con columna: ' || reg_detail.TABLE_COLUMN);
              columna := genera_campo_select (reg_detail);
              if primera_col = 1 then
                UTL_FILE.put_line(fich_salida_pkg, '    ' || columna || ' ' || reg_detail.TABLE_COLUMN);
                primera_col := 0;
              else
                UTL_FILE.put_line(fich_salida_pkg, '    , ' || columna || ' ' || reg_detail.TABLE_COLUMN);
              end if;
            end if;
            /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
          end loop;
          close MTDT_TC_DETAIL;
          /****/
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/ 
          /****/
          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          /****/
          dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
          UTL_FILE.put_line(fich_salida_pkg,'  FROM');
          /* (20170104) Angel Ruiz. Pueden aparecer Queries en TABLE_BASE_NAME */
          if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt] ') > 0) then
            /* Tenemos una query en TABLE_BASE_NAME */
            /* Es una query que posee un ALIAS */
            v_alias_dim_table_base_name := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$'), 2));
            --UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || ' LEFT OUTER JOIN ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
            UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || ' LEFT OUTER JOIN ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
          else
            /* No hay una query en TABLE_BASE_NAME*/
            /* COMPROBAMOS SI AUNQUE NO HAY QUERY, HAY UN ALIAS */
            if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
              /* Hay un ALIAS */
              v_alias_dim_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_scenario.TABLE_BASE_NAME), ' +[a-zA-Z_0-9]+$'));
            else
              v_alias_dim_table_base_name:=reg_scenario.TABLE_BASE_NAME;
            end if;
            UTL_FILE.put_line(fich_salida_pkg, '  ' || OWNER_DM || '.' || reg_scenario.TABLE_BASE_NAME || ' LEFT OUTER JOIN ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
          end if;
          /* (20170104) Angel Ruiz. FIN. Pueden aparecer Queries en TABLE_BASE_NAME */
          dbms_output.put_line ('Interface COLUMNS: ' || reg_scenario.INTERFACE_COLUMNS);
          dbms_output.put_line ('Table COLUMNS: ' || reg_scenario.TABLE_COLUMNS);
          where_interface_columns := split_string_punto_coma (reg_scenario.INTERFACE_COLUMNS);
          where_table_columns := split_string_punto_coma(reg_scenario.TABLE_COLUMNS);
          dbms_output.put_line ('El numero de valores del Where interface es: ' || where_interface_columns.count);
          dbms_output.put_line ('El numero de valores del Where interface es: ' || where_table_columns.count);
          /* (20161205) Angel Ruiz. Generacion de Dimensiones BIG DATA */
          IF (where_interface_columns.COUNT > 0  and 
            where_table_columns.COUNT > 0 and 
            where_interface_columns.COUNT = where_table_columns.COUNT) 
          THEN
            /****/
            /* INICIO generacion parte  WHERE */
            /****/    
            --UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
            /* Procesamos el campo FILTER . Lo añado a posteriori en la recta final (20141126*/
            FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
            LOOP
              if indx = 1 then  /* Se trata del primer elemento */
                /* (20160301) Angel Ruiz. NF: DECODE en campos */
                if (regexp_instr(where_table_columns(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 ) then
                  --UTL_FILE.put_line(fich_salida_pkg,'    ' || transformo_decode(where_interface_columns(indx), reg_detail.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(where_table_columns(indx), reg_detail.TABLE_NAME, 0));
                  UTL_FILE.put_line(fich_salida_pkg,'  ' || transformo_decode(where_interface_columns(indx), v_alias_dim_table_base_name, 0) || ' = ' || transformo_decode(where_table_columns(indx), reg_scenario.TABLE_NAME, 0));
                else
                  /* (20191219) Angel Ruiz. BUG. Pone un punto cuando no deberia ponerlo*/
                  if (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') = 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') = 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  else
                    --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_detail.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_detail.TABLE_NAME || '.' || where_table_columns(indx));
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  end if;
                  /* (20191219) Angel Ruiz. FIN */
                
                  --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_detail.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_detail.TABLE_NAME || '.' || where_table_columns(indx));
                  --UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                end if;
              else
                if (regexp_instr(where_table_columns(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 ) then
                  UTL_FILE.put_line(fich_salida_pkg,'  AND ' || transformo_decode(where_interface_columns(indx), v_alias_dim_table_base_name, 0) || ' = ' || transformo_decode(where_table_columns(indx), reg_scenario.TABLE_NAME, 0));
                else
                  /* (20191219) Angel Ruiz. BUG. Pone un punto cuando no deberia ponerlo*/
                  if (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') = 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') = 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  else
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  end if;
                  /* (20191219) Angel Ruiz. FIN */
                
                  --UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                end if;
              end if;
            END LOOP;
            UTL_FILE.put_line (fich_salida_pkg,'    )');
          end if;
          /* (20160630) Angel Ruiz. NF: Dimensiones sin funciones cache, es decir, con JOINS */
          v_hay_look_up:='N';
          if l_FROM.count > 0 then
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
              v_hay_look_up := 'Y';
            END LOOP;
          end if;
          /* FIN */
          
          dbms_output.put_line ('Despues del FROM');
          UTL_FILE.put_line(fich_salida_pkg, '  WHERE ');
          dbms_output.put_line ('Despues del WHERE');          
          if (reg_scenario.FILTER is not null) then
            /* Añadimos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            --UTL_FILE.put_line(fich_salida_pkg, '    AND ' || campo_filter || ';');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || campo_filter );
            UTL_FILE.put_line(fich_salida_pkg, '  AND '  || reg_scenario.TABLE_NAME || '.' || where_table_columns ( where_table_columns.FIRST) || ' IS NULL');
          else
            dbms_output.put_line ('El campo reg_scenario.FILTER está nulo.');
            UTL_FILE.put_line(fich_salida_pkg, '      '  || reg_scenario.TABLE_NAME || '.' || where_table_columns ( where_table_columns.FIRST) || ' IS NULL');
            dbms_output.put_line ('Después de escribir la comprobación de que el campo FILTER está nulo.');
          end if;
          dbms_output.put_line ('Una línea mas.');
          UTL_FILE.put_line(fich_salida_pkg,'  ;');
          dbms_output.put_line ('Dos línea mas.');
          UTL_FILE.put_line(fich_salida_pkg,'  GET DIAGNOSTICS num_filas_insertadas := ROW_COUNT;');
          dbms_output.put_line ('Tres línea mas.');
          UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_insertadas;');
          dbms_output.put_line ('Cuatro línea mas.');
          UTL_FILE.put_line(fich_salida_pkg,'  EXCEPTION');
          dbms_output.put_line ('Cinco línea mas.');
          UTL_FILE.put_line(fich_salida_pkg,'  WHEN OTHERS THEN');
          dbms_output.put_line ('Seis línea mas.');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Se ha producido un error en la inserción del escenario NUEVO: ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ''';');
          dbms_output.put_line ('Siete línea mas.');
          UTL_FILE.put_line(fich_salida_pkg,'    GET STACKED DIAGNOSTICS v_error_code := RETURNED_SQLSTATE, v_error_msg := MESSAGE_TEXT;');
          dbms_output.put_line ('Ocho línea mas.');          
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Error code: %. Mensaje: %'', v_error_code, v_error_msg;');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE EXCEPTION USING ERRCODE = v_error_code, MESSAGE = v_error_msg;');
          --UTL_FILE.put_line(fich_salida_pkg,'    RAISE;');
          UTL_FILE.put_line(fich_salida_pkg,'END;');
          UTL_FILE.put_line(fich_salida_pkg,'$$ LANGUAGE plpgsql;');
          UTL_FILE.put_line(fich_salida_pkg, '');
        end if;
      end if;
      end loop;
      close MTDT_SCENARIO;
      

      /** COMIENZO  ESCENARIO EXISTENTE **/

      open MTDT_SCENARIO (reg_scenario.TABLE_NAME);
      loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      if (reg_scenario.TABLE_COLUMNS is not null and reg_scenario.INTERFACE_COLUMNS is not null) then
        --if (reg_scenario.SCENARIO like 'E%')
        if (reg_scenario.SCENARIO = 'E' or regexp_instr(reg_scenario.SCENARIO, '^E_.+') > 0 or regexp_instr(reg_scenario.SCENARIO, '.+_E$') > 0)
        then
          /* (20160701) Angel Ruiz. BUG: Debo borrar en cada escenario las listas de */
          /* componentes del From y del Where */
          l_FROM.delete;
          l_WHERE.delete;
          l_FROM_solo_tablas.delete;
          lista_variables_rownumber.delete;
          v_num_sce_EXISTENTES := v_num_sce_EXISTENTES + 1; /* (20170315) Angel Ruiz. NF: Pueden venir varios escenarios existentes */
          
          /* ESCENARIO EXISTENTE */
          dbms_output.put_line ('Estoy en el escenario: E');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, '-- ### ESCENARIO EXISTENTE: ' || reg_scenario.SCENARIO || ' ###');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, 'DROP FUNCTION IF EXISTS ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');          
          UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8))');
          UTL_FILE.put_line(fich_salida_pkg, 'RETURNS integer AS $$');
          UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_upd INTEGER := 0;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio TIMESTAMP := CURRENT_TIMESTAMP;');
          UTL_FILE.put_line(fich_salida_pkg, '  v_error_code text;');
          UTL_FILE.put_line(fich_salida_pkg, '  v_error_msg text;');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'  INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'  INTO ' || OWNER_DM || '.' || nombre_proceso || '_T');
          UTL_FILE.put_line(fich_salida_pkg,'  (');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if primera_col = 1 then
              --UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
              UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
              primera_col := 0;
            else
              --UTL_FILE.put_line(fich_salida_pkg, ' ,' || reg_detail.TABLE_COLUMN);
              UTL_FILE.put_line(fich_salida_pkg, ' ,' || reg_detail.TABLE_COLUMN);
            end if;        
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,'  )');
          
          dbms_output.put_line ('He pasado la parte del INTO');
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          UTL_FILE.put_line(fich_salida_pkg,'  SELECT');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            dbms_output.put_line ('Antes de la llamada a la funcion con columna: ' || reg_detail.TABLE_COLUMN);
            columna := genera_campo_select (reg_detail);
            if primera_col = 1 then
              --UTL_FILE.put_line(fich_salida_pkg, '  ' || columna || ' ' || reg_detail.TABLE_COLUMN);
              UTL_FILE.put_line(fich_salida_pkg, '    ' || columna || ' ' || reg_detail.TABLE_COLUMN);
              primera_col := 0;
            else
              --UTL_FILE.put_line(fich_salida_pkg, '  ,' || columna || ' ' || reg_detail.TABLE_COLUMN);
              UTL_FILE.put_line(fich_salida_pkg, '    , ' || columna || ' ' || reg_detail.TABLE_COLUMN);
            end if;        
          end loop;
          close MTDT_TC_DETAIL;
          /****/
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/ 
          /****/
          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          /****/
          dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
          UTL_FILE.put_line(fich_salida_pkg,'FROM');
          /* (20170104) Angel Ruiz. Pueden aparecer Queries en TABLE_BASE_NAME */
          if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt] ') > 0) then
            /* Tenemos una query en TABLE_BASE_NAME */
            /* Es una query que posee un ALIAS */
            v_alias_dim_table_base_name := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$'), 2));
            --UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || ' JOIN ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
            UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || ' JOIN ' || ESQUEMA_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
          else
            /* No hay una query en TABLE_BASE_NAME*/
            /* COMPROBAMOS SI AUNQUE NO HAY QUERY, HAY UN ALIAS */
            if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
              /* Hay un ALIAS */
              v_alias_dim_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_scenario.TABLE_BASE_NAME), ' +[a-zA-Z_0-9]+$'));
            else
              v_alias_dim_table_base_name:=reg_scenario.TABLE_BASE_NAME;
            end if;
            --UTL_FILE.put_line(fich_salida_pkg, '    ' || ESQUEMA_DM || '.' || reg_scenario.TABLE_BASE_NAME || ' JOIN ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
            UTL_FILE.put_line(fich_salida_pkg, '    ' || ESQUEMA_DM || '.' || reg_scenario.TABLE_BASE_NAME || ' JOIN ' || ESQUEMA_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
          end if;
          /* (20170104) Angel Ruiz. FIN. Pueden aparecer Queries en TABLE_BASE_NAME */
          /* (20161205) Angel Ruiz. Dimensiones en BIG DATA */
          dbms_output.put_line ('Interface COLUMNS: ' || reg_scenario.INTERFACE_COLUMNS);
          dbms_output.put_line ('Table COLUMNS: ' || reg_scenario.TABLE_COLUMNS);
          where_interface_columns := split_string_punto_coma (reg_scenario.INTERFACE_COLUMNS);
          where_table_columns := split_string_punto_coma(reg_scenario.TABLE_COLUMNS);
          dbms_output.put_line ('El numero de valores del Where interface es: ' || where_interface_columns.count);
          dbms_output.put_line ('El numero de valores del Where interface es: ' || where_table_columns.count);
    
          IF (where_interface_columns.COUNT > 0  and 
            where_table_columns.COUNT > 0 and 
            where_interface_columns.COUNT = where_table_columns.COUNT) 
          THEN
            /****/
            /* INICIO generacion parte  WHERE */
            /****/    
            --UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
            FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
            LOOP
              if indx = 1 then  /* (20161205) Angel Ruiz. Se trata del campo primero */
                if (regexp_instr(where_table_columns(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 ) then
                  UTL_FILE.put_line(fich_salida_pkg,'  ' || transformo_decode (where_interface_columns(indx), v_alias_dim_table_base_name, 0) || ' = ' || transformo_decode (where_table_columns(indx), reg_scenario.TABLE_NAME, 0));
                else
                  /* (20191219) Angel Ruiz. BUG. Pone un punto cuando no deberia ponerlo*/
                  if (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') = 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') = 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  else
                    --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_detail.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_detail.TABLE_NAME || '.' || where_table_columns(indx));
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  end if;
                  /* (20191219) Angel Ruiz. FIN */
                
                  --UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                end if;
              else
                /* (20160301) Angel Ruiz. NF: DECODE en campos */
                if (regexp_instr(where_table_columns(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 ) then
                  UTL_FILE.put_line(fich_salida_pkg,'  AND ' || transformo_decode (where_interface_columns(indx), v_alias_dim_table_base_name, 0)  || ' = ' || transformo_decode (where_table_columns(indx), reg_scenario.TABLE_NAME, 0));
                else
                  /* (20191219) Angel Ruiz. BUG. Pone un punto cuando no deberia ponerlo*/
                  if (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') = 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') = 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  else
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  end if;
                  /* (20191219) Angel Ruiz. FIN */
                
                  --UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                end if;
              end if;
            END LOOP;
            UTL_FILE.put_line(fich_salida_pkg, '        )');
          END IF;

          /* (20160630) Angel Ruiz. NF: Dimensiones sin funciones cache */
          v_hay_look_up:='N';
          if l_FROM.count > 0 then
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
              v_hay_look_up := 'Y';
            END LOOP;
          end if;
          /* FIN */
          dbms_output.put_line ('Despues del FROM');
          
          if (reg_scenario.FILTER is not null) then
            UTL_FILE.put_line(fich_salida_pkg, 'WHERE ');
            /* Añadimos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, '    ' || campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg,'    ;');
          UTL_FILE.put_line(fich_salida_pkg,'  GET DIAGNOSTICS num_filas_upd := ROW_COUNT;');
          UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_upd;');
          UTL_FILE.put_line(fich_salida_pkg,'  EXCEPTION');
          UTL_FILE.put_line(fich_salida_pkg,'  WHEN OTHERS THEN');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Se ha producido un error en la inserción del escenario EXISTENTE: ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ''';');          
          UTL_FILE.put_line(fich_salida_pkg,'    GET STACKED DIAGNOSTICS v_error_code := RETURNED_SQLSTATE, v_error_msg := MESSAGE_TEXT;');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Error code: %. Mensaje: %'', v_error_code, v_error_msg;');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE EXCEPTION USING ERRCODE = v_error_code, MESSAGE = v_error_msg;');
          --UTL_FILE.put_line(fich_salida_pkg,'    RAISE;');
          UTL_FILE.put_line(fich_salida_pkg,'END;');
          UTL_FILE.put_line(fich_salida_pkg,'$$ LANGUAGE plpgsql;');
          UTL_FILE.put_line(fich_salida_pkg, '');
        end if;
      end if;
      end loop;
      close MTDT_SCENARIO;

      open MTDT_SCENARIO (reg_scenario.TABLE_NAME);
      loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
        /** COMIENZO  ESCENARIO HISTORICO **/
      if (reg_scenario.TABLE_COLUMNS is not null and reg_scenario.INTERFACE_COLUMNS is not null) then
        --if (reg_scenario.SCENARIO like 'H%')
        if (reg_scenario.SCENARIO = 'H' or regexp_instr(reg_scenario.SCENARIO, '^H_.+') > 0 or regexp_instr(reg_scenario.SCENARIO, '.+_H$') > 0)
        then
          /* (20160701) Angel Ruiz. BUG: Debo borrar en cada escenario las listas de */
          /* componentes del From y del Where */
          l_FROM.delete;
          l_WHERE.delete;
          l_FROM_solo_tablas.delete;
          lista_variables_rownumber.delete;
          v_num_sce_HISTORICOS := v_num_sce_HISTORICOS + 1; /* (20170315) Angel Ruiz. NF: Pueden venir varios escenarios historicos */
          
          /* ESCENARIO HISTORICO */
          dbms_output.put_line ('Estoy en el escenario: H');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, '-- ### ESCENARIO HISTORICO: ' || reg_scenario.SCENARIO || ' ###');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, 'DROP FUNCTION IF EXISTS ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in VARCHAR(8))');
          UTL_FILE.put_line(fich_salida_pkg, 'RETURNS integer AS $$');
          UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_hst INTEGER := 0;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio TIMESTAMP := CURRENT_TIMESTAMP;');
          UTL_FILE.put_line(fich_salida_pkg, '  v_error_code text;');
          UTL_FILE.put_line(fich_salida_pkg, '  v_error_msg text;');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg, '  INSERT');
          UTL_FILE.put_line(fich_salida_pkg, '  INTO ' || OWNER_DM || '.' || nombre_proceso || '_T');
          UTL_FILE.put_line(fich_salida_pkg,'  (');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if primera_col = 1 then
              --UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
              UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
              primera_col := 0;
            else
              --UTL_FILE.put_line(fich_salida_pkg, ' ,' || reg_detail.TABLE_COLUMN);
              UTL_FILE.put_line(fich_salida_pkg, ' ,' || reg_detail.TABLE_COLUMN);
            end if;        
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,'  )');
          
          dbms_output.put_line ('He pasado la parte del INTO');
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          UTL_FILE.put_line(fich_salida_pkg,'SELECT');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            dbms_output.put_line ('Antes de la llamada a la funcion con columna: ' || reg_detail.TABLE_COLUMN);
            columna := genera_campo_select (reg_detail);
            if primera_col = 1 then
              --UTL_FILE.put_line(fich_salida_pkg, '  ' || columna || ' ' || reg_detail.TABLE_COLUMN);
              UTL_FILE.put_line(fich_salida_pkg, '    ' || columna || ' ' || reg_detail.TABLE_COLUMN);
              primera_col := 0;
            else
              UTL_FILE.put_line(fich_salida_pkg, '    , ' || columna || ' ' || reg_detail.TABLE_COLUMN);
            end if;        
          end loop;
          close MTDT_TC_DETAIL;
          /****/
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/ 
          /****/
          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          /****/
          dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
          UTL_FILE.put_line(fich_salida_pkg,'FROM');
          /* (20170104) Angel Ruiz. Pueden aparecer Queries en TABLE_BASE_NAME */
          if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt] ') > 0) then
            /* Tenemos una query en TABLE_BASE_NAME */
            /* Es una query que posee un ALIAS */
            v_alias_dim_table_base_name := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$'), 2));
            UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || ' RIGHT OUTER JOIN ' || ESQUEMA_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
          else
            /* No hay una query en TABLE_BASE_NAME*/
            /* COMPROBAMOS SI AUNQUE NO HAY QUERY, HAY UN ALIAS */
            if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
              /* Hay un ALIAS */
              v_alias_dim_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_scenario.TABLE_BASE_NAME), ' +[a-zA-Z_0-9]+$'));
            else
              v_alias_dim_table_base_name:=reg_scenario.TABLE_BASE_NAME;
            end if;
            UTL_FILE.put_line(fich_salida_pkg, '  ' || ESQUEMA_DM || '.' || reg_scenario.TABLE_BASE_NAME || ' RIGHT OUTER JOIN ' || ESQUEMA_DM || '.' || reg_scenario.TABLE_NAME || ' ON (');
          end if;
          /* (20170104) Angel Ruiz. FIN. Pueden aparecer Queries en TABLE_BASE_NAME */
          /* (20161106) Angel Ruiz. Se trata de formar la clausula ON del RIGHT OUTER JOIN, ya que estamos en el escenario Historico */
          dbms_output.put_line ('Interface COLUMNS: ' || reg_scenario.INTERFACE_COLUMNS);
          dbms_output.put_line ('Table COLUMNS: ' || reg_scenario.TABLE_COLUMNS);
          where_interface_columns := split_string_punto_coma (reg_scenario.INTERFACE_COLUMNS);
          where_table_columns := split_string_punto_coma(reg_scenario.TABLE_COLUMNS);
          dbms_output.put_line ('El numero de valores del Where interface es: ' || where_interface_columns.count);
          dbms_output.put_line ('El numero de valores del Where interface es: ' || where_table_columns.count);
    
          IF (where_interface_columns.COUNT > 0  and 
            where_table_columns.COUNT > 0 and 
            where_interface_columns.COUNT = where_table_columns.COUNT) 
          THEN
            /****/
            /* INICIO generacion parte  WHERE */
            /****/    
            --UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
            /* Procesamos el campo FILTER . Lo añado a posteriori en la recta final (20141126*/
            FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
            LOOP
              /* (20160301) Angel Ruiz. NF: DECODE en campos */
              if (indx = 1) then /* (20161106. Angel Ruiz. Se trata del primer elemento del ON */
                if (instr(where_table_columns(indx), 'DECODE') > 0 or instr(where_table_columns(indx), 'decode') > 0) then
                  UTL_FILE.put_line(fich_salida_pkg,'    ' || transformo_decode (where_table_columns(indx), reg_scenario.TABLE_NAME, 0) || ' = ' || transformo_decode (where_interface_columns(indx), v_alias_dim_table_base_name, 0));
                else
                  /* (20191219) Angel Ruiz. BUG. Pone un punto cuando no deberia ponerlo*/
                  if (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') = 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') = 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  else
                    --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_detail.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_detail.TABLE_NAME || '.' || where_table_columns(indx));
                    UTL_FILE.put_line(fich_salida_pkg,'  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  end if;
                  /* (20191219) Angel Ruiz. FIN */
                
                  --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx) || ' = ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx));
                end if;
              else
                if (instr(where_table_columns(indx), 'DECODE') > 0 or instr(where_table_columns(indx), 'decode') > 0) then
                  UTL_FILE.put_line(fich_salida_pkg,'  AND ' || transformo_decode (where_table_columns(indx), reg_scenario.TABLE_NAME, 0) || ' = ' || transformo_decode (where_interface_columns(indx), v_alias_dim_table_base_name, 0));
                else
                  /* (20191219) Angel Ruiz. BUG. Pone un punto cuando no deberia ponerlo*/
                  if (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') > 0 and instr(where_table_columns(indx), '.') = 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  elsif (instr(where_interface_columns(indx), '.') = 0 and instr(where_table_columns(indx), '.') > 0) then
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || where_table_columns(indx));
                  else
                    UTL_FILE.put_line(fich_salida_pkg,'  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                  end if;
                  /* (20191219) Angel Ruiz. FIN */

                  --UTL_FILE.put_line(fich_salida_pkg,'  AND ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx) || ' = ' || v_alias_dim_table_base_name || '.' || where_interface_columns(indx));
                end if;
              end if;
            END LOOP;
            UTL_FILE.put_line(fich_salida_pkg, '    )');
            --UTL_FILE.put_line(fich_salida_pkg, '    AND ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(where_interface_columns.FIRST) || ' IS NULL)' );
          END IF;
          
          /* (20160630) Angel Ruiz. NF: Dimensiones sin funciones cache */
          v_hay_look_up:='N';
          if l_FROM.count > 0 then
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
              v_hay_look_up := 'Y';
            END LOOP;
          end if;
          /* FIN */
          dbms_output.put_line ('Despues del FROM');
          UTL_FILE.put_line(fich_salida_pkg, '  WHERE ');
          --UTL_FILE.put_line(fich_salida_pkg, '    ' || v_alias_dim_table_base_name || '.' || where_interface_columns(where_interface_columns.FIRST) || ' IS NULL' );
          if (reg_scenario.FILTER is not null) then
            /* Anadimos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            --UTL_FILE.put_line(fich_salida_pkg, '    AND ' || campo_filter);
            UTL_FILE.put_line(fich_salida_pkg, '      ' || campo_filter);
            UTL_FILE.put_line(fich_salida_pkg, '  AND ' || v_alias_dim_table_base_name || '.' || where_interface_columns(where_interface_columns.FIRST) || ' IS NULL' );
          else
            UTL_FILE.put_line(fich_salida_pkg, '  ' || v_alias_dim_table_base_name || '.' || where_interface_columns(where_interface_columns.FIRST) || ' IS NULL' );
          end if;
          UTL_FILE.put_line(fich_salida_pkg,'  ;');
          UTL_FILE.put_line(fich_salida_pkg,'  GET DIAGNOSTICS num_filas_hst := ROW_COUNT;');
          UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_hst;');
          UTL_FILE.put_line(fich_salida_pkg,'  EXCEPTION');
          UTL_FILE.put_line(fich_salida_pkg,'  WHEN OTHERS THEN');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Se ha producido un error en la inserción del escenario HISTÓRICO: ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ''';');          
          UTL_FILE.put_line(fich_salida_pkg,'    GET STACKED DIAGNOSTICS v_error_code := RETURNED_SQLSTATE, v_error_msg := MESSAGE_TEXT;');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Error code: %. Mensaje: %'', v_error_code, v_error_msg;');
          UTL_FILE.put_line(fich_salida_pkg,'    RAISE EXCEPTION USING ERRCODE = v_error_code, MESSAGE = v_error_msg;');
          --UTL_FILE.put_line(fich_salida_pkg,'    RAISE;');
          UTL_FILE.put_line(fich_salida_pkg,'END;');
          UTL_FILE.put_line(fich_salida_pkg,'$$ LANGUAGE plpgsql;');
          UTL_FILE.put_line(fich_salida_pkg, '');
          
        end if;

/*******************************************/
/*******************************************/
      end if;
      end loop;
      close MTDT_SCENARIO;


      
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
      loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      /* (20200128) Angel Ruiz. Se trata de escenarios que no se corresponden con los tipicos escenarios N, E, H */
      if (reg_scenario.TABLE_COLUMNS is null and reg_scenario.INTERFACE_COLUMNS is null) then
          if (reg_scenario.SCENARIO = 'I' or regexp_instr(reg_scenario.SCENARIO, '^I_.+') > 0 or regexp_instr(reg_scenario.SCENARIO, '.+_I$') > 0)
          then
            /* Tenemos el escenario Integracon (I) */
            /* En este escenario se produce un truncado de la tabla y una carga de registros desde TABLE_BASE_NAME */
            /* (20160701) Angel Ruiz. BUG: Debo borrar en cada escenario las listas de */
            /* componentes del From y del Where */
            l_FROM.delete;
            l_WHERE.delete;
            l_FROM_solo_tablas.delete;
            lista_variables_rownumber.delete;
            
            v_hay_regla_seq:=false;
            dbms_output.put_line ('Estoy dentro del scenario $' || reg_scenario.SCENARIO || '$');
            v_TABLE_BASE_NAME_SCENARIO_I := procesa_campo_filter(reg_scenario.TABLE_BASE_NAME);
            v_TABLE_NAME_SCENARIO_I := reg_scenario.TABLE_NAME;
            V_EXISTE_ESCENARIO_I := true;
            UTL_FILE.put_line(fich_salida_pkg, '-- ### ESCENARIO ' || reg_scenario.SCENARIO || ' ###');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, 'DROP FUNCTION IF EXISTS ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');
            UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8))');
            UTL_FILE.put_line(fich_salida_pkg, 'RETURNS integer AS $$');            
            UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
            UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas INTEGER := 0;');
            UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio TIMESTAMP := CURRENT_TIMESTAMP;');
            UTL_FILE.put_line(fich_salida_pkg, '  v_error_code text;');
            UTL_FILE.put_line(fich_salida_pkg, '  v_error_msg text;');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');            
            UTL_FILE.put_line(fich_salida_pkg,'INSERT');
            --UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in ||');
            UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_DM || '.' || nombre_proceso || '_T');
            /****/
            /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
            /****/
            UTL_FILE.put_line(fich_salida_pkg,'  (');
            open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
            primera_col := 1;
            loop
              fetch MTDT_TC_DETAIL
              into reg_detail;
              exit when MTDT_TC_DETAIL%NOTFOUND;
              /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
              if (upper(trim(reg_detail.RUL)) != 'AUTO') then
                if primera_col = 1 then
                  UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
                  primera_col := 0;
                else
                  UTL_FILE.put_line(fich_salida_pkg, '  ,' || reg_detail.TABLE_COLUMN);
                end if;
              end if;
              /* (20200303) Angel Ruiz. FIN NF: Nueva regla AUTO para no tener que usar las secuencias*/
            end loop;
            close MTDT_TC_DETAIL;
            UTL_FILE.put_line(fich_salida_pkg,'  )');
            
            dbms_output.put_line ('He pasado la parte del INTO');
            /****/
            /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
            /****/
            /****/
            /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
            /****/
            UTL_FILE.put_line(fich_salida_pkg,'SELECT ');
            open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
            primera_col := 1;
            loop
              fetch MTDT_TC_DETAIL
              into reg_detail;
              exit when MTDT_TC_DETAIL%NOTFOUND;
              /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
              if (upper(trim(reg_detail.RUL)) != 'AUTO') then
                columna := genera_campo_select (reg_detail);
                if primera_col = 1 then
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || columna || ' ' || reg_detail.TABLE_COLUMN);
                  primera_col := 0;
                else
                  UTL_FILE.put_line(fich_salida_pkg, '    , ' || columna || ' ' || reg_detail.TABLE_COLUMN);
                end if;
              end if;
              /* (20200303) Angel Ruiz. FIN NF: Nueva regla AUTO para no tener que usar las secuencias*/              
            end loop;
            close MTDT_TC_DETAIL;
            /****/
            /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
            /****/      
            /****/
            /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
            /****/    
            dbms_output.put_line ('Despues del SELECT');
            UTL_FILE.put_line(fich_salida_pkg,'FROM');
            --UTL_FILE.put_line(fich_salida_pkg, '   app_mvnosa.'  || reg_scenario.TABLE_BASE_NAME || ''' || ''_'' || fch_datos_in;');
            UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_scenario.TABLE_BASE_NAME));
            /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
            v_hay_look_up:='N';
            /* (20150311) ANGEL RUIZ. se produce un error al generar ya que la tabla de hechos no tiene tablas de LookUp */
            if l_FROM.count > 0 then
              FOR indx IN l_FROM.FIRST .. l_FROM.LAST
              LOOP
                UTL_FILE.put_line(fich_salida_pkg, l_FROM(indx));
                v_hay_look_up := 'Y';
              END LOOP;
            end if;
            /* FIN */
            --UTL_FILE.put_line(fich_salida_pkg,'    ' || v_FROM);
            dbms_output.put_line ('Despues del FROM');
            
            if (reg_scenario.FILTER is not null) then
              /* Procesamos el campo FILTER */
              UTL_FILE.put_line(fich_salida_pkg,'WHERE');
              dbms_output.put_line ('Antes de procesar el campo FILTER');
              --campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
              campo_filter := procesa_campo_filter(reg_scenario.FILTER);
              UTL_FILE.put_line(fich_salida_pkg, campo_filter);
            end if;
            dbms_output.put_line ('Despues de procesar el campo FILTER');
            UTL_FILE.put_line(fich_salida_pkg, ';');
            UTL_FILE.put_line(fich_salida_pkg,'  GET DIAGNOSTICS num_filas_upd := ROW_COUNT;');
            UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_upd;');
            UTL_FILE.put_line(fich_salida_pkg,'  EXCEPTION');
            UTL_FILE.put_line(fich_salida_pkg,'  WHEN OTHERS THEN');
            UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Se ha producido un error en la inserción del escenario INTEGRACIÓN: ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ''';');
            UTL_FILE.put_line(fich_salida_pkg,'    GET STACKED DIAGNOSTICS v_error_code := RETURNED_SQLSTATE, v_error_msg := MESSAGE_TEXT;');
            UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Error code: %. Mensaje: %'', v_error_code, v_error_msg;');
            UTL_FILE.put_line(fich_salida_pkg,'    RAISE EXCEPTION USING ERRCODE = v_error_code, MESSAGE = v_error_msg;');
            --UTL_FILE.put_line(fich_salida_pkg,'    RAISE;');
            UTL_FILE.put_line(fich_salida_pkg,'END;');
            UTL_FILE.put_line(fich_salida_pkg,'$$ LANGUAGE plpgsql;');
            UTL_FILE.put_line(fich_salida_pkg, '');
          else
            /* (20190128) Angel Ruiz. Cualquier escenario que no sea I */
            l_FROM.delete;
            l_WHERE.delete;
            l_FROM_solo_tablas.delete;
            lista_variables_rownumber.delete;
            
            v_hay_regla_seq:=false;
            dbms_output.put_line ('Estoy dentro del scenario $' || reg_scenario.SCENARIO || '$');
            UTL_FILE.put_line(fich_salida_pkg, '-- ### ESCENARIO ' || reg_scenario.SCENARIO || ' ###');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, 'DROP FUNCTION IF EXISTS ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');
            UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_DM || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8))');
            UTL_FILE.put_line(fich_salida_pkg, 'RETURNS integer AS $$');
            UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
            UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas integer := 0;');
            UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio TIMESTAMP := CURRENT_TIMESTAMP;');
            UTL_FILE.put_line(fich_salida_pkg, '  v_error_code text;');
            UTL_FILE.put_line(fich_salida_pkg, '  v_error_msg text;');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');            
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg,'INSERT');
            --UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in ||');
            UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_DM || '.' || nombre_proceso || '_T');
            /****/
            /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
            /****/
            UTL_FILE.put_line(fich_salida_pkg,'  (');
            open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
            primera_col := 1;
            loop
              fetch MTDT_TC_DETAIL
              into reg_detail;
              exit when MTDT_TC_DETAIL%NOTFOUND;
              /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
              if (upper(trim(reg_detail.RUL)) != 'AUTO') then
                if primera_col = 1 then
                  UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
                  primera_col := 0;
                else
                  UTL_FILE.put_line(fich_salida_pkg, '  ,' || reg_detail.TABLE_COLUMN);
                end if;
              end if;
              /* (20200303) Angel Ruiz. FIN NF: Nueva regla AUTO para no tener que usar las secuencias*/
            end loop;
            close MTDT_TC_DETAIL;
            UTL_FILE.put_line(fich_salida_pkg,'  )');
            
            dbms_output.put_line ('He pasado la parte del INTO');
            /****/
            /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
            /****/
            /****/
            /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
            /****/
            UTL_FILE.put_line(fich_salida_pkg,'  SELECT ');
            open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
            primera_col := 1;
            loop
              fetch MTDT_TC_DETAIL
              into reg_detail;
              exit when MTDT_TC_DETAIL%NOTFOUND;
              /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
              if (upper(trim(reg_detail.RUL)) != 'AUTO') then
                columna := genera_campo_select (reg_detail);
                if primera_col = 1 then
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || columna || ' ' || reg_detail.TABLE_COLUMN);
                  primera_col := 0;
                else
                  UTL_FILE.put_line(fich_salida_pkg, '    , ' || columna || ' ' || reg_detail.TABLE_COLUMN);
                end if;
              end if;
              /* (20200303) Angel Ruiz. NF: Nueva regla AUTO para no tener que usar las secuencias*/
            end loop;
            close MTDT_TC_DETAIL;
            /****/
            /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
            /****/      
            /****/
            /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
            /****/    
            dbms_output.put_line ('Despues del SELECT');
            UTL_FILE.put_line(fich_salida_pkg,'FROM');
            --UTL_FILE.put_line(fich_salida_pkg, '   app_mvnosa.'  || reg_scenario.TABLE_BASE_NAME || ''' || ''_'' || fch_datos_in;');
            UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_scenario.TABLE_BASE_NAME));
            /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
            v_hay_look_up:='N';
            /* (20150311) ANGEL RUIZ. se produce un error al generar ya que la tabla de hechos no tiene tablas de LookUp */
            if l_FROM.count > 0 then
              FOR indx IN l_FROM.FIRST .. l_FROM.LAST
              LOOP
                UTL_FILE.put_line(fich_salida_pkg, l_FROM(indx));
                v_hay_look_up := 'Y';
              END LOOP;
            end if;
            /* FIN */
            --UTL_FILE.put_line(fich_salida_pkg,'    ' || v_FROM);
            dbms_output.put_line ('Despues del FROM');
            
            if (reg_scenario.FILTER is not null) then
              /* Procesamos el campo FILTER */
              UTL_FILE.put_line(fich_salida_pkg,'WHERE');
              dbms_output.put_line ('Antes de procesar el campo FILTER');
              --campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
              campo_filter := procesa_campo_filter(reg_scenario.FILTER);
              UTL_FILE.put_line(fich_salida_pkg, campo_filter);
            end if;
            dbms_output.put_line ('Despues de procesar el campo FILTER');
            UTL_FILE.put_line(fich_salida_pkg, ';');
            UTL_FILE.put_line(fich_salida_pkg,'  GET DIAGNOSTICS num_filas_upd := ROW_COUNT;');
            UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_upd;');
            UTL_FILE.put_line(fich_salida_pkg,'  EXCEPTION');
            UTL_FILE.put_line(fich_salida_pkg,'  WHEN OTHERS THEN');
            UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Se ha producido un error en la inserción del escenario INTEGRACIÓN: ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ''';');
            UTL_FILE.put_line(fich_salida_pkg,'    GET STACKED DIAGNOSTICS v_error_code := RETURNED_SQLSTATE, v_error_msg := MESSAGE_TEXT;');
            UTL_FILE.put_line(fich_salida_pkg,'    RAISE NOTICE ''Error code: %. Mensaje: %'', v_error_code, v_error_msg;');
            UTL_FILE.put_line(fich_salida_pkg,'    RAISE EXCEPTION USING ERRCODE = v_error_code, MESSAGE = v_error_msg;');
            --UTL_FILE.put_line(fich_salida_pkg,'    RAISE;');
            UTL_FILE.put_line(fich_salida_pkg,'END;');
            UTL_FILE.put_line(fich_salida_pkg,'$$ LANGUAGE plpgsql;');
            UTL_FILE.put_line(fich_salida_pkg, '');
          end if;
      end if;
      end loop;
      close MTDT_SCENARIO;

/*******************************************/
/*******************************************/
      UTL_FILE.put_line(fich_salida_pkg, 'DROP PROCEDURE IF EXISTS ' || OWNER_DM || '.prc_' || nombre_proceso || ';');
      UTL_FILE.put_line(fich_salida_pkg, 'CREATE PROCEDURE ' || OWNER_DM || '.prc_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8), forzado_in varchar(1))');
      UTL_FILE.put_line(fich_salida_pkg, 'LANGUAGE plpgsql');
      UTL_FILE.put_line(fich_salida_pkg, 'AS $$');
      UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_new integer := 0;');
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_updt integer := 0;');
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_hist integer := 0;');
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_read integer := 0;');
      V_EXISTE_ESCENARIO_I := false;      
      FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
      LOOP
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_' || lista_scenarios_presentes(indx) || ' integer := 0;');
        dbms_output.put_line ('El escenario es: %' || lista_scenarios_presentes(indx) || '%');
        /* (20200117) Angel Ruiz. Se trata de un codigo Ad-hod para el caso de que haya un escenario I para KRC_PRODUCT_AVAIL */
        if (upper(lista_scenarios_presentes(indx)) = 'I') then
          V_EXISTE_ESCENARIO_I := true;
        end if;
        /* (20200117) FIN Angel Ruiz. Se trata de un codigo Ad-hod para el caso de que haya un escenario I para KRC_PRODUCT_AVAIL */        
      END LOOP;
      if (V_EXISTE_ESCENARIO_I = true) then
        UTL_FILE.put_line(fich_salida_pkg, '  v_conteo integer := 0;');      
      end if;      
      UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar integer;');
      UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP := current_timestamp;');
      UTL_FILE.put_line(fich_salida_pkg, '  num_reg integer;');
      UTL_FILE.put_line(fich_salida_pkg, '  msg TEXT;');
      UTL_FILE.put_line(fich_salida_pkg, '  errno TEXT;');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');
      
      UTL_FILE.put_line(fich_salida_pkg, '  /* INICIAMOS EL BUCLE POR CADA UNA DE LAS INSERCIONES EN LA TABLA DE STAGING */');
      UTL_FILE.put_line(fich_salida_pkg, '  /* EN EL CASO DE LAS DIMENSIONES SOLO DEBE HABER UN REGISTRO YA QUE NO HAY RETRASADOS */');
      --UTL_FILE.put_line(fich_salida_pkg, '  select concat(''Inicio del proceso de carga: '', ''' || OWNER_DM || '.prc_' || nombre_proceso || ''', ''.'');');
      UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''Inicio del proceso de carga: ' || OWNER_DM || '.prc_' || nombre_proceso || ''';');
      /* (20191011) Angel Ruiz. BUG.*/
      UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''El valor del parámetro fch_carga_in es: %.'', fch_carga_in;');
      UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''El valor del parámetro fch_datos_in es: %.'', fch_datos_in;');
      UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''El valor del parámetro forzado_in es: %.'', forzado_in;');
      --UTL_FILE.put_line(fich_salida_pkg, '  CALL COMBI.siguiente_paso(''' || nombre_fich_carga || ''', STR_TO_DATE(fch_datos_in, ''%Y%m%d''), STR_TO_DATE(fch_carga_in, ''%Y%m%d''), siguiente_paso_a_ejecutar);');
      UTL_FILE.put_line(fich_salida_pkg, '  CALL ' || OWNER_DM || '.prc_comun_siguiente_paso(''' || nombre_fich_carga || ''', to_date(fch_carga_in, ''YYYYMMDD''), to_date(fch_datos_in, ''YYYYMMDD''), siguiente_paso_a_ejecutar);');
      /* (20191011) Angel Ruiz.FIN */
      UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''Después de la llamada a siguiente_paso. El valor siguiente_paso_a_ejecutar es: %: '', siguiente_paso_a_ejecutar;');
      UTL_FILE.put_line(fich_salida_pkg, '  if (forzado_in = ''F'') then');
      UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := 1;');
      UTL_FILE.put_line(fich_salida_pkg, '  end if;');
      UTL_FILE.put_line(fich_salida_pkg, '  if (siguiente_paso_a_ejecutar = 1) then');
      UTL_FILE.put_line(fich_salida_pkg, '    inicio_paso_tmr := CURRENT_TIMESTAMP;');
      UTL_FILE.put_line(fich_salida_pkg, '    /* Truncamos la tabla antes de insertar los nuevos registros por si se lanza dos veces*/');
      UTL_FILE.put_line(fich_salida_pkg, '    TRUNCATE TABLE ' || OWNER_DM || '.' || nombre_proceso || '_T;');

      /* (20200117) Angel Ruiz. Se trata de un codigo Ad-hod para el caso de que haya un escenario I para KRC_PRODUCT_AVAIL */
      if (V_EXISTE_ESCENARIO_I = true) then
        UTL_FILE.put_line(fich_salida_pkg, '    select count(1) into v_conteo');
        UTL_FILE.put_line(fich_salida_pkg, '    from');
        UTL_FILE.put_line(fich_salida_pkg, '    ' || v_TABLE_BASE_NAME_SCENARIO_I);
        UTL_FILE.put_line(fich_salida_pkg, '    ;');
        UTL_FILE.put_line(fich_salida_pkg, '');
        UTL_FILE.put_line(fich_salida_pkg, '    if (v_conteo > 0) then');
        UTL_FILE.put_line(fich_salida_pkg, '      TRUNCATE TABLE ' || OWNER_DM || '.' || trim(v_TABLE_NAME_SCENARIO_I) || ';');
        FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
        LOOP
          if (upper(lista_scenarios_presentes(indx)) = 'I') then
            UTL_FILE.put_line(fich_salida_pkg, '      numero_reg_' || lista_scenarios_presentes(indx) || ' := numero_reg_' || lista_scenarios_presentes(indx) || ' + ' || OWNER_DM || '.fnc_' || lista_scenarios_presentes(indx) || '_' || nombre_proceso || ' (fch_carga_in, fch_datos_in)' || ';');
            UTL_FILE.put_line(fich_salida_pkg, '      RAISE NOTICE ''El numero de registros insertados en el escenario ' || lista_scenarios_presentes(indx) || ' es: %'', numero_reg_' || lista_scenarios_presentes(indx) || ';');
            UTL_FILE.put_line(fich_salida_pkg, '      numero_reg_new := numero_reg_new + numero_reg_' || lista_scenarios_presentes(indx) || ';');
          end if;
        END LOOP;
        UTL_FILE.put_line(fich_salida_pkg, '    end if;');
        
      end if;
      
      /* (20200117) FIN Angel Ruiz. Se trata de un codigo Ad-hod para el caso de que haya un escenario I para KRC_PRODUCT_AVAIL */


      
      FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
      LOOP
        --if lista_scenarios_presentes (indx) = 'E'
        if (lista_scenarios_presentes(indx) = 'E' or regexp_instr(lista_scenarios_presentes(indx), '^E_.+') > 0 or regexp_instr(lista_scenarios_presentes(indx), '.+_E$') > 0)
        then
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_' || lista_scenarios_presentes(indx) || ' := numero_reg_' || lista_scenarios_presentes(indx) || ' + ' || OWNER_DM || '.fnc_' || lista_scenarios_presentes(indx) || '_' || nombre_proceso || ' (fch_carga_in, fch_datos_in)' || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros insertados en el escenario ' || lista_scenarios_presentes(indx) || ' es: %'', numero_reg_' || lista_scenarios_presentes(indx) || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_updt = numero_reg_updt + numero_reg_' || lista_scenarios_presentes(indx) || ';');
          
        end if;
      END LOOP;
      FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
      LOOP
        --if lista_scenarios_presentes (indx) = 'H'
        if (lista_scenarios_presentes(indx) = 'H' or regexp_instr(lista_scenarios_presentes(indx), '^H_.+') > 0 or regexp_instr(lista_scenarios_presentes(indx), '.+_H$') > 0)
        then
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_' || lista_scenarios_presentes(indx) || ' := numero_reg_' || lista_scenarios_presentes(indx) || ' + ' || OWNER_DM || '.fnc_' || lista_scenarios_presentes(indx) || '_' || nombre_proceso || ' (fch_carga_in, fch_datos_in)' || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros insertados en el escenario ' || lista_scenarios_presentes(indx) || ' es: %'', numero_reg_' || lista_scenarios_presentes(indx) || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_hist = numero_reg_hist + numero_reg_' || lista_scenarios_presentes(indx) || ';');
        end if;
      END LOOP;
      FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
      LOOP
        --if lista_scenarios_presentes (indx) = 'N'
        if (lista_scenarios_presentes(indx) = 'N' or regexp_instr(lista_scenarios_presentes(indx), '^N_.+') > 0 or regexp_instr(lista_scenarios_presentes(indx), '.+_N$') > 0)
        then
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_' || lista_scenarios_presentes(indx) || ' := numero_reg_' || lista_scenarios_presentes(indx) || ' + ' || OWNER_DM || '.fnc_' || lista_scenarios_presentes(indx) || '_' || nombre_proceso || ' (fch_carga_in, fch_datos_in)' || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros insertados en el escenario ' || lista_scenarios_presentes(indx) || ' es: %'', numero_reg_' || lista_scenarios_presentes(indx) || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_new = numero_reg_new + numero_reg_' || lista_scenarios_presentes(indx) || ';');
          
        end if;
      END LOOP;
      /* (20200303) Angel Ruiz. NF: Escenario I en las dimensiones */
      FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
      LOOP
        /* (20200117) Angel Ruiz.Tema de escenario I para Inventario o re-inventario*/
        if (upper(lista_scenarios_presentes(indx)) <> 'I' and upper(lista_scenarios_presentes(indx)) <> 'N' and upper(lista_scenarios_presentes(indx)) <> 'E' and upper(lista_scenarios_presentes(indx)) <> 'H') then
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_' || lista_scenarios_presentes(indx) || ' := numero_reg_' || lista_scenarios_presentes(indx) || ' + ' || OWNER_DM || '.fnc_' || lista_scenarios_presentes(indx) || '_' || nombre_proceso || ' (fch_carga_in, fch_datos_in)' || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros insertados en el escenario ' || lista_scenarios_presentes(indx) || ' es: %'', numero_reg_' || lista_scenarios_presentes(indx) || ';');
          UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_new = numero_reg_new + numero_reg_' || lista_scenarios_presentes(indx) || ';');
        end if;
      END LOOP;
      /* (20200303) Angel Ruiz. Fin NF: Escenario I en las dimensiones */
      
      --UTL_FILE.put_line(fich_salida_pkg, '    SELECT COUNT(1) into numero_reg_read FROM ' || OWNER_DM || '.T_' || nombre_proceso || ';');
      UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_read := numero_reg_new + numero_reg_updt + numero_reg_hist;');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros leidos es: %'', numero_reg_read;');
      UTL_FILE.put_line(fich_salida_pkg, '    /* Este tipo de procesos tienen dos pasos, y ha terminado OK el paso primero por eso aparece un 0 en el siguiente campo */');
      UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_carga || ''', 1, 0, numero_reg_new, numero_reg_updt, numero_reg_hist, numero_reg_read, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');
      --UTL_FILE.put_line(fich_salida_pkg, '    commit;');
      /* Implemento que la exchange se haga como el paso dos de lhe_ */
      UTL_FILE.put_line(fich_salida_pkg, '    /* Se inicia el segundo paso */');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Comienza el segundo paso (de EXCHANGE).'';');
      UTL_FILE.put_line(fich_salida_pkg, '    inicio_paso_tmr := CURRENT_TIMESTAMP;');
      UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');

/*******************************************/
/*******************************************/
      UTL_FILE.put_line(fich_salida_pkg, '    inicio_paso_tmr := CURRENT_TIMESTAMP;');
      UTL_FILE.put_line(fich_salida_pkg, '    SELECT COUNT(*) INTO num_reg FROM ' || OWNER_DM || '.' || nombre_proceso || '_T;');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros final que se van a intercambiar es: %'', num_reg;');      
      UTL_FILE.put_line(fich_salida_pkg, '    /* Truncamos la tabla antes de insertar los nuevos registros por si se lanza dos veces*/');
      UTL_FILE.put_line(fich_salida_pkg, '    TRUNCATE TABLE ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ';');
      UTL_FILE.put_line(fich_salida_pkg, '    INSERT');
      UTL_FILE.put_line(fich_salida_pkg, '    INTO ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME);
      UTL_FILE.put_line(fich_salida_pkg, '    (');
      open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
      primera_col := 1;
      loop
        fetch MTDT_TC_DETAIL
        into reg_detail;
        exit when MTDT_TC_DETAIL%NOTFOUND;
        if primera_col = 1 then
          UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
          primera_col := 0;
        else
          UTL_FILE.put_line(fich_salida_pkg, '   , ' || reg_detail.TABLE_COLUMN);
        end if;        
      end loop;
      close MTDT_TC_DETAIL;
      UTL_FILE.put_line(fich_salida_pkg, '    )');
      UTL_FILE.put_line(fich_salida_pkg, '    SELECT');
      open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
      primera_col := 1;
      loop
        fetch MTDT_TC_DETAIL
        into reg_detail;
        exit when MTDT_TC_DETAIL%NOTFOUND;
        if primera_col = 1 then
          UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
          primera_col := 0;
        else
          UTL_FILE.put_line(fich_salida_pkg, '    ,' || reg_detail.TABLE_COLUMN);
        end if;        
      end loop;
      close MTDT_TC_DETAIL;
      UTL_FILE.put_line(fich_salida_pkg, '    FROM ' || OWNER_DM || '.' || nombre_proceso || '_T');
      UTL_FILE.put_line(fich_salida_pkg, '    ;');
      UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_carga || ''', 2, 0, num_reg, 0, 0, 0, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');
      UTL_FILE.put_line(fich_salida_pkg, '    --commit;');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El segundo paso ha terminado correctamente.'';');
      --UTL_FILE.put_line(fich_salida_pkg, '  end if;');
      UTL_FILE.put_line(fich_salida_pkg, '  elsif (siguiente_paso_a_ejecutar = 2) then');
      UTL_FILE.put_line(fich_salida_pkg, '    /* comienza el segundo paso */');
      UTL_FILE.put_line(fich_salida_pkg, '    inicio_paso_tmr := CURRENT_TIMESTAMP;');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Comienza el segundo paso'';');
      UTL_FILE.put_line(fich_salida_pkg, '    INSERT');
      UTL_FILE.put_line(fich_salida_pkg, '    INTO ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME);
      UTL_FILE.put_line(fich_salida_pkg, '    (');
      open MTDT_TC_DETAIL(reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
      primera_col := 1;
      loop
        fetch MTDT_TC_DETAIL
        into reg_detail;
        exit when MTDT_TC_DETAIL%NOTFOUND;
        if primera_col = 1 then
          UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
          primera_col := 0;
        else
          UTL_FILE.put_line(fich_salida_pkg, '    , ' || reg_detail.TABLE_COLUMN);
        end if;        
      end loop;
      close MTDT_TC_DETAIL;
      UTL_FILE.put_line(fich_salida_pkg, '    )');
      UTL_FILE.put_line(fich_salida_pkg, '    SELECT');
      open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
      primera_col := 1;
      loop
        fetch MTDT_TC_DETAIL
        into reg_detail;
        exit when MTDT_TC_DETAIL%NOTFOUND;
        if primera_col = 1 then
          UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
          primera_col := 0;
        else
          UTL_FILE.put_line(fich_salida_pkg, '    ,' || reg_detail.TABLE_COLUMN);
        end if;        
      end loop;
      close MTDT_TC_DETAIL;
      UTL_FILE.put_line(fich_salida_pkg, '    FROM ' || OWNER_DM || '.' || nombre_proceso || '_T');
      UTL_FILE.put_line(fich_salida_pkg, '    ;');
      UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_exchange || ''', 2, 0, numero_reg, 0, 0, 0, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');

      UTL_FILE.put_line(fich_salida_pkg, '    --commit;');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El segundo paso ha terminado correctamente.'';');
/*******************************************/
/*******************************************/

      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El procedimiento: ' || OWNER_DM || '.prc_' || nombre_proceso || ' ha acabdo correctamente.'';');
      UTL_FILE.put_line(fich_salida_pkg, '  else');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El procedimiento: ' || OWNER_DM || '.prc_' || nombre_proceso || ' ya se ha ejecutado previamente de manera exitosa.'';');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''No se ha vuelto a ejecutar.'';');
      UTL_FILE.put_line(fich_salida_pkg, '  end if;');
      UTL_FILE.put_line(fich_salida_pkg, 'EXCEPTION');
      UTL_FILE.put_line(fich_salida_pkg, '    WHEN OTHERS THEN');
      UTL_FILE.put_line(fich_salida_pkg, '        GET STACKED DIAGNOSTICS errno := RETURNED_SQLSTATE, msg := MESSAGE_TEXT;');
      UTL_FILE.put_line(fich_salida_pkg, '        RAISE NOTICE ''Error al realizar la transformación en el procedimiento: %.'', ' || OWNER_DM || '.prc_' || nombre_proceso || ';');
      UTL_FILE.put_line(fich_salida_pkg, '        RAISE NOTICE ''Error code: %. Mensaje: %'', errno, msg;');
      UTL_FILE.put_line(fich_salida_pkg, '        CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_carga || ''', siguiente_paso_a_ejecutar, 1, 0, 0, 0, 0, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');      
      UTL_FILE.put_line(fich_salida_pkg, '        RAISE; -- Reraises the caught exception');
      UTL_FILE.put_line(fich_salida_pkg, 'END;');
      UTL_FILE.put_line(fich_salida_pkg, '$$;');
      UTL_FILE.put_line(fich_salida_pkg, '');
    
      /******/
      /* FIN DE LA GENERACION DEL PACKAGE */
      /******/    
      UTL_FILE.put_line(fich_salida_pkg, '');
      --UTL_FILE.put_line(fich_salida_pkg, '!quit');
      UTL_FILE.put_line(fich_salida_pkg, '');
      --UTL_FILE.put_line(fich_salida_pkg, 'grant execute on app_mvnodm.pkg_' || reg_tabla.TABLE_NAME || ' to app_mvnotc;');
      --UTL_FILE.put_line(fich_salida_pkg, 'grant execute on ' || OWNER_DM || '.pkg_' || nombre_proceso || ' to ' || OWNER_TC || ';');
      --UTL_FILE.put_line(fich_salida_pkg, '/');
      --UTL_FILE.put_line(fich_salida_pkg, 'exit SUCCESS;');
  
    
      /******/
      /* FIN DEL PACKGE BODY */
      /******/    
  /****************************************************/
  /****************************************************/
  /****************************************************/
  /****************************************************/
  /****************************************************/
      /******/
      /* INICIO DE LA GENERACION DEL sh de NUEVOS Y EXISTENTES */
      /******/
      
  /***********************/
      UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
      UTL_FILE.put_line(fich_salida_load, '#############################################################################');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Millicom. Tigo.                                                             #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Archivo    :       load_ne_' ||  reg_tabla.TABLE_NAME || '.sh             #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                                                  #');
      UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta los procesos de                            #');
      UTL_FILE.put_line(fich_salida_load, '#              TRANSFORMACIÓN y CARGA para ' || NAME_DM_FULL || '.          #');
      UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Ejecucion  :                                                              #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Historia : 02-Octubre-2024 -> Creacion                                    #');
      UTL_FILE.put_line(fich_salida_load, '# Caja de Control - M :                                                     #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
      UTL_FILE.put_line(fich_salida_load, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Caducidad del Requerimiento :                                             #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Dependencias :                                                            #');
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Usuario:                                                                  #');   
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '# Telefono:                                                                 #');   
      UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_load, '#############################################################################');
      UTL_FILE.put_line(fich_salida_load, '');
      /******************************************/
      /******************************************/
      --UTL_FILE.put_line(fich_salida_load, 'InsertaFinFallido()');
      --UTL_FILE.put_line(fich_salida_load, '{');
      --UTL_FILE.put_line(fich_salida_load, '    insert_record_monitoreo ' || 'load_ne_' || nombre_proceso || '.sh 1 1 0 0 0 0 0 "${FCH_DATOS}" "${INICIO_PASO_TMR}"' || ' >> "${' || NAME_DM || '_TRAZAS}"/load_ne_' || nombre_proceso || '_"${FECHA_HORA}"' || '.log 2>' || '&' || '1');
      --UTL_FILE.put_line(fich_salida_load, '    rc=$?');
      --UTL_FILE.put_line(fich_salida_load, '    if [ $rc -ne 0 ]');
      --UTL_FILE.put_line(fich_salida_load, '    then');
      --UTL_FILE.put_line(fich_salida_load, '        SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
      --UTL_FILE.put_line(fich_salida_load, '        echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '        ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      --UTL_FILE.put_line(fich_salida_load, '        exit 1;');
      --UTL_FILE.put_line(fich_salida_load, '    fi');
      --UTL_FILE.put_line(fich_salida_load, '    return 0');
      --UTL_FILE.put_line(fich_salida_load, '}');
      --UTL_FILE.put_line(fich_salida_load, '');
      --UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK()');
      --UTL_FILE.put_line(fich_salida_load, '{');
      --UTL_FILE.put_line(fich_salida_load, '    insert_record_monitoreo ' || 'load_ne_' || nombre_proceso || '.sh 1 0 "${TOT_INSERTADOS}" 0 0 "${TOT_LEIDOS}" "${TOT_RECHAZADOS}" "${FCH_DATOS}" "${INICIO_PASO_TMR}"' || ' >> "${' || NAME_DM || '_TRAZAS}"/load_ne_' || nombre_proceso || '_"${FECHA_HORA}"' || '.log 2>&' || '1');
      --UTL_FILE.put_line(fich_salida_load, '    rc=$?');
      --UTL_FILE.put_line(fich_salida_load, '    if [ $rc -ne 0 ]');
      --UTL_FILE.put_line(fich_salida_load, '    then');
      --UTL_FILE.put_line(fich_salida_load, '        SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
      --UTL_FILE.put_line(fich_salida_load, '        echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '        ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      --UTL_FILE.put_line(fich_salida_load, '        exit 1;');
      --UTL_FILE.put_line(fich_salida_load, '    fi');
      --UTL_FILE.put_line(fich_salida_load, '    return 0');
      --UTL_FILE.put_line(fich_salida_load, '}');
      --UTL_FILE.put_line(fich_salida_load, '');
      /******************************************/
      /******************************************/
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# shellcheck disable=SC1091');
      UTL_FILE.put_line(fich_salida_load, '. "${HOME_PRODUCCION}"/' || NAME_DM || '/COMUN/Shell/Entorno/Entorno' || NAME_DM || '_' || PAIS || '.sh');
      UTL_FILE.put_line(fich_salida_load, '# Comprobamos si el numero de parametros es el correcto');
      UTL_FILE.put_line(fich_salida_load, 'if [ $# -ne 3 ] ; then');
      UTL_FILE.put_line(fich_salida_load, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
      UTL_FILE.put_line(fich_salida_load, '  echo "${SUBJECT}"');        
      UTL_FILE.put_line(fich_salida_load, '  exit 1');
      UTL_FILE.put_line(fich_salida_load, 'fi');
      UTL_FILE.put_line(fich_salida_load, '# Recogida de parametros');
      UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA=${1}');
      UTL_FILE.put_line(fich_salida_load, 'FCH_DATOS=${2}');
      UTL_FILE.put_line(fich_salida_load, 'BAN_FORZADO=${3}');
      UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_$(date +%Y%m%d_%H%M%S)');      
      UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
      UTL_FILE.put_line(fich_salida_load, 'if [ ! -d "${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}" ] ; then');
      UTL_FILE.put_line(fich_salida_load, '  mkdir "${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}"');
      UTL_FILE.put_line(fich_salida_load, 'fi');
      UTL_FILE.put_line(fich_salida_load, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
      UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > "${' || NAME_DM || '_TRAZAS}/load_ne_' || nombre_proceso || '_${FECHA_HORA}"' || '.log ');
      UTL_FILE.put_line(fich_salida_load, '# shellcheck disable=SC2129');
      UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: $(date +%d/%m/%Y\ %H:%M:%S)" >> "${' || NAME_DM || '_TRAZAS}/load_ne_' || nombre_proceso || '_${FECHA_HORA}"' || '.log');
      UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}" >> "${' || NAME_DM || '_TRAZAS}/load_ne_' || nombre_proceso || '_${FECHA_HORA}"' || '.log');
      UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}" >> "${' || NAME_DM || '_TRAZAS}/load_ne_' || nombre_proceso || '_${FECHA_HORA}"' || '.log');
      UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}" >> "${' || NAME_DM || '_TRAZAS}/load_ne_' || nombre_proceso || '_${FECHA_HORA}"' || '.log');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=' || 'load_ne_' || nombre_proceso || '.sh');
      UTL_FILE.put_line(fich_salida_load, '');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# Cuentas  Produccion / Desarrollo                                             #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, 'if [ "$(/sbin/ifconfig -a | grep ''192.168.2.'' | awk ''{print $2}'')" = "192.168.2.109" ]||[ "$(/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}'')" = "10.225.173.184" ]; then');
      UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
      UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
      UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_Usuario_ReportesBI.txt)');
      UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_ReportesBI.txt)');
      UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TelefonosMantto.txt)');
      UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
      UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TELEFONOS_USUARIOS.txt)');
      UTL_FILE.put_line(fich_salida_load, 'else');
      UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
      UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
      UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_Usuario_ReportesBI.txt)');
      UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_ReportesBI.txt)');
      UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TelefonosMantto.txt)');
      UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
      UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TELEFONOS_USUARIOS.txt)');
      UTL_FILE.put_line(fich_salida_load, 'fi');
      UTL_FILE.put_line(fich_salida_load, '');
      UTL_FILE.put_line(fich_salida_load, '# Llamada a PostgreSQL');
      UTL_FILE.put_line(fich_salida_load, 'psql -h "$HOST" -p "$PORT" -U "$BD_USUARIO" -d "$DB_NAME" -c "call ' || OWNER_DM || '.prc_' || nombre_proceso || ' (''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');" >> "${' || NAME_DM || '_TRAZAS}/load_ne_' || nombre_proceso || '_${FECHA_HORA}"' || '.log ' ||  '2>&' || '1');
      UTL_FILE.put_line(fich_salida_load, '');
      UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
      UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el postgres en la llamada a prc_' || nombre_proceso || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_load, '  echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_ne' || '_' || nombre_proceso || '_${FECHA_HORA}".log');        
      UTL_FILE.put_line(fich_salida_load, '  echo "$(date)" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_ne' || '_' || nombre_proceso || '_${FECHA_HORA}.log"');
      --UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_load, '  exit 1');
      UTL_FILE.put_line(fich_salida_load, 'fi');
      UTL_FILE.put_line(fich_salida_load, 'echo "El proceso ' || nombre_fich_carga || ' se ha realizado correctamente." >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_ne_' || nombre_proceso || '_${FECHA_HORA}".log');
      UTL_FILE.put_line(fich_salida_load, 'exit 0');
      UTL_FILE.put_line(fich_salida_load, '');

      
      /**************/
      UTL_FILE.FCLOSE (fich_salida_load);
      UTL_FILE.FCLOSE (fich_salida_pkg);
      --UTL_FILE.FCLOSE (fich_salida_exchange);
    end if;
  end loop;   
  close MTDT_TABLA;
end;

