/******/
/* I M P O R T A N T E  A  L E E R*/
/******/
/******/
/* EN ESTA VERSIÓN del generador NO HAY EXCHANGE YA QUE TODO SE HACE EN EL lh_ */
/******/
/******/

declare

cursor MTDT_TABLA
  is
SELECT
      DISTINCT TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME", /*(20150907) Angel Ruiz NF. Nuevas tablas.*/
      --TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME",
      --TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      --TRIM(mtdt_modelo_logico.TABLESPACE) "TABLESPACE" (20150907) Angel Ruiz NF. Nuevas tablas.
      TRIM(mtdt_modelo_summary.TABLESPACE) "TABLESPACE",
      TRIM(mtdt_modelo_summary.PARTICIONADO) "PARTICIONADO"
    FROM
      --MTDT_TC_SCENARIO, mtdt_modelo_logico (20150907) Angel Ruiz NF. Nuevas tablas.
      MTDT_TC_SCENARIO, mtdt_modelo_summary
    WHERE MTDT_TC_SCENARIO.TABLE_TYPE = 'F' and
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_logico.TABLE_NAME) and (20150907) Angel Ruiz NF. Nuevas tablas.
    trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_summary.TABLE_NAME) and
    trim(MTDT_TC_SCENARIO.TABLE_NAME) in 
    (
    --'CMBF_SURVEY_USERS', 'CMBF_SURVEY_USERS_DETAIL', 'CMBF_USERS_COURSES', 'CMBF_USERS_COURSES_DETAIL', 'CMBF_EVENTS'
    --'DMF_VENTAS_USUARIO', 'DMF_VENTAS_MESA', 'DMF_VENTAS_TIPO_PAGO', 'KRF_SALES_FORECAST', 'KRF_PRODUCT_FORECAST'
    --, 'KRF_OFFER_COMP', 'KRF_SALES_FORECAST', 'KRF_PRODUCT_AVAIL_HIST', 'KRF_OFFER_COMP_HIST'
    --, 'KRF_WEEK_SALES'
    --'KRF_TRANSACTIONS', 'KRF_TRANSACTIONS_HIST'
    --'DWF_SUBSCRIBER', 'DWF_TOPUP'
    --, 'DWF_TRAFV_TA', 'DWF_TRAFE_TA'
    --, 'DWF_TRAFD_TA'
    --'DWF_TRANSAC_SUBSCRIBER'
      --'DWF_CSTMR_INV_DTL', 'DWF_CSTMR_INV', 'DWF_CSTMR_PNDG_DOC', 'DWF_CSTMR_COLLECT'
      --------------------------------
      -- INICIO SP1
      --'AR_MVMT_FCT'
      --, 'AR_TRCKNG_FCT'
      -- FIN SP1
      -------------------------------
      -- INICIO SP2
      'CSTMR_INV_FCT'
      , 'CSTMR_DOC_FCT'
      , 'CSTMR_INV_DTL_FCT'
      , 'CSTMR_PNDG_DOC_FCT'
       --'TRN_PDUSG_MVMT_SUBS_FCT' -- Ya ha sido generada
      --,'TRN_PDUSG_CSTMR_DOC_FCT'  -- Ya ha sido generada
      --, 'TRN_PDUSG_CSTMR_INV_DTL_FCT' -- Ya ha sido generada
      --, 'TRN_PDUSG_CSTMR_INV_FCT' -- Ya ha sido generada
      --, 'TRN_PDUSG_CSTMR_PNDG_DOC_FCT'  -- Ya ha sido generada
      --, 'TRN_PDUSG_CSTMR_COLLECT_FCT' NO EXISTE MAS
      -- FIN SP2
      -------------------------------
      -- INICIO SP3
      , 'HH_MVMT_FCT'
      , 'HH_TRCKNG_FCT'
      -------------------------------
      -- INICIO SP4
      , 'PRVN_SVC_TRCKNG_FCT'
      --, 'TRN_PDUSG_PRVN_SVC_TRCKNG_FCT' -- Ya ha sido generada
      -- FIN SP4
    );
    
  cursor MTDT_SCENARIO (table_name_in IN VARCHAR2)
  is
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE",
      TRIM(TABLE_COLUMNS) "TABLE_COLUMNS",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM("SOURCE") "SOURCE",
      TRIM ("SOURCE_TYPE") "GROUP",
      TRIM(FILTER) "FILTER",
      TRIM(INTERFACE_COLUMNS) "INTERFACE_COLUMNS",
      TRIM(SCENARIO) "SCENARIO",
      DATE_CREATE,
      DATE_MODIFY
    FROM 
      MTDT_TC_SCENARIO
    WHERE
      TRIM(TABLE_NAME) = table_name_in
    ORDER BY SCENARIO ASC;
  
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
      TRIM(MTDT_TC_DETAIL.VALUE) "VALUE",
      TRIM(MTDT_TC_DETAIL.RUL) "RUL",
      MTDT_TC_DETAIL.DATE_CREATE,
      MTDT_TC_DETAIL.DATE_MODIFY
  FROM
      MTDT_TC_DETAIL, MTDT_MODELO_DETAIL
  WHERE
      TRIM(MTDT_TC_DETAIL.TABLE_NAME) = table_name_in and
      TRIM(MTDT_TC_DETAIL.SCENARIO) = scenario_in and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_NAME)) = UPPER(trim(MTDT_MODELO_DETAIL.TABLE_NAME)) and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_COLUMN)) = UPPER(trim(MTDT_MODELO_DETAIL.COLUMN_NAME))
  ORDER BY MTDT_MODELO_DETAIL.POSITION ASC;

  /* (20161228) Angel Ruiz. */
  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_MODELO_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_MODELO_DETAIL.COLUMN_NAME) "COLUMN_NAME",
      MTDT_MODELO_DETAIL.DATA_TYPE,
      MTDT_MODELO_DETAIL.PK,
      TRIM(MTDT_MODELO_DETAIL.NULABLE) "NULABLE",
      TRIM(MTDT_MODELO_DETAIL.VDEFAULT) "VDEFAULT",
      TRIM(MTDT_MODELO_DETAIL.INDICE) "INDICE",
      TRIM(MTDT_MODELO_SUMMARY.PARTICIONADO) "PARTICIONADO"
    FROM MTDT_MODELO_DETAIL, MTDT_MODELO_SUMMARY
    WHERE
      UPPER(trim(MTDT_MODELO_DETAIL.TABLE_NAME)) = UPPER(trim(MTDT_MODELO_SUMMARY.TABLE_NAME)) and
      UPPER(trim(MTDT_MODELO_DETAIL.TABLE_NAME)) = UPPER(trim(table_name_in))
    ORDER BY POSITION ASC;
      
  CURSOR MTDT_TC_LOOKUP (table_name_in IN VARCHAR2)
  IS
    SELECT
      DISTINCT
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
      TABLE_LKUP_COND "TABLE_LKUP_COND",
      --IE_COLUMN_LKUP "IE_COLUMN_LKUP",
      TRIM("VALUE") "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      (trim(RUL) = 'LKUP' or trim(RUL) = 'LKUPC') and
      TRIM(TABLE_NAME) = table_name_in;

  CURSOR MTDT_TC_FUNCTION (table_name_in IN VARCHAR2)
  IS
    SELECT
      DISTINCT
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
      TABLE_LKUP_COND "TABLE_LKUP_COND",
      IE_COLUMN_LKUP "IE_COLUMN_LKUP",
      TRIM("VALUE") "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      RUL = 'FUNCTION' and
      TRIM(TABLE_NAME) = table_name_in;
      

  reg_tabla MTDT_TABLA%rowtype;     
  reg_scenario MTDT_SCENARIO%rowtype;
  reg_detail MTDT_TC_DETAIL%rowtype;
  reg_lookup MTDT_TC_LOOKUP%rowtype;
  reg_function MTDT_TC_FUNCTION%rowtype;
  r_mtdt_modelo_logico_COLUMNA c_mtdt_modelo_logico_COLUMNA%rowtype;

  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(500);
  type lista_tablas_from is table of varchar(4000);
  type lista_condi_where is table of varchar(500);
  type list_columns_par  IS TABLE OF VARCHAR(30);

  
  lista_pk                                      list_columns_primary := list_columns_primary ();
  lista_par                                      list_columns_par := list_columns_par ();
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(30500);
  prototipo_fun                             VARCHAR2(2000);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(60);
  nombre_fich_exchange            VARCHAR2(60);
  nombre_fich_pkg                      VARCHAR2(60);
  lista_scenarios_presentes                                    list_strings := list_strings();
  campo_filter                                VARCHAR2(2000);
  nombre_proceso                        VARCHAR2(30);
  nombre_tabla_reducido           VARCHAR2(30);
  nombre_tabla_T                        VARCHAR2(30);
  v_nombre_particion                  VARCHAR2(30);
  --nombre_tabla_base_reducido           VARCHAR2(30);
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR2(60);
  OWNER_RD                        VARCHAR2(60);
  OWNER_DWH                        VARCHAR2(60);
  OWNER_TC                              VARCHAR2(60);
  PREFIJO_DM                            VARCHAR2(60);
  ESQUEMA_DM  VARCHAR2(60);
  NAME_DM_FULL VARCHAR2(60);
  PAIS                              VARCHAR2(60);
  VAR_REGS_MEDIA                        VARCHAR2(60);
  v_contador                        PLS_INTEGER:=0;
  v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;
  l_FROM                                      lista_tablas_from := lista_tablas_from();
  l_FROM_solo_tablas                               lista_tablas_from := lista_tablas_from();  
  l_WHERE                                   lista_condi_where := lista_condi_where();
  l_WHERE_ON_clause                         lista_condi_where := lista_condi_where();
  lista_variables_rownumber              list_strings := list_strings();
  v_hay_look_up                           VARCHAR2(1):='N';
  v_nombre_seqg                          VARCHAR(120):='N';
  v_bandera                                   VARCHAR2(1):='S';
  v_nombre_tabla_agr                VARCHAR2(30):='No Existe';
  v_nombre_tabla_agr_redu           VARCHAR2(30):='No Existe';
  v_nombre_proceso_agr              VARCHAR2(30);
  nombre_tabla_T_agr                VARCHAR2(30);
  v_existen_retrasados              VARCHAR2(1) := 'N';
  v_numero_indices                  PLS_INTEGER:=0;
  v_MULTIPLICADOR_PROC                   VARCHAR2(60);
  v_alias                           VARCHAR2(40);
  v_hay_regla_seq                   BOOLEAN:=false; /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_nombre_seq                      VARCHAR2(50); /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_nombre_campo_seq                VARCHAR2(50); /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_VAR_PCT_COMISIONES BOOLEAN:=false;  /* (Angel Ruiz) NF. Variable #VAR_PCT_COMISIONES#*/
  v_variables_sesion BOOLEAN;
  v_row_number VARCHAR2(70);
  v_encontrado_var_row_number BOOLEAN;
  v_tipo_particionado VARCHAR2(10);
  v_campo_parti_en_pk    PLS_integer;
  v_CVE_DIA_es_col        PLS_integer;
  v_FCT_DT_KEY_es_col     PLS_integer;
  v_CVE_MES_es_col      PLS_integer;
  v_CVE_WEEK_es_col     PLS_integer;
  v_nombre_campo_particionado VARCHAR2(30);
  

  


/************/
/*************/
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
        v_cadena_temp := regexp_replace (cadena_in, ' *([Nn][Vv][Ll]) *\( *([A-Za-z_]+) *,', ' \1(' || alias_in || '.' || '\2' || ',');
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
        v_cadena_temp := regexp_replace (cadena_in, ' *([Uu][Pp][Pp][Ee][Rr]) *\( *([A-Za-z_]+) *\)', ' \1(' || alias_in || '.' || '\2' || ')');
        v_cadena_result := v_cadena_temp;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
      /* Se trata de que el campo de join posee la funcion REPLACE */
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *') > 0) then
        v_cadena_temp := regexp_replace (cadena_in, ' *([Rr][Ee][Pp][Ll][Aa][Cc][Ee]) *\( *([A-Za-z_]+) *,', ' \1(' || alias_in || '.' || '\2,');
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


/**************/

/* (20161117) Angel Ruiz. */
  function extrae_campo (cadena_in in varchar2) return varchar2
  is
    v_campo varchar2(200);
    v_cadena_temp varchar2(200);
    v_cadena_result varchar2(200);
  begin
    dbms_output.put_line('Estoy en el extrae_campo. La cadena de entrada es: ' || cadena_in);
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
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *\)') > 0) then
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
    dbms_output.put_line('Estoy en el extrae_campo. La cadena de salida es: ' || v_cadena_result);
    return v_cadena_result;
  end;
/* (20150918) Angel Ruiz. NUEVA FUNCION */
  function sustituye_comillas_dinam (cadena_in in varchar2) return varchar2
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
      end if;
      return cadena_resul;
    end;

/************/

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
  elemento varchar2 (400);
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
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant)-1);
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

--  function transformo_decode(cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
--  is
--    parte_1 varchar2(100);
--    parte_2 varchar2(100);
--    parte_3 varchar2(100);
--    parte_4 varchar2(100);
--    decode_out varchar2(500);
--    lista_elementos list_strings := list_strings ();
  
--  begin
--    /* Ejemplo de Deode que analizo DECODE (ID_FUENTE,'SER', ID_CANAL,'1') */
--    lista_elementos := split_string_coma(cadena_in);
--    parte_1 := trim(substr(lista_elementos(1), instr(lista_elementos(1), '(') + 1)); /* Me quedo con ID_FUENTE*/
--    parte_2 := lista_elementos(2);  /* Me quedo con 'SER' */
--    parte_3 := trim(lista_elementos(3));
--    parte_4 := lista_elementos(4);
--    if (outer_in = 1) then
--      /* En la tranformacion del DECODE es necesario ponerle el signo de OUTER */
--      decode_out := 'DECODE(' || alias_in || '.' || parte_1 || '(+), ' || sustituye_comillas_dinam(parte_2) || ', ' || alias_in || '.'|| parte_3 || '(+), ' || sustituye_comillas_dinam(parte_4);
--    else    
--      /* En la tranformacion del DECODE no es necesario ponerle el signo de OUTER */
--      decode_out := 'DECODE(' || alias_in || '.' || parte_1 || ', ' || sustituye_comillas_dinam(parte_2) || ', ' || alias_in || '.'|| parte_3 || ', ' || sustituye_comillas_dinam(parte_4);
--    end if;
--    return decode_out;
--  end transformo_decode;
--  function transformo_decode(cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
--  is
--    parte_1 varchar2(100);
--    parte_2 varchar2(100);
--    parte_3 varchar2(100);
--    parte_4 varchar2(100);
--    decode_out varchar2(500);
--    lista_elementos list_strings := list_strings ();
  
--  begin
    /* Ejemplo de Decode que analizo DECODE (ID_FUENTE,'SER', ID_CANAL,'1') */
--    lista_elementos := split_string_coma(cadena_in);
--    parte_1 := trim(substr(lista_elementos(1), instr(lista_elementos(1), '(') + 1)); /* Me quedo con ID_FUENTE*/
--    parte_2 := lista_elementos(2);  /* Me quedo con 'SER' */
--    parte_3 := trim(lista_elementos(3));  /* Me quedo con ID_CANAL */
--    parte_4 := trim(substr(lista_elementos(4), 1, instr(lista_elementos(4), ')') - 1));  /* Me quedo con '1' */
--    if (instr(parte_1, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_1 := alias_in || '.' || parte_1 || '(+)';
--      else
--        parte_1 := alias_in || '.' || parte_1;
--      end if;
--    end if;
--    if (instr(parte_2, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_2 := alias_in || '.' || parte_2 || '(+)';
--      else
--        parte_2 := alias_in || '.' || parte_2;
--      end if;
--    end if;
--    if (instr(parte_3, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_3 := alias_in || '.' || parte_3 || '(+)';
--      else
--        parte_3 := alias_in || '.' || parte_3;
--      end if;
--    end if;
--    if (instr(parte_4, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_4 := alias_in || '.' || parte_4 || '(+)';
--      else
--        parte_4 := alias_in || '.' || parte_4;
--      end if;
--    end if;
    /* Puede ocurrir que alguna parte del decode tanga el signo ' como seria el caso de los campos literales */
    /* como estamos generando querys dinamicas, tenemos que escapar las comillas */
--    if (instr(parte_1, '''') > 0) then
--      parte_1 := sustituye_comillas_dinam(parte_1);
--    end if;
--    if (instr(parte_2, '''') > 0) then
--      parte_2 := sustituye_comillas_dinam(parte_2);
--    end if;
--    if (instr(parte_3, '''') > 0) then
--      parte_3 := sustituye_comillas_dinam(parte_3);
--    end if;
--    if (instr(parte_4, '''') > 0) then
--      parte_4 := sustituye_comillas_dinam(parte_4);
--    end if;
--    decode_out := 'DECODE(' || parte_1 || ', ' || parte_2 || ', ' || parte_3 || ', ' || parte_4 || ')';
--    return decode_out;
--  end transformo_decode;
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
            if (instr(parte_1, '.') = 0) then /* (20170221) Angel Ruiz. BUG */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1' || ' (+)'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              else
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              end if;
            else  /* Viene el campo ya con el alias por lo que no lo pongo */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_.]+) *', '\1' || ' (+)'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              else
                v_cadena_temp := v_cadena_temp || parte_1;
              end if;
            end if;
            v_cadena_temp := v_cadena_temp || ', '; /* Tengo LA CADENA: "DECODE (alias_in.ID_FUENTE (+), " */
          elsif (indx = lista_elementos.LAST) then
            /* Se trata del ultimo elemento '1') */
            if (instr(lista_elementos(indx), '''') = 0) then
              /* Se trata de un elemnto tipo ID_CANAL pero situado al final del DECODE */
              if (instr(lista_elementos(indx), '.') = 0) then /* (20170221) Angel Ruiz. BUG. Si no viene alias */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.\1' || ' (+))'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                else
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.\1' || ')'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                end if;
              else  /* (20170221) Angel Ruiz. Si viene alias no lo pongo*/
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_.]+) *\)', '\1' || ' (+))'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                else
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_.]+) *\)', '\1' || ')'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
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
              if (instr(lista_elementos(indx), '.') = 0) then /* (20170221) Angel Ruiz. BUG. Si no viene alias */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *', alias_in || '.\1' || ' (+)');
                else  
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *', alias_in || '.\1');
                end if;
              else /* (20170221) Angel Ruiz. BUG. Si viene alias */
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


  function proc_campo_value_condicion (cadena_in in varchar2, nombre_funcion_lookup in varchar2) return varchar2
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
      /* Busco el signo = o el simbolo != */
      --if (instr(cadena_resul, '!=') > 0) then
        /* Busco el signo != */
        --sustituto := ' (+)!= ';
        --loop
          --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '!=', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('!='));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + (length (' (+)!= '));
          --dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
          --pos := pos_ant;
        --end loop;
      --else
        --if (instr(cadena_resul, '=') > 0) then
          --sustituto := ' (+)= ';
          --loop
            --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
            --pos := instr(cadena_resul, '=', pos+1);
            --exit when pos = 0;
            --dbms_output.put_line ('Pos es mayor que 0');
            --dbms_output.put_line ('Primer valor de Pos: ' || pos);
            --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
            --dbms_output.put_line ('La cabeza es: ' || cabeza);
            --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
            --cola := substr(cadena_resul, pos + length ('='));
            --dbms_output.put_line ('La cola es: ' || cola);
            --cadena_resul := cabeza || sustituto || cola;
            --pos_ant := pos + (length (' (+)= '));
            --dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
            --pos := pos_ant;
          --end loop;
        --end if;
      --end if;
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
  
/************/
/*************/
  function procesa_campo_filter_dinam (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (4000);
    sustituto              varchar2(100);
    cola                      varchar2(4000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(4000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco VAR_FCH_CARGA */
        sustituto := ' to_date ('''' ||  fch_datos_in || '''', ''yyyymmdd'') ';
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
        /* Busco VAR_FCH_INICIO */
        sustituto := ' to_date ('''' ||  fch_registro_in || '''', ''yyyymmdd'') ';
        loop
          dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_FCH_INICIO', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_FCH_INICIO'));
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
        /* (20150914) Angel Ruiz. BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */
        /* Busco LA COMILLA para poner comillas dobles */
        /*(20161118) Angel Ruiz. Modifico la forma de cambiar la ' por '' usando regexp_replace */
        cadena_resul := regexp_replace(cadena_resul, '''', '''''');
        --pos := 0;
        --posicion_ant := 0;
        --sustituto := '''''';
        --loop
          --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '''', pos);
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
        /* (20150914) Angel Ruiz. FIN BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */
      end if;
      return cadena_resul;
    end;

/************/
  

  function procesa_campo_filter (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (30500);
    sustituto              varchar2(100);
    cola                      varchar2(30500);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(30500);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_CARGA#', 'fch_carga_in');
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_DATOS#', 'fch_datos_in');        
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_INICIO#', 'var_fch_inicio');

        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_INI_MES#', ' date_format(''#VAR_FCH_DATOS#'', ''%Y%m01'') ');
        cadena_resul := regexp_replace(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', ' 90 ');
        
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_DM#', OWNER_DM);
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_SA#', OWNER_SA);
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_T#', OWNER_T);
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_TC#', OWNER_TC);
        
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_MTDT#', OWNER_MTDT);
        cadena_resul := regexp_replace(cadena_resul, '#VAR_MARGEN_COMISION#', ' 0.3 ');
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FIN_DEFAULT#', ' ''9999-12-31'' ');
        --cadena_resul := regexp_replace(cadena_resul, '#VAR_REGS_MEDIA#', VAR_REGS_MEDIA);
        /* (20170925) Angel Ruiz. NF. Aparece la varieble #VAR_PCT_COMISIONES#*/
        if (INSTR(cadena_resul, '#VAR_PCT_COMISIONES#') > 0) then 
          v_VAR_PCT_COMISIONES := true;
        end if;

      end if;
      return cadena_resul;
    end;

  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (30500);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(500);
    cad_seg         VARCHAR(500);
    cadena            VARCHAR(200);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(200);
    condicion_pro         VARCHAR2(200);
    constante         VARCHAR2(100);
    posicion_ant    PLS_integer;
    pos                    PLS_integer;
    cadena_resul  VARCHAR(20000);
    sustituto           VARCHAR(30);
    lon_cadena     PLS_integer;
    cabeza             VARCHAR2(2000);
    cola                   VARCHAR2(2000);
    pos_ant            PLS_integer;
    v_encontrado  VARCHAR2(1);
    v_alias             VARCHAR2(40);
    v_alias_table_base  VARCHAR2(40);/* (20161227) Angel Ruiz */
    table_columns_lkup  list_strings := list_strings();
    ie_column_lkup    list_strings := list_strings();
    tipo_columna  VARCHAR2(30);
    mitabla_look_up VARCHAR2(4000);
    v_tabla_base_name VARCHAR2(20000);
    mi_tabla_base_name VARCHAR2(50);
    mi_tabla_base_name_alias VARCHAR2(50);
    l_registro          ALL_TAB_COLUMNS%rowtype;
    l_registro1         ALL_TAB_COLUMNS%rowtype;
    l_registro2         v_MTDT_CAMPOS_DETAIL%rowtype;
    v_value VARCHAR(200);
    nombre_campo  VARCHAR2(200);
    v_alias_incluido PLS_Integer:=0;
    v_alias_incluido_table_base PLS_Integer:=0;
    v_es_query_table_base PLS_Integer:=0;
    v_table_look_up varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_reg_table_lkup varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_alias_table_look_up varchar2(10000);  /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_tipo_campo  VARCHAR2(30);
    v_existe_valor  BOOLEAN;
    v_table_lkup_prima varchar2(10000);  /*(20170109) Angel Ruiz. BUG.*/
    v_no_se_generara_case             BOOLEAN:=false;
    v_operador_para_join  VARCHAR2(3);
    v_temporal varchar2(500);
    v_table_base_name varchar2(100);
    v_alias_table_base_name varchar2(100);

    
  begin
    /* Seleccionamos el escenario primero */
      dbms_output.put_line ('REGLA RUL: #' || reg_detalle_in.RUL || '#');
      dbms_output.put_line ('REGLA RUL: #' || reg_detalle_in.TABLE_COLUMN || '#');
      case trim(reg_detalle_in.RUL)
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;
      when 'LKUPC' then
        /* (20150626) Angel Ruiz.  Se trata de hacer el LOOK UP con la tabla dimension de manera condicional */
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
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
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
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
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
        
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM*/
          --v_encontrado:='N';
          --FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          --LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              --v_encontrado:='Y';
            --end if;
          --END LOOP;
          --if (v_encontrado='Y') then
            --v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          --else
            --v_alias := reg_detalle_in.TABLE_LKUP;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP;
          --end if;
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        table_columns_lkup := split_string_punto_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_punto_coma (reg_detalle_in.IE_COLUMN_LKUP);

        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        
        /* Construyo el campo de SELECT */
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          valor_retorno := 'CASE WHEN (';
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = TRIM(ie_column_lkup(indx));
          
            if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
              if (indx = 1) then
                valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
              else
                valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
              end if;
            else 
              if (indx = 1) then
                valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
              else
                valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
              end if;
            end if;
          END LOOP;
          valor_retorno := valor_retorno || ') THEN -3 ELSE ' || proc_campo_value_condicion(reg_detalle_in.LKUP_COM_RULE, 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)') || ' END';
        else
          valor_retorno :=  proc_campo_value_condicion (reg_detalle_in.LKUP_COM_RULE, 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)');
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
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = TRIM(ie_column_lkup(indx));
            if (l_WHERE.count = 1) then
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  l_WHERE(l_WHERE.last) :=  'COALESCE(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            else
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  l_WHERE(l_WHERE.last) :=  ' AND COALESCE(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
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
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = reg_detalle_in.IE_COLUMN_LKUP;
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  l_WHERE(l_WHERE.last) := 'COALESCE(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
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
        dbms_output.put_line('El campo es: ' || reg_detalle_in.TABLE_COLUMN);
        l_FROM.extend;
        l_FROM_solo_tablas.extend;  /*(20161222) Angel Ruiz */
        /* (20150130) Angel Ruiz */
        /* Nueva incidencia. */
        if (regexp_instr (reg_detalle_in.TABLE_LKUP,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$')) then
          --if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+ *[-]* *([a-zA-Z_0-9]* *)+$')) then
          /* (20160629) Angel Ruiz. NF: Se aceptan tablas de LKUP que son SELECT que ademas tienen un ALIAS */
            v_alias := trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            --v_alias := REGEXP_SUBSTR(trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+ *[-]* *([a-zA-Z_0-9]* *)+$'), 2)), '^[a-zA-Z_0-9]+');
            --mitabla_look_up := reg_detalle_in.TABLE_LKUP;
            mitabla_look_up := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
            v_alias_incluido := 1;
            dbms_output.put_line('EXISTE ALIAS EN LA QUERY TABLE_LKUP');
            v_table_lkup_prima := v_alias; /*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
          else
            v_alias := 'LKUP_' || l_FROM.count;
            mitabla_look_up := '(' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ') "LKUP_' || l_FROM.count || '"';
            --mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
            v_alias_incluido := 0;
            dbms_output.put_line('NO EXISTE ALIAS EN LA QUERY TABLE_LKUP');
            v_table_lkup_prima := v_alias; /*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
          end if;
          --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
          l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
          /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
          --l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
          if (reg_detalle_in.OUTER is null) then
            l_FROM (l_FROM.last) := 'INNER JOIN ' || mitabla_look_up || ' ';
          else
            l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
          end if;
        else  /* La TABLA_LKUP no es una query */

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
              v_table_lkup_prima := substr(regexp_substr(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);/*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
              v_table_look_up := v_table_look_up;
              
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              v_table_lkup_prima:= v_table_look_up; /*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
              --v_table_look_up := OWNER_DM || '.' || v_table_look_up;
              v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
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
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
              /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
              --l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              if (reg_detalle_in.OUTER is null) then
                l_FROM (l_FROM.last) := 'INNER JOIN ' || mitabla_look_up || ' ';
              else
                l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
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
              v_table_lkup_prima := v_alias_table_look_up; /*(20170109) Angel Ruiz. BUG.Despues la uso para buscar en el metadato*/
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              v_table_lkup_prima := v_table_look_up; /*(20170109) Angel Ruiz. BUG.Despues la uso para buscar en el metadato*/
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              v_table_look_up := ESQUEMA_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: $$' || v_alias_table_look_up || '$$');
            dbms_output.put_line('La tabla de LKUP es: $$' || v_table_look_up || '$$');
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM_solo_tablas.FIRST .. l_FROM_solo_tablas.LAST
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
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
              /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
              --l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              if (reg_detalle_in.OUTER is null) then
                l_FROM (l_FROM.last) := 'INNER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              else
                l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              end if;
              
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
              /* (20170725) Angel Ruiz. BUG. Cuando no se pone Y en el campo OUTER debe hacerse INNER */
              --l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              if (reg_detalle_in.OUTER is null) then
                l_FROM (l_FROM.last) := 'INNER JOIN ' || mitabla_look_up || ' ';
              else
                l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              end if;
              
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
            end if;
          end if;
        end if;
        /*********************************/
        /* (20161227) Angel Ruiz. Ocurre que pueden venir Queries en la columna TABLE_BASE_NAME */
        /*********************************/
        if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
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
        table_columns_lkup := split_string_punto_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_punto_coma (reg_detalle_in.IE_COLUMN_LKUP);
        dbms_output.put_line ('¡¡¡¡¡¡¡¡¡¡¡¡¡HOLA HOLA !!!!!!!!!!!!!');
        
        
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/

        /*************************************************************************/
        /* (20170109) Angel Ruiz. BUG. Existen ocasiones en las que no es posible */
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
          if (reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD' or reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD_ANT' or 
          (reg_detalle_in.TABLE_NAME = 'NGA_PARQUE_DESC_MES' and (reg_detalle_in.TABLE_COLUMN = 'BAN_OBLIGATORIEDAD_ANT' or reg_detalle_in.TABLE_COLUMN = 'CVE_TRATAMIENTO_ABO_ANT' or 
          reg_detalle_in.TABLE_COLUMN = 'ID_PLAN_TARIFARIO'))
          or (reg_detalle_in.TABLE_NAME = 'NGG_TRANSACCIONES_DETAIL' and (reg_detalle_in.TABLE_COLUMN = 'IMP_DESCUENTOS_PCT_CLIENTE' ))
          ) then
            v_no_se_generara_case:=true;
          end if;
        else
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
          if (reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD' or reg_detalle_in.TABLE_COLUMN = 'ID_TRAZABILIDAD_ANT' or
          (reg_detalle_in.TABLE_NAME = 'NGA_PARQUE_DESC_MES' and (reg_detalle_in.TABLE_COLUMN = 'BAN_OBLIGATORIEDAD_ANT' or reg_detalle_in.TABLE_COLUMN = 'CVE_TRATAMIENTO_ABO_ANT' or 
          reg_detalle_in.TABLE_COLUMN = 'ID_PLAN_TARIFARIO'))
          or (reg_detalle_in.TABLE_NAME = 'NGG_TRANSACCIONES_DETAIL' and (reg_detalle_in.TABLE_COLUMN = 'IMP_DESCUENTOS_PCT_CLIENTE' ))
          ) then
            v_no_se_generara_case:=true;
          end if;
        end if;
        /* (20170109) Angel Ruiz. FIN BUG.*/
        
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
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN ifnull(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
        else
          /* Construyo el campo de SELECT */
          if (v_no_se_generara_case = false) then /*(20170109) Angel Ruiz. BUG: Hay campos con JOIN en los que no se va a generar CASE WHEN */
            if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
              valor_retorno := 'CASE WHEN (';
              FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
              LOOP
                /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0)
                then
                  --nombre_campo := extrae_campo_decode (ie_column_lkup(indx));
                  /* (20161117) Angel Ruiz. NF: Pueden venir funciones en los campos de join como */
                  /* UPPER, NVL, DECODE, ... */
                  dbms_output.put_line ('El campo por el que voy a hacer LookUp de la TABLE_BASE es: ' || TRIM(ie_column_lkup(indx)));
                  dbms_output.put_line ('La TABLE_BASE es: ' || TRIM(mi_tabla_base_name));
                  nombre_campo := extrae_campo (ie_column_lkup(indx));
                  dbms_output.put_line ('El campo por el que voy a hacer LooUp de la TABLE_BASE es: ' || TRIM(nombre_campo));
                  --SELECT * INTO l_registro
                  --FROM ALL_TAB_COLUMNS
                  --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                  --COLUMN_NAME = TRIM(nombre_campo);
                  SELECT * INTO l_registro2
                  FROM v_MTDT_CAMPOS_DETAIL
                  WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                  UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
                else
                  dbms_output.put_line ('El campo por el que voy a hacer LookUp de la TABLE_BASE es: ' || TRIM(ie_column_lkup(indx)));
                  dbms_output.put_line ('La TABLE_BASE es: ' || TRIM(mi_tabla_base_name));
                  --SELECT * INTO l_registro
                  --FROM ALL_TAB_COLUMNS
                  --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                  --COLUMN_NAME = TRIM(ie_column_lkup(indx));
                  SELECT * INTO l_registro2
                  FROM V_MTDT_CAMPOS_DETAIL
                  WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                  UPPER(trim(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
                end if;
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
                  if (indx = 1) then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IN (''NI#'', ''NO INFORMADO'') ';
                    else
                      valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' IN (''NI#'', ''NO INFORMADO'') ';
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IN (''NI#'''', ''NO INFORMADO'') ';
                    else
                      valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' IN (''NI#'', ''NO INFORMADO'') ';
                    end if;
                  end if;
                else
                  if (instr(l_registro2.TYPE, 'ARRAY') = 0) then
                    /* (20170725). Angel Ruiz. BUG cuando se trata de un campo ARRAY*/
                    /* NO escribo CASE para este tipo de campo */
                    if (indx = 1) then
                      /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                      if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                        valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' = -3 ';
                      else
                        valor_retorno := valor_retorno || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' = -3 ';
                      end if;
                    else
                      /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                      if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                        valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' = -3 ';
                      else
                        valor_retorno := valor_retorno || 'OR ' || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || mi_tabla_base_name_alias || '.' || l_registro2.COLUMN_NAME || ' = -3 ';
                      end if;
                    end if;
                  end if; /* if (instr(l_registro2.TYPE, 'ARRAY') = 0) then*/
                end if;
              END LOOP;
              /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
              --SELECT * INTO l_registro1
              --FROM ALL_TAB_COLUMNS
              --WHERE TABLE_NAME =  reg_detalle_in.TABLE_NAME and
              --COLUMN_NAME = reg_detalle_in.TABLE_COLUMN;
              SELECT * INTO l_registro2
              FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.TABLE_COLUMN);
              dbms_output.put_line ('Estoy donde quiero.');
              dbms_output.put_line ('El nombre de TABLE_NAME ES: ' || reg_detalle_in.TABLE_NAME);
              dbms_output.put_line ('El nombre de TABLE_COLUMN ES: ' || reg_detalle_in.TABLE_COLUMN);
              dbms_output.put_line ('El tipo de DATOS es: ' || l_registro2.TYPE);
              if (l_registro2.TYPE = 'NUMBER') then
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  --valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', -2) END';
                  if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                  /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                    --valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -2) END';
                    valor_retorno := valor_retorno || ') THEN -1 ELSE ' || 'coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -3) END';
                  else
                    --valor_retorno := valor_retorno || ') THEN 0 ELSE ' || 'ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0) END';
                    valor_retorno := valor_retorno || ') THEN 0 ELSE ' || 'coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0) END';
                  end if;
                else
                  if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                  /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                    --valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'ifnull(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) END';
                    valor_retorno := valor_retorno || ') THEN -1 ELSE ' || 'coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', -3) END';
                  else
                    valor_retorno := valor_retorno || ') THEN 0 ELSE ' || 'coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', 0) END';
                  end if;
                end if;
              elsif (UPPER(TRIM(l_registro2.TYPE)) = 'DATE') then
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE)) END';
                else
                  valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', CAST(''2000-01-01'' AS DATE)) END';
                end if;
              else
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  --valor_retorno := valor_retorno || ') THEN ''''NO INFORMADO'''' ELSE ' || 'NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''') END';
                  --valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'') END';
                  valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'') END';
                else
                  --valor_retorno := valor_retorno || ') THEN ''''NO INFORMADO'''' ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''') END';
                  --valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'ifnull(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'') END';
                  valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'') END';
                end if;
              end if;
            else  /* (20170109) Angel Ruiz. if table_columns_lkup.COUNT > 1 */
              /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
              SELECT * INTO l_registro2
              FROM v_MTDT_CAMPOS_DETAIL
              WHERE TABLE_NAME =  reg_detalle_in.TABLE_NAME and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN));
              if (l_registro2.TYPE = 'NUMBER') then
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', -2)';
                  /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                  if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                    --valor_retorno :=  '    ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -2)';
                    valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -3)';
                  else
                    --valor_retorno :=  '    ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                    valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                  end if;
                else
                  if (regexp_instr(reg_detalle_in.VALUE, '[A-Za-z0-9_]') = 0) then
                    /* (20170627) Angel Ruiz. Compruebo VALUE sólo sea un campo */
                    if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                    /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                      --valor_retorno :=  '    ifnull(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)';
                      valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', -3)';
                    else
                      --valor_retorno :=  '    ifnull(' || v_alias || '.' || reg_detalle_in.VALUE || ', 0)';
                      valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', 0)';
                    end if;
                  else
                    if (instr(reg_detalle_in.TABLE_COLUMN, 'CVE_') > 0) then
                    /* (20170929) Angel Ruiz. BUG. Aparece -2 cuando se trata de importes*/
                      --valor_retorno :=  '    ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -2)';
                      valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -3)';
                    else
                      --valor_retorno :=  '    ifnull(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                      valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', 0)';
                    end if;
                  end if;
                end if;
              elsif (l_registro2.TYPE = 'DATE') then
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''')';
                  valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE))';
                else
                  --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''')';
                  valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', CAST(''2000-01-01'' AS DATE))';
                end if;
              else
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''')';
                  valor_retorno :=  '    coalesce(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'')';
                else
                  --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''')';
                  valor_retorno :=  '    coalesce(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'')';
                end if;
              end if;
            end if;
          else  /* (20170109) Angel Ruiz. if (v_no_se_generara_case = false) then */
            /*(20170109) Angel Ruiz. BUG: Hay campos con JOIN en los que no se va a generar CASE WHEN */
            valor_retorno :=  '    ' || procesa_campo_filter(reg_detalle_in.VALUE);
          end if;
        end if;
        
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        l_WHERE_ON_clause.delete;   /* (20161222) Angel Ruiz */
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            --l_WHERE.extend;
            l_WHERE_ON_clause.extend;
            /* (20170127) Angel Ruiz. BUG. Cuando en TABLE_COLUMN_LKUP viene un campo con BETWEEN */
            /* entonces el operador de JOIN no sera el = sino ese BETWEEN que nos viene */
            if (regexp_instr(table_columns_lkup(indx), '[Bb][Ee][Tt][Ww][Ee][Ee][Nn]') = 0) then
              /* si no hay un between entonces el operador de join sera el operador por defecto, el = */
              v_operador_para_join := ' = ';
            else
              /* Si lo hay, entonces sera el mismo between, por lo que nuestro operador solo sera un blanco */
              v_operador_para_join := ' ';
            end if;
            /* (20170127) Angel Ruiz. Fin */
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna TABLE_COLUMN es: ' || reg_detalle_in.TABLE_COLUMN);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Table_Base_Name es: ' || mi_tabla_base_name);
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            /************************/
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0)
            then
              --nombre_campo := extrae_campo_decode (ie_column_lkup(indx));
              /* (20161117) Angel Ruiz. NF: Pueden venir funciones en los campos de join como */
              /* UPPER, NVL, DECODE, ... */
              nombre_campo := extrae_campo (ie_column_lkup(indx));
              /****************************************/
              /* (20170109) Angel Ruiz. BUG. Hay campos de los q no se puede hayar su tipo pq tienen muchas funciones */
              /****************************************/
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
                dbms_output.put_line('EXISTE VALOR IGUAL A TRUE. Tabla: ' || mi_tabla_base_name || ' Campo: ' || nombre_campo);
              end loop;
              if (v_existe_valor = true) then
                SELECT * INTO l_registro2
                FROM V_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
              end if;
            else  /* NO poseen funciones el campo por el que se va a hacer JOIN */
              --SELECT * INTO l_registro
              --FROM ALL_TAB_COLUMNS
              --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
              --COLUMN_NAME = TRIM(ie_column_lkup(indx));
              /****************************************/
              /* (20170109) Angel Ruiz. BUG. Hay campos de los q no se puede hayar su tipo pq tienen muchas funciones */
              /****************************************/
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx))))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                /* podemos encontrar el campo en el diccionario de datos */
                SELECT * INTO l_registro2
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
              end if;
            end if;
            if (l_WHERE_ON_clause.count = 1) then
              if (v_existe_valor = true) then
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number((l_registro2.LENGTH)) < 3 and l_registro2.NULABLE = 'Y') then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'coalesce(' || transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ', ''NI#'')' || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'coalesce(' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'coalesce(' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) ||  v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ie_column_lkup(indx) ||  v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) ||  v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                  if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(ie_column_lkup(indx), '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                /* No podemos encontar el campo en el diccionario de datos */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                elsif (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || v_operador_para_join || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                elsif (instr(ie_column_lkup(indx), '.') = 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                end if;
              end if;
            else  /* if (l_WHERE_ON_clause.count = 1) then */
              if (v_existe_valor = true) then
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number(l_registro2.LENGTH) < 3 and l_registro2.NULABLE = 'Y') then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || ', ''NI#'')' || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode (ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else                    
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  end if;
                else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */                
                  if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), mi_tabla_base_name_alias, 0) || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Ii][Ff] *\(') > 0 or regexp_instr(table_columns_lkup(indx), '[Ii][Ff] *\(') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(ie_column_lkup(indx), '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                elsif (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                  end if;
                elsif (instr(ie_column_lkup(indx), '.') = 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' ||table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                end if;
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          --l_WHERE.extend;
          l_WHERE_ON_clause.extend;
          /* (20170127) Angel Ruiz. BUG. Cuando en TABLE_COLUMN_LKUP viene un campo con BETWEEN */
          /* entonces el operador de JOIN no sera el = sino ese BETWEEN que nos viene */
          if (regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Bb][Ee][Tt][Ww][Ee][Ee][Nn]') = 0) then
            /* si no hay un between entonces el operador de join sera el operador por defecto, el = */
            v_operador_para_join := ' = ';
          else
            /* Si lo hay, entonces sera el mismo between, por lo que nuestro operador solo sera un blanco */
            v_operador_para_join := ' ';
          end if;
          /* (20170127) Angel Ruiz. Fin */
          
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0 and instr(reg_detalle_in.TABLE_LKUP, 'SELECT') = 0 )then
            if (l_WHERE_ON_clause.count = 1) then
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
              l_WHERE_ON_clause.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            else
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP;
              l_WHERE_ON_clause.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('#ESTOY EN EL LOOKUP. La Tabla es: $' || mi_tabla_base_name || '$');
            dbms_output.put_line('#ESTOY EN EL LOOKUP. La Columna es: $' || reg_detalle_in.IE_COLUMN_LKUP || '$');
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0)
            then
            --if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
              --nombre_campo := extrae_campo_decode (reg_detalle_in.IE_COLUMN_LKUP);
              nombre_campo := extrae_campo (reg_detalle_in.IE_COLUMN_LKUP);
              --SELECT * INTO l_registro2
              --FROM ALL_TAB_COLUMNS
              --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
              --COLUMN_NAME = trim(nombre_campo);
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
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
              end if;
            else  /* (20170109) Angel Ruiz. NO hay funciones en las columnas de JOIN */
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
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number(l_registro2.LENGTH) <3 and l_registro2.NULABLE = 'Y') then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || ', ''NI#'')' ||  v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else                      
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'coalesce(' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) ||  v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 and regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 and regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else                    
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) ||  v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              end if;
            else  /* sino es el primer campo del Where  */
              if (v_existe_valor = true) then
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number(l_registro2.LENGTH) <3 and l_registro2.NULABLE = 'Y') then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || ', ''NI#'')' || v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND coalesce(' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  end if;
                else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias, 0) || v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ii][Ff] *\(') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, mi_tabla_base_name_alias) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                end if;
              else  /* else del if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then                  
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || mi_tabla_base_name_alias || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE_ON_clause.extend;
          l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
        end if;
        /* (20161223) Angel Ruiz */
        /* Modifico esta parte para HIVE */
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || chr(10) || ' ON (';
        FOR indx IN l_WHERE_ON_clause.FIRST .. l_WHERE_ON_clause.LAST
        LOOP
          l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || l_WHERE_ON_clause(indx);
        END LOOP;
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || ')';
        
      when 'FUNCTION' then
        /* se trata de la regla FUNCTION */
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
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN ifnull(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
        else
          valor_retorno :=  '    ' || 'PKG_' || reg_detalle_in.TABLE_NAME || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
        end if;
      when 'DLOAD' then
        --valor_retorno :=  '    ' || ''' || ''TO_DATE ('''''' || fch_datos_in || '''''', ''''YYYYMMDD'''') '' || ''';
        valor_retorno := '    ' || 'date_format (''#VAR_FCH_CARGA#'', ''yyyy-MM-dd'')'; /* (20161223) Angel Ruiz */
      when 'DSYS' then
        --valor_retorno :=  '    ' || 'SYSDATE';
        valor_retorno := '    ' || 'current_date'; /* (20161223) Angel Ruiz */
      when 'CODE' then
        /* 20141204 Angel Ruiz. Como es codigo dinamico he de detectar si hay una comilla para poner dos */
        /* Esto lo añado nuevo y solo en este generador pq genera procesos que soportan retrasados */
        pos := 0;
        posicion_ant := 0;
        cadena_resul:= trim(reg_detalle_in.VALUE);
        lon_cadena := length (cadena_resul);
        if lon_cadena > 0 then
          valor_retorno := procesa_campo_filter (cadena_resul);
          /* Busco LA COMILLA */
          --sustituto := '''''';
          --loop
            --dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
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
        end if;
          /************/
        --valor_retorno := '    ' || trim(reg_detalle_in.VALUE);
        --valor_retorno := cadena_resul;
        --posicion := instr(valor_retorno, 'VAR_IVA');
        --if (posicion >0) then
          --cad_pri := substr(valor_retorno, 1, posicion-1);
          --cad_seg := substr(valor_retorno, posicion + length('VAR_IVA'));
          --valor_retorno :=  cad_pri || '21' || cad_seg;
        --end if;
        --posicion := instr(valor_retorno, '#VAR_FCH_CARGA#');
        --if (posicion >0) then
          --cad_pri := substr(valor_retorno, 1, posicion-1);
          --cad_seg := substr(valor_retorno, posicion + length('#VAR_FCH_CARGA#'));
          --valor_retorno :=  cad_pri || ''' || ''TO_DATE ('' || fch_datos_in || '''''', ''''YYYYMMDD'''') '' || ''' || cad_seg;
          --valor_retorno :=  '    ' || cad_pri || ' date_format(''#VAR_FCH_DATOS#'', ''yyyy-MM-dd'') ' || cad_seg; /* (20161208) Angel Ruiz */
        --end if;
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
        --valor_retorno :=  '    ' || sustituye_comillas_dinam(reg_detalle_in.VALUE);
        --valor_retorno := '    ' || reg_detalle_in.VALUE;
      when 'SEQ' then
        --valor_retorno := '    ' || OWNER_DM || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
        --if (instr(reg_detalle_in.VALUE, '.NEXTVAL') > 0) then
        --  valor_retorno := '    ' || reg_detalle_in.VALUE;
        --else
        --  valor_retorno := '    ' || reg_detalle_in.VALUE || '.NEXTVAL';
        --end if;
        --valor_retorno := '    ' || ''' || var_seqg || ''';
        /*(20170107) Angel Ruiz. NF: Secuencias en tablas de hechos */
        dbms_output.put_line('ESTOY EN LA REGLA SEQ.');
        --l_FROM.extend;
        --l_FROM (l_FROM.LAST) := ' LEFT OUTER JOIN (SELECT NVL(MAX(' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN || '), 0) maximo from ' || OWNER_DM || '.' || reg_detalle_in.TABLE_NAME || ') ' || reg_detalle_in.value || ' ';
        --l_FROM (l_FROM.LAST) := ' LEFT OUTER JOIN (SELECT ULT_VAL maximo from ' || OWNER_MTDT || '.MTDT_SEQUENCIAS WHERE ID_SEQ=''SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5) || ''') ' || 'SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5);
        --valor_retorno := '    ' || reg_detalle_in.value || '.' || 'maximo + (row_number() over (order by ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE || '))' ;
        --valor_retorno := '    ' || 'SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5) || '.' || 'maximo + (row_number() over (order by ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE || '))' ;
        --valor_retorno := '    ' || ESQUEMA_DM || '.nextval(''' ||  reg_detalle_in.VALUE || ''')';         
        valor_retorno := '     default';
        v_hay_regla_seq := true;
        --v_nombre_seq := 'SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5);
        v_nombre_seq := reg_detalle_in.VALUE;
        v_nombre_campo_seq := reg_detalle_in.TABLE_COLUMN;

        
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        /* (20170127) Angel Ruiz. BUG. Si ya lleva punto es que se le ha puesto el propietario */
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
          --valor_retorno :=  '    ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;
        else
          --valor_retorno :=  '    ' || reg_detalle_in.VALUE;
          valor_retorno := '    ' || reg_detalle_in.VALUE;
        end if;
        
      when 'VAR_FCH_INICIO' then
        --valor_retorno :=  '    ' || ''' || var_fch_inicio || ''';
        --valor_retorno :=  '    SYSDATE';
        --valor_retorno :=  '    TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')'; /*(20151221) Angel Ruiz BUG. Debe insertarse la fecha de inicio del proceso de insercion */
        /* (20161223) Angel Ruiz. Ocurre que esta regla la podemos usar tanto en */
        /* campos DATE como en campos DATETIME, por lo que hay que saber de que tipo de campo se trata */
        select type into v_tipo_campo from v_MTDT_CAMPOS_DETAIL where table_name = reg_detalle_in.TABLE_NAME and column_name = reg_detalle_in.TABLE_COLUMN;
        if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
          --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd hh:mm:ss'')';
          valor_retorno := '    ' || 'var_fch_inicio';
        else
          --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd'')';
          valor_retorno := '    ' || 'var_fch_inicio';
        end if;
      when 'VAR_FCH_CARGA' then
          --valor_retorno := '     ' || '''#VAR_FCH_DATOS#'''; /* (20161219) Angel Ruiz */
          --valor_retorno := '     ' || 'fch_carga_in'; /* (20161219) Angel Ruiz */
          valor_retorno := '     ' || 'fch_datos_in'; /* (20161219) Angel Ruiz */
      when 'VAR_FCH_INI_MES' then
        /* (20170522) Angel Ruiz. Incñuyo una nueva variable VAR_FCH_INI_MES que contiene el primer dia del mes */
        --valor_retorno := '     ' || 'date_format(''#VAR_FCH_DATOS#'', ''yyyy-MM-01'')';
        valor_retorno := '     ' || 'TO_DATE(fch_datos_in, ''YYYYMM01'')';          
      when 'VAR_FIN_DEFAULT' then
          valor_retorno := '    ' || '''99991231''';
      when 'VAR_USER' then
          valor_retorno := '    ' || '''#VAR_USER#''';
      when 'VAR' then
        /* Se toma el valor de una variable de entorno */
        --if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '    ' || ''' || fch_datos_in || ''';        
        --end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_DATOS' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '     ' || 'fch_datos_in';        
          --valor_retorno := '     ' || '''#VAR_FCH_DATOS#'''; /* (20161208) Angel Ruiz */
          valor_retorno := '     ' || 'TO_DATE(fch_datos_in, ''YYYYMMDD'')'; /* (20161208) Angel Ruiz */
          
        end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno := '     ' || 'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '     ' || 'fch_datos_in';        
          --valor_retorno := '     ' || '''#VAR_FCH_DATOS#''';  /* (20161208) Angel Ruiz */      
          valor_retorno := '     ' || 'TO_DATE(fch_datos_in, ''YYYYMMDD'')';  /* (20161208) Angel Ruiz */      
          
        end if;
        if reg_detalle_in.VALUE =  'VAR_PAIS_TM' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          valor_retorno := '    ' || '1';
        end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_INICIO' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          /* (20161208) Angel Ruiz. Ocurre que esta regla la podemos usar tanto en */
          /* campos DATE como en campos DATETIME, por lo que hay que saber de que tipo de campo se trata */
          select trim(data_type) into v_tipo_campo from mtdt_modelo_detail where trim(table_name) = reg_detalle_in.TABLE_NAME and trim(column_name) = reg_detalle_in.TABLE_COLUMN;
          if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
            --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd hh:mm:ss'')';
            --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd hh:mm:ss'')';
            valor_retorno := '    ' || 'var_fch_inicio';
          else
            --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd'')';
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
            
          
          /* (20161111) Angel Ruiz. NF FIN. Puede haber ALIAS EN LA TABLA DE LOOKUP */
        
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM*/
          --v_encontrado:='N';
          --FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          --LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              --v_encontrado:='Y';
            --end if;
          --END LOOP;
          --if (v_encontrado='Y') then
            --v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          --else
            --v_alias := reg_detalle_in.TABLE_LKUP;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP;
          --end if;
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
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN ifnull(' || v_value || ', -2) ELSE ' || trim(constante) || ' END';
        else
          /* Construyo el campo de SELECT */
          if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
            valor_retorno := 'CASE WHEN (';
            FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
            LOOP
              SELECT * INTO l_registro
              FROM ALL_TAB_COLUMNS
              WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
              COLUMN_NAME = TRIM(ie_column_lkup(indx));
            
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                end if;
              else 
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                end if;
              end if;
            END LOOP;
            valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'ifnull(' || v_value || ', -2) END';
          else
            valor_retorno :=  '    ifnull(' || v_value || ', -2)';
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
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = TRIM(ie_column_lkup(indx));
            if (l_WHERE.count = 1) then
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  'ifnull(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  'ifnull(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            else
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ifnull(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
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
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = reg_detalle_in.IE_COLUMN_LKUP;
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) := 'ifnull(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ifnull(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
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

/*************/

/************/

begin
  /* (20141223) ANGEL RUIZ */
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO OWNER_RD FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_RD';
  SELECT VALOR INTO OWNER_DWH FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DWH';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';  
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  SELECT VALOR INTO ESQUEMA_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'ESQUEMA_DM';
  SELECT VALOR INTO PAIS FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PAIS_DM';
  --SELECT VALOR INTO VAR_REGS_MEDIA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'VAR_REGS_MEDIA';
  SELECT VALOR INTO NAME_DM_FULL FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM_FULL';

  --SELECT VALOR INTO v_MULTIPLICADOR_PROC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'MULTIPLICADOR_PROC';
  
  /* (20141223) FIN*/

  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    v_VAR_PCT_COMISIONES:=false; /* (Angel Ruiz) NF. Variable #VAR_PCT_COMISIONES#*/
    dbms_output.put_line ('Estoy en el primero LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME);
    nombre_fich_carga := 'load_he_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql';
    fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
    --fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
    fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W', 32767);
    --nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, instr(reg_tabla.TABLE_NAME, '_')+1); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
    nombre_tabla_reducido := reg_tabla.TABLE_NAME;
    --nombre_tabla_base_reducido := substr(reg_tabla.TABLE_BASE_NAME, 4); /* Le quito al nombre de la tabla los caracteres SA_ */
    /* Angel Ruiz (20150311) Hecho porque hay paquetes que no compilan porque el nombre es demasiado largo*/
    if (length(reg_tabla.TABLE_NAME) < 25) then
      nombre_proceso := reg_tabla.TABLE_NAME;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    nombre_proceso := reg_tabla.TABLE_NAME;
    /* (20150414) Angel Ruiz. Incidencia. El nombre de la partición es demasiado largo */
    if (length(nombre_tabla_reducido) <= 18) then
      v_nombre_particion := 'PA_' || nombre_tabla_reducido;
    else
      v_nombre_particion := nombre_tabla_reducido;
    end if;
    v_nombre_particion := reg_tabla.TABLE_NAME;
    /* (20151112) Angel Ruiz. BUG. Si el nombre de la tabla es superior a los 19 caracteres*/
    /* El nombre de la tabla que se crea T_*_YYYYMMDD supera los 30 caracteres y da error*/
    /* (20241011). Angel Ruiz. Para PostgreSQL no se va a crear tablas T_*_YYYYMMDD sino que las tablas */
    /*  van a ser tablas del tipo DWH_TABLE_NAME_YYYYMMDD ya que será la partición que se añadirá a la tabla */
    /* de los hechos DWH_ */
    if (length(nombre_tabla_reducido) > 19) then
      nombre_tabla_T := substr(nombre_tabla_reducido,1, length(nombre_tabla_reducido) - (length(nombre_tabla_reducido) - 19));
    else
      nombre_tabla_T := nombre_tabla_reducido;
    end if;
    --nombre_tabla_T := 'T_' || reg_tabla.TABLE_NAME;
    /* (20241011) Ángel Ruiz Cantón. */
    nombre_tabla_T := reg_tabla.TABLE_NAME;
    
    --UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
    lista_scenarios_presentes.delete;
    /******/
    /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
    /******/
    dbms_output.put_line ('Comienzo la generacion del PACKAGE DEFINITION');

    /* Tercero genero los metodos para los escenarios */
    open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      lista_scenarios_presentes.EXTEND;
      lista_scenarios_presentes(lista_scenarios_presentes.LAST) := reg_scenario.SCENARIO;      
    end loop; /* fin del LOOP MTDT_SCENARIO  */
    close MTDT_SCENARIO;

    dbms_output.put_line ('Estoy en PACKAGE IMPLEMENTATION. :-)');
    

    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '-- ### INICIO DEL SCRIPT');
    UTL_FILE.put_line(fich_salida_pkg, '');
    /* Tercero genero los cuerpos de los metodos que implementan los escenarios */
    open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
      v_hay_regla_seq:=false; /*(20170107) Angl Ruiz. NF: Reglas SEQ */
      if (reg_scenario.SCENARIO = 'N')  /* Proceso el escenario NEW */
      then
        /* SCENARIO NUEVO */
        dbms_output.put_line ('Estoy dentro del scenario NUEVO');
        UTL_FILE.put_line(fich_salida_pkg, '-- ### ESCENARIO NUEVO ###');
        /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
        l_FROM.delete;
        l_WHERE.delete;
        l_FROM_solo_tablas.delete;
        /* Fin de la inicialización */
        
        lista_variables_rownumber.delete;
        
        UTL_FILE.put_line(fich_salida_pkg, 'DROP FUNCTION IF EXISTS ' || OWNER_TC || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');
        UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_TC || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8))');
        UTL_FILE.put_line(fich_salida_pkg, 'returns integer AS $$');
        UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
        UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas INTEGER := 0;');
        UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio TIMESTAMP := current_timestamp;');
        UTL_FILE.put_line(fich_salida_pkg, '  v_error_code text;');
        UTL_FILE.put_line(fich_salida_pkg, '  v_error_msg text;');
        UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');
        UTL_FILE.put_line(fich_salida_pkg, '');
        UTL_FILE.put_line(fich_salida_pkg, '');
        UTL_FILE.put_line(fich_salida_pkg,'INSERT');
        /* (20250320). Angel Ruiz . -i */
        /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
        /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
        /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
        --UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_RD || '.' || nombre_tabla_reducido || '_T');
        --UTL_FILE.put_line(fich_salida_pkg,'(');
        dbms_output.put_line ('El nombre de nombre_tabla_reducido es: ' || nombre_tabla_reducido);
        if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
          UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_TC || '.' || nombre_tabla_reducido || '_T');
        else
          UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_RD || '.' || nombre_tabla_reducido || '_T');
        end if;
        /* (20250320). Angel Ruiz . -f */
        UTL_FILE.put_line(fich_salida_pkg,'(');
        open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
        primera_col := 1;
        loop
          fetch MTDT_TC_DETAIL
          into reg_detail;
          exit when MTDT_TC_DETAIL%NOTFOUND;
          if primera_col = 1 then
            UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
            primera_col := 0;
          else
            UTL_FILE.put_line(fich_salida_pkg, ' ,' || reg_detail.TABLE_COLUMN);
          end if;        
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
        UTL_FILE.put_line(fich_salida_pkg,'SELECT');
        open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
        primera_col := 1;
        loop
          fetch MTDT_TC_DETAIL
          into reg_detail;
          exit when MTDT_TC_DETAIL%NOTFOUND;
          columna := genera_campo_select (reg_detail);
          if primera_col = 1 then
            UTL_FILE.put_line(fich_salida_pkg,columna || ' ' || reg_detail.TABLE_COLUMN);
            primera_col := 0;
          else
            UTL_FILE.put_line(fich_salida_pkg, ', ' || columna || ' ' || reg_detail.TABLE_COLUMN);
          end if;        
        end loop;
        close MTDT_TC_DETAIL;
        /****/
        /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
        /****/      
        /****/
        /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
        /****/    
        dbms_output.put_line ('Despues del SELECT');
        --dbms_output.put_line ('El valor que han cogifo v_FROM:' || v_FROM);
        --dbms_output.put_line ('El valor que han cogifo v_WHERE:' || v_WHERE);
        dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
        UTL_FILE.put_line(fich_salida_pkg,'FROM');
        --UTL_FILE.put_line(fich_salida_pkg, '   app_mvnosa.'  || reg_scenario.TABLE_BASE_NAME || ''' || ''_'' || fch_datos_in;');
        --UTL_FILE.put_line(fich_salida_pkg, '   ' || procesa_campo_filter_dinam(reg_scenario.TABLE_BASE_NAME));
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
          campo_filter := procesa_campo_filter(reg_scenario.FILTER);
          UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          dbms_output.put_line ('Despues de procesar el campo FILTER');
        end if;
        UTL_FILE.put_line(fich_salida_pkg, ';');
        UTL_FILE.put_line(fich_salida_pkg,'  GET DIAGNOSTICS num_filas_insertadas := ROW_COUNT;');
        UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_insertadas;');
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
        /* (20161117) Angel Ruiz. NF: Puede venir cualquier escenario */
        /* CUALQUIER OTRO SCENARIO */
        dbms_output.put_line ('Estoy dentro del scenario $' || reg_scenario.SCENARIO || '$');
        UTL_FILE.put_line(fich_salida_pkg, '-- ### ESCENARIO ' || reg_scenario.SCENARIO || ' ###');
        UTL_FILE.put_line(fich_salida_pkg, '');
        /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
        l_FROM.delete;
        l_WHERE.delete;
        l_FROM_solo_tablas.delete;
        /* Fin de la inicialización */
        lista_variables_rownumber.delete;
        UTL_FILE.put_line(fich_salida_pkg, '');
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
        UTL_FILE.put_line(fich_salida_pkg, 'DROP FUNCTION IF EXISTS ' || OWNER_TC || '.' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');
        UTL_FILE.put_line(fich_salida_pkg, 'CREATE FUNCTION ' || OWNER_TC || '.fnc_' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8))');
        UTL_FILE.put_line(fich_salida_pkg, 'RETURNS integer AS $$');
        UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
        UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas INT := 0;');
        UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio TIMESTAMP := current_timestamp;');
        UTL_FILE.put_line(fich_salida_pkg, '  v_error_code text;');
        UTL_FILE.put_line(fich_salida_pkg, '  v_error_msg text;');
        UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');
        UTL_FILE.put_line(fich_salida_pkg, '');          
        UTL_FILE.put_line(fich_salida_pkg,'INSERT');
        /* (20250320). Angel Ruiz . -i */
        /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
        /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
        /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
        --UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in ||');
        --UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_RD || '.' || nombre_tabla_reducido || '_T');
        if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
          UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_TC || '.' || nombre_tabla_reducido || '_T');
        else
          UTL_FILE.put_line(fich_salida_pkg,'INTO ' || OWNER_RD || '.' || nombre_tabla_reducido || '_T');
        end if;
        /* (20250320). Angel Ruiz . -f */
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
          if primera_col = 1 then
            UTL_FILE.put_line(fich_salida_pkg, '  ' || reg_detail.TABLE_COLUMN);
            primera_col := 0;
          else
            UTL_FILE.put_line(fich_salida_pkg, ' , ' || reg_detail.TABLE_COLUMN);
          end if;        
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
          columna := genera_campo_select (reg_detail);
          if primera_col = 1 then
            UTL_FILE.put_line(fich_salida_pkg, columna || ' ' || reg_detail.TABLE_COLUMN);
            primera_col := 0;
          else
            UTL_FILE.put_line(fich_salida_pkg, ', ' || columna || ' ' || reg_detail.TABLE_COLUMN);
          end if;
        end loop;
        close MTDT_TC_DETAIL;
        /****/
        /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
        /****/      
        /****/
        /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
        /****/    
        dbms_output.put_line ('Despues del SELECT');
        --dbms_output.put_line ('El valor que han cogifo v_FROM:' || v_FROM);
        --dbms_output.put_line ('El valor que han cogifo v_WHERE:' || v_WHERE);
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
          campo_filter := procesa_campo_filter (reg_scenario.FILTER);
          UTL_FILE.put_line (fich_salida_pkg, campo_filter);
        end if;
        dbms_output.put_line ('Despues de procesar el campo FILTER');
        UTL_FILE.put_line(fich_salida_pkg, ';');
        UTL_FILE.put_line(fich_salida_pkg,'  GET DIAGNOSTICS num_filas_insertadas := ROW_COUNT;');
        UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_insertadas;');
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
        UTL_FILE.put_line(fich_salida_pkg, '');
      /* (20161117) Angel Ruiz. FIN NF: Puede venir cualquier escenario */

      end if;   /* FIN de la generacion de las funciones*/
    
    end loop;
    close MTDT_SCENARIO;

/*/////////////////////////////////// */

    UTL_FILE.put_line(fich_salida_pkg, 'DROP PROCEDURE IF EXISTS ' || OWNER_TC || '.prc_' || nombre_proceso || ';');
    UTL_FILE.put_line(fich_salida_pkg, 'CREATE PROCEDURE ' || OWNER_TC || '.prc_' || nombre_proceso || ' (fch_carga_in varchar(8), fch_datos_in varchar(8), forzado_in varchar(1))');
    UTL_FILE.put_line(fich_salida_pkg, 'LANGUAGE plpgsql');
    UTL_FILE.put_line(fich_salida_pkg, 'AS $$');
    UTL_FILE.put_line(fich_salida_pkg, 'DECLARE');
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_new int := 0;');
    UTL_FILE.put_line(fich_salida_pkg, '  num_reg INT := 0;');
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_' || lista_scenarios_presentes(indx) || ' int := 0;');
    END LOOP;
    UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar int;');
    UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP := current_timestamp;');
    UTL_FILE.put_line(fich_salida_pkg, '  v_fch_datos text;');
    UTL_FILE.put_line(fich_salida_pkg, '  v_fch_particion text;');
    UTL_FILE.put_line(fich_salida_pkg, '  msg TEXT;');
    UTL_FILE.put_line(fich_salida_pkg, '  errno TEXT;');
    UTL_FILE.put_line(fich_salida_pkg, '  sql TEXT;');
    UTL_FILE.put_line(fich_salida_pkg, 'BEGIN');
    --UTL_FILE.put_line(fich_salida_pkg, '  SET numero_reg_updt = 0;');
    --UTL_FILE.put_line(fich_salida_pkg, '  SET numero_reg_hist = 0;');
    --UTL_FILE.put_line(fich_salida_pkg, '  SET numero_reg_read = 0;');
    UTL_FILE.put_line(fich_salida_pkg, '  /* Calculo la fecha de la partición */');
    UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''Inicio del proceso de carga: ' || OWNER_TC || '.prc_' || nombre_proceso || ''';');
    UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''El valor del parámetro fch_carga_in es: %.'', fch_carga_in;');
    UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''El valor del parámetro fch_datos_in es: %.'', fch_datos_in;');
    UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''El valor del parámetro forzado_in es: %.'', forzado_in;');
    --UTL_FILE.put_line(fich_salida_pkg, '  CALL ' || OWNER_TC || '.siguiente_paso(''' || nombre_fich_carga || ''', to_date(fch_datos_in, ''YYYYMMDD''), siguiente_paso_a_ejecutar);');
    UTL_FILE.put_line(fich_salida_pkg, '  CALL ' || OWNER_TC || '.prc_comun_siguiente_paso(''' || nombre_fich_carga || ''', to_date(fch_carga_in, ''YYYYMMDD''), to_date(fch_datos_in, ''YYYYMMDD''), siguiente_paso_a_ejecutar);');    
    UTL_FILE.put_line(fich_salida_pkg, '  RAISE NOTICE ''Después de la llamada a siguiente_paso. El valor siguiente_paso_a_ejecutar es: %: '', siguiente_paso_a_ejecutar;');

    UTL_FILE.put_line(fich_salida_pkg, '  if (forzado_in = ''F'') then');
    UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := 1;');
    UTL_FILE.put_line(fich_salida_pkg, '  end if;');
    UTL_FILE.put_line(fich_salida_pkg, '  if (siguiente_paso_a_ejecutar = 1) then');
    UTL_FILE.put_line(fich_salida_pkg, '    inicio_paso_tmr := current_timestamp;');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El valor del timestamp de inicio de paso es: %.'', inicio_paso_tmr;');    
    --UTL_FILE.put_line(fich_salida_pkg, '    /* Truncamos la tabla antes de insertar los nuevos registros por si se lanza dos veces*/');
    --UTL_FILE.put_line(fich_salida_pkg, '    TRUNCATE TABLE ' || OWNER_TC || '.T_' || nombre_tabla_reducido || ';');
    /* (20200409) Angel Ruiz. NF. Tengo en cuenta el particionado Semanal para calcular la particion q he de crear */
    OPEN c_mtdt_modelo_logico_COLUMNA (reg_tabla.TABLE_NAME);
    v_tipo_particionado := 'S';  /* (20150821) Angel Ruiz. Por defecto la tabla no estara particionada */
    LOOP
      FETCH c_mtdt_modelo_logico_COLUMNA
      INTO r_mtdt_modelo_logico_COLUMNA;
      EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
      --if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??F_',1,'i') >0 AND 
      if ((regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) AND 
      (upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_DIA' or upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_DAY' or upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'DAY')
      and r_mtdt_modelo_logico_COLUMNA.PARTICIONADO IS NULL)
      or (r_mtdt_modelo_logico_COLUMNA.PARTICIONADO IS NOT NULL and (r_mtdt_modelo_logico_COLUMNA.PARTICIONADO = 'CVE_DIA'))
      then 
        /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_DIA ==> PARTICIONADO DIARIO */
        v_tipo_particionado := 'D';   /* Particionado Diario */
        v_CVE_DIA_es_col := 1;
      end if;
      if ( regexp_count(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '^?+_FCT$',1,'i') >0 AND
      (
        ( 
          r_mtdt_modelo_logico_COLUMNA.PARTICIONADO IS NULL AND
          (upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'FCT_DT_KEY')
        ) 
        or
        (
          r_mtdt_modelo_logico_COLUMNA.PARTICIONADO IS NOT NULL and
          (upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'FCT_DT_KEY' and upper(r_mtdt_modelo_logico_COLUMNA.PARTICIONADO) = 'FCT_DT_KEY')
        )
      )
      ) then 
        /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_DIA ==> PARTICIONADO DIARIO */
        v_tipo_particionado := 'D';   /* Particionado Diario */
        v_FCT_DT_KEY_es_col := 1;
      end if;
      if ((regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) AND 
      upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_WEEK') then 
        /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_WEEK ==> PARTICIONADO SEMANAL */
        v_tipo_particionado := 'W';   /* Particionado Semanal */
        v_CVE_WEEK_es_col := 1;
      end if;
      /* Gestionamos el posible particionado de la tabla */
      --if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4) ,'??F_',1,'i') >0 AND
      if ((regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) AND
      upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_MES') then 
        /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_MES ==> PARTICIONADO MENSUAL */
        if (reg_tabla.PARTICIONADO = 'M24') then
          /* (20150918) Angel Ruiz. NF: Se trata del particionado para BSC. Mensual pero 24 Particiones fijas.*/
          /* La filosofia cambia */
            v_tipo_particionado := 'M24';   /* Particionado Mensual */
        else
          v_tipo_particionado := 'M';   /* Particionado Mensual, aunque para una tabla de Agregados*/
        end if;
        v_CVE_MES_es_col := 1;
      end if;
      --if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??A_',1,'i') >0 AND
      if ((regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+A_',1,'i') >0) AND
      upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_MES') then
        /* SE TRATA DE UNA TABLA DE AGREGADOS CON PARTICIONAMIENTO POR MES */
        v_tipo_particionado := 'M';   /* Particionado Mensual, aunque para una tabla de Agregados*/
      end if;
    END LOOP;
    CLOSE c_mtdt_modelo_logico_COLUMNA;
    /* (20200409) Angel Ruiz. Fin NF: Tengo en cuenta el particionado Semanal para calcular la particion q he de crear  */
    /* (20200409) Angel Ruiz. NF: Tengo en cuenta el particionado Semanal para calcular la particion q he de crear*/
    if (v_tipo_particionado = 'D') then
      /* Hablamos de un particionado normal diario */
      /* Elijo la opción de siempre borrar y crear la partición en lugar de consultar si existe la misma */
      UTL_FILE.put_line(fich_salida_pkg,'    /* Gestiono la partición en la que se van a insertar los nuevos registros */');
      UTL_FILE.put_line(fich_salida_pkg,'    /* Crearé una nueva tabla que después la añadiré como partición a la tabla particionada ' || reg_tabla.TABLE_NAME || ' */');
      /* (20250320). Angel Ruiz . -i */
      /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
      /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
      /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
      if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg,'    drop table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T;');
        UTL_FILE.put_line(fich_salida_pkg,'    create table if not exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T (like ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ' INCLUDING DEFAULTS INCLUDING CONSTRAINTS);');
      else
        UTL_FILE.put_line(fich_salida_pkg,'    drop table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T;');
        UTL_FILE.put_line(fich_salida_pkg,'    create table if not exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T (like ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ' INCLUDING DEFAULTS INCLUDING CONSTRAINTS);');
      end if;
      if v_CVE_DIA_es_col = 1 THEN
        UTL_FILE.put_line(fich_salida_pkg,'    v_fch_particion := to_char(to_date(fch_datos_in, ''YYYYMMDD'') + INTERVAL ''1 day'', ''YYYYMMDD'');');
        /* (20250320). Angel Ruiz . -i */
        /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
        /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
        /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T add constraint ' || reg_tabla.TABLE_NAME || '_' || ''' || fch_datos_in || ''_'' || v_fch_particion || '' CHECK (CVE_DIA >= '' || fch_datos_in || '' AND CVE_DIA < '' || v_fch_particion || '')'';');
        if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T add constraint ' || reg_tabla.TABLE_NAME || '_' || ''' || fch_datos_in || ''_'' || v_fch_particion || '' CHECK (CVE_DIA >= '' || fch_datos_in || '' AND CVE_DIA < '' || v_fch_particion || '')'';');
        else
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T add constraint ' || reg_tabla.TABLE_NAME || '_' || ''' || fch_datos_in || ''_'' || v_fch_particion || '' CHECK (CVE_DIA >= '' || fch_datos_in || '' AND CVE_DIA < '' || v_fch_particion || '')'';');
        end if;
      else
        UTL_FILE.put_line(fich_salida_pkg,'    v_fch_particion := to_char(to_date(fch_datos_in, ''YYYYMMDD'') + INTERVAL ''1 day'', ''YYYYMMDD'');');
        /* (20250320). Angel Ruiz . -i */
        /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
        /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
        /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T add constraint ' || reg_tabla.TABLE_NAME || '_' || ''' || fch_datos_in || ''_'' || v_fch_particion || '' CHECK (FCT_DT_KEY >= '' || fch_datos_in || '' AND FCT_DT_KEY < '' || v_fch_particion || '')'';');
        if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T add constraint ' || reg_tabla.TABLE_NAME || '_' || ''' || fch_datos_in || ''_'' || v_fch_particion || '' CHECK (FCT_DT_KEY >= '' || fch_datos_in || '' AND FCT_DT_KEY < '' || v_fch_particion || '')'';');
        else
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T add constraint ' || reg_tabla.TABLE_NAME || '_' || ''' || fch_datos_in || ''_'' || v_fch_particion || '' CHECK (FCT_DT_KEY >= '' || fch_datos_in || '' AND FCT_DT_KEY < '' || v_fch_particion || '')'';');
        end if;
      end if;
    end if;

    if (v_tipo_particionado = 'W') then
    /* Hablamos de un particionado semanal */
      /* Elijo la opción de siempre borrar y crear la partición en lugar de consultar si existe la misma */
      /* (20250320). Angel Ruiz . -i */
      /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
      /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
      /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
      if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg,'    /* Gestiono la partición en la que se van a insertar los nuevos registros */');
        UTL_FILE.put_line(fich_salida_pkg,'    /* Crearé una nueva tabla que después la añadiré como partición a la tabla particionada ' || reg_tabla.TABLE_NAME || ' */');
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''drop table if exists ' || OWNER_TC || '.T_' || reg_tabla.TABLE_NAME || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''create table if not exists ' || OWNER_TC || '.T_' || reg_tabla.TABLE_NAME || ' (like ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ' INCLUDING DEFAULTS INCLUDING CONSTRAINTS)'';');
      else
        UTL_FILE.put_line(fich_salida_pkg,'    /* Gestiono la partición en la que se van a insertar los nuevos registros */');
        UTL_FILE.put_line(fich_salida_pkg,'    /* Crearé una nueva tabla que después la añadiré como partición a la tabla particionada ' || reg_tabla.TABLE_NAME || ' */');
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''drop table if exists ' || OWNER_RD || '.T_' || reg_tabla.TABLE_NAME || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''create table if not exists ' || OWNER_RD || '.T_' || reg_tabla.TABLE_NAME || ' (like ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ' INCLUDING DEFAULTS INCLUDING CONSTRAINTS)'';');
      end if;
      /* (20250320). Angel Ruiz . -f */
      --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''drop table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in;');
      --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE ''create table if not exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in || '' ( like '' || ''' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ''' || '' including defaults including constraints)'';');
    end if;
    /* (20200409) Angel Ruiz. NF FIN: Tengo en cuenta el particionado Semanal para calcular la particion q he de crear*/
    
    UTL_FILE.put_line(fich_salida_pkg, '');
    
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_' || lista_scenarios_presentes(indx) || ' := numero_reg_' || lista_scenarios_presentes(indx) || ' + ' || OWNER_TC || '.fnc_' || lista_scenarios_presentes(indx) || '_' || nombre_proceso || ' (fch_carga_in, fch_datos_in);');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros insertados en el escenario ' || lista_scenarios_presentes(indx) || ' es: %.'', numero_reg_' || lista_scenarios_presentes(indx) || ';');
      UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_new := numero_reg_new + numero_reg_' || lista_scenarios_presentes(indx) || ';' );
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros nuevos acumulados es: %'', numero_reg_new;');
    END LOOP;
    UTL_FILE.put_line(fich_salida_pkg, '    /* Este tipo de procesos tienen dos pasos, y ha terminado OK el primer paso por eso aparece un 0 en el siguiente campo */');
    --UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_TC || '.inserta_monitoreo (''' || nombre_fich_carga || ''', 1, 0, numero_reg_new, 0, 0, 0, 0, fch_datos_in, inicio_paso_tmr);');
    UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_carga || ''', 1, 0, numero_reg_new, 0, 0, numero_reg_new, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');    
    --UTL_FILE.put_line(fich_salida_pkg, '    commit;');
    UTL_FILE.put_line(fich_salida_pkg, '    /* Fin del primer paso */');
    UTL_FILE.put_line(fich_salida_pkg, '    /* Comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '    inicio_paso_tmr := clock_timestamp();');
    UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Comienza el segundo paso'';');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El timestamp del comienzo del paso es: %'', inicio_paso_tmr;');
    /* (20250320). Angel Ruiz . -i */
    /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
    /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
    /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
    if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg, '    SELECT COUNT(*) INTO num_reg FROM ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T;');    
    else
        UTL_FILE.put_line(fich_salida_pkg, '    SELECT COUNT(*) INTO num_reg FROM ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T;');
    end if;
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros final que se van a intercambiar es: %'', num_reg;');        
    /* (20200409) Angel Ruiz. NF: Tengo en cuenta el particionado */
    if (v_tipo_particionado = 'D') then
      /* Se trata de particionamiento diario */
      --UTL_FILE.put_line(fich_salida_pkg, '    v_fch_particion := to_char(to_date(fch_datos_in, ''YYYYMMDD'') + INTERVAL ''1 day'', ''YYYYMMDD'');');
      UTL_FILE.put_line(fich_salida_pkg, '');
      /* (20250320). Angel Ruiz . -i */
      /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
      /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
      /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
      if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' drop constraint ' ||  reg_tabla.TABLE_NAME || '_'' || fch_datos_in || ''_'' || v_fch_particion;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del drop constraint'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Ejecutando truncate_local_data_after_distributing_table...'';');
        UTL_FILE.put_line(fich_salida_pkg, '    sql := format(''SELECT truncate_local_data_after_distributing_table(%L)'',''' || lower(OWNER_TC) || '.' || lower(reg_tabla.TABLE_NAME) || '_'' || fch_datos_in);');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE sql;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después de ejecutar: %'', sql;');
        UTL_FILE.put_line(fich_salida_pkg, '    -- Purgado de particiones antiguas');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''% Purgando las particiones obsoletas...'',to_char(clock_timestamp(), ''YYYYMMDD HH24:MI:SS'');');
        UTL_FILE.put_line(fich_salida_pkg, '    call raw.prc_adm_clean_anl_partitions (''' || lower(reg_tabla.TABLE_NAME) || ''');');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''% Purgado de particiones obsoletas finalizado.'',to_char(clock_timestamp(), ''YYYYMMDD HH24:MI:SS'');');
      else
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' drop constraint ' ||  reg_tabla.TABLE_NAME || '_'' || fch_datos_in || ''_'' || v_fch_particion;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del drop constraint'';');
        UTL_FILE.put_line(fich_salida_pkg, '    sql := format(''SELECT truncate_local_data_after_distributing_table(%L)'',''' || lower(OWNER_RD) || '.' || lower(reg_tabla.TABLE_NAME) || '_'' || fch_datos_in);');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE sql;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después de ejecutar: %'', sql;');
        UTL_FILE.put_line(fich_salida_pkg, '    -- Purgado de particiones antiguas');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''% Purgando las particiones obsoletas...'',to_char(clock_timestamp(), ''YYYYMMDD HH24:MI:SS'');');
        UTL_FILE.put_line(fich_salida_pkg, '    call raw.prc_adm_clean_anl_partitions (''' || lower(reg_tabla.TABLE_NAME) || ''');');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''% Purgado de particiones obsoletas finalizado.'',to_char(clock_timestamp(), ''YYYYMMDD HH24:MI:SS'');');
      end if;
      /* (20250320). Angel Ruiz . -f */
    end if;
    if (v_tipo_particionado = 'W') then
      /* Se trata de particionamiento semanal */
      --UTL_FILE.put_line(fich_salida_pkg, '    set v_fch_particion := date_format(adddate(str_to_date(fch_datos_in, ''%Y%m%d''), 1), ''%Y%u'');');
      --UTL_FILE.put_line(fich_salida_pkg, '    v_fch_particion := to_char(to_date(fch_datos_in, ''YYYYMMDD'') - INTERVAL  ''1 day'', ''IYYYIW'');');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El valor de v_fch_particion es: %'', v_fch_particion;');
      UTL_FILE.put_line(fich_salida_pkg, '    v_fch_datos = to_char(to_date(fch_datos_in, ''%Y%m%d''), ''IYYYIW'');');
      UTL_FILE.put_line(fich_salida_pkg, '');
      /* (20250320). Angel Ruiz . -i */
      /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
      /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
      /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
      if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
      else
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
      end if;
      /* (20250320). Angel Ruiz . -f */
    end if;
    --UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_TC || '.inserta_monitoreo (''' || nombre_fich_exchange || ''', 2, 0, num_reg, 0, 0, 0, 0, fch_datos_in, inicio_paso_tmr);');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El timestamp del fin del paso es: %'', clock_timestamp();');
    UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_carga || ''', 2, 0, num_reg, 0, 0, 0, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');    
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El segundo paso ha terminado correctamente.'';');
    UTL_FILE.put_line(fich_salida_pkg, '    --commit;');
    --UTL_FILE.put_line(fich_salida_pkg, '  end if;');
    UTL_FILE.put_line(fich_salida_pkg, '  elsif (siguiente_paso_a_ejecutar = 2) then');

    UTL_FILE.put_line(fich_salida_pkg, '    /* Comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '    inicio_paso_tmr := clock_timestamp();');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Comienza el segundo paso'';');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El timestamp del comienzo del paso es: %'', inicio_paso_tmr;');    
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Inicio del proceso de EXCHANGE: ' || OWNER_TC || '.prc_' || nombre_proceso || ''';');
    /* (20250320). Angel Ruiz . -i */
    /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
    /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
    /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
    if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg, '    SELECT COUNT(*) INTO num_reg FROM ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T;');
    else
        UTL_FILE.put_line(fich_salida_pkg, '    SELECT COUNT(*) INTO num_reg FROM ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T;');
    end if;
    /* (20250320). Angel Ruiz . -f */    
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El numero de registros final que se van a intercambiar es: %'', num_reg;');        
    /* (20200409) Angel Ruiz. NF: Tengo en cuenta el particionado */
    if (v_tipo_particionado = 'D') then
      /* Se trata de particionamiento diario */
      UTL_FILE.put_line(fich_salida_pkg, '    v_fch_particion := to_char(to_date(fch_datos_in, ''YYYYMMDD'') + INTERVAL ''1 day'', ''YYYYMMDD'');');
      UTL_FILE.put_line(fich_salida_pkg, '');
      /* (20250320). Angel Ruiz . -i */
      /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
      /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
      /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
      if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' drop constraint ' ||  reg_tabla.TABLE_NAME || '_'' || fch_datos_in || ''_'' || v_fch_particion;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del drop constraint'';');
      else
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ''' || ''_'' || fch_datos_in || '' drop constraint ' ||  reg_tabla.TABLE_NAME || '_'' || fch_datos_in || ''_'' || v_fch_particion;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del drop constraint'';');
      end if;
    end if;
    if (v_tipo_particionado = 'W') then
      /* Se trata de particionamiento semanal */
      --UTL_FILE.put_line(fich_salida_pkg, '    set v_fch_particion := date_format(adddate(str_to_date(fch_datos_in, ''%Y%m%d''), 1), ''%Y%u'');');
      UTL_FILE.put_line(fich_salida_pkg, '    v_fch_particion := to_char(to_date(fch_datos_in, ''YYYYMMDD'') - INTERVAL  ''1 day'', ''IYYYIW'');');
      UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El valor de v_fch_particion es: %'', v_fch_particion;');
      UTL_FILE.put_line(fich_salida_pkg, '    v_fch_datos = to_char(to_date(fch_datos_in, ''%Y%m%d''), ''IYYYIW'');');
      UTL_FILE.put_line(fich_salida_pkg, '');
      /* (20250320). Angel Ruiz . -i */
      /* Veo que hay tablas DE HECHOS que están también en el esquema de transformación */
      /* por lo que tengo que detectar cuando comienzan por TRN_ y cuando no */
      /* Si comienzan por TRN_ son del esquema de transformacion (raw) sino son del anl */
      if (substr(nombre_tabla_reducido, 1, 4) = 'TRN_') then
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
      else
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table if exists ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || '_T rename to ' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del rename'';');
        UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE ''alter table ' || OWNER_RD || '.' || reg_tabla.TABLE_NAME || ' attach partition ' || OWNER_TC || '.' || reg_tabla.TABLE_NAME || '_'' || fch_datos_in || '' for values from ('' || fch_datos_in || '') to ('' || v_fch_particion || '')'';');
        UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''Después del attach'';');
      end if;
    end if;
    --UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_TC || '.inserta_monitoreo (''' || nombre_fich_exchange || ''', 2, 0, num_reg, 0, 0, 0, 0, fch_datos_in, inicio_paso_tmr);');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El timestamp del fin del paso es: %'', clock_timestamp();');
    UTL_FILE.put_line(fich_salida_pkg, '    CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_carga || ''', 2, 0, num_reg, 0, 0, 0, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El segundo paso ha terminado correctamente.'';');
    UTL_FILE.put_line(fich_salida_pkg, '    --commit;');

    UTL_FILE.put_line(fich_salida_pkg, '  else');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''El procedimiento ' || nombre_fich_carga || ' ya se ha ejecutado previamente de manera exitosa.'';');
    UTL_FILE.put_line(fich_salida_pkg, '    RAISE NOTICE ''No se ha vuelto a ejecutar.'';');
    UTL_FILE.put_line(fich_salida_pkg, '  end if;');
    UTL_FILE.put_line(fich_salida_pkg, 'EXCEPTION');
    UTL_FILE.put_line(fich_salida_pkg, '    WHEN OTHERS THEN');
    UTL_FILE.put_line(fich_salida_pkg, '        GET STACKED DIAGNOSTICS errno := RETURNED_SQLSTATE, msg := MESSAGE_TEXT;');
    UTL_FILE.put_line(fich_salida_pkg, '        RAISE NOTICE ''Error al realizar la transformación en el procedimiento: %.'', ' || OWNER_TC || '.neh_' || nombre_proceso || ';');
    UTL_FILE.put_line(fich_salida_pkg, '        RAISE NOTICE ''Error code: %. Mensaje: %'', errno, msg;');
    --UTL_FILE.put_line(fich_salida_pkg, '        CALL ' || OWNER_TC || '.inserta_monitoreo (''' || nombre_fich_carga || ''', siguiente_paso_a_ejecutar, 1, 0, 0, 0, 0, 0, fch_datos_in, inicio_paso_tmr);');
    UTL_FILE.put_line(fich_salida_pkg, '        CALL ' || OWNER_DM || '.prc_comun_inserta_monitoreo (''' || nombre_fich_carga || ''', siguiente_paso_a_ejecutar, 1, 0, 0, 0, 0, 0, fch_carga_in, fch_datos_in, inicio_paso_tmr);');    
    UTL_FILE.put_line(fich_salida_pkg, '        RAISE; -- Reraises the caught exception');
    UTL_FILE.put_line(fich_salida_pkg, 'END;');
    UTL_FILE.put_line(fich_salida_pkg, '$$;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/
    
    UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_load, '#############################################################################');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Millicom. Tigo.                                                             #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Archivo    :       load_he_' ||  reg_tabla.TABLE_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                               #');
    UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta los procesos de                            #');
    UTL_FILE.put_line(fich_salida_load, '#              TRANSFORMACIÓN y CARGA para ' || NAME_DM_FULL || '.          #');
    UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_load, '#              - 3 parámetros: <fch_carga> <fch_datos> <forzado>            #');
    UTL_FILE.put_line(fich_salida_load, '#              - 2 parámetros: <fch_carga> <forzado>                        #');
    UTL_FILE.put_line(fich_salida_load, '#                (en este caso fch_datos = fch_carga)                       #');
    UTL_FILE.put_line(fich_salida_load, '# Ejecucion  :                                                              #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Historia : 07-03-2025 -> Creacion                                         #');
    UTL_FILE.put_line(fich_salida_load, '#            27-Marzo-2025  -> Ajuste para permitir 2 o 3 parámetros        #');
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
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinFallido()');
    UTL_FILE.put_line(fich_salida_load, '{');
    UTL_FILE.put_line(fich_salida_load, '    insert_record_monitoreo ' || 'load_he_' || nombre_proceso || '.sh 1 1 0 0 0 0 0 "${FCH_DATOS}" "${FCH_CARGA}" "${INICIO_PASO_TMR}"' || ' >> "${' || NAME_DM || '_TRAZAS}"/load_he_' || nombre_proceso || '_"${FECHA_HORA}"' || '.log 2>' || '&' || '1');
    UTL_FILE.put_line(fich_salida_load, '    rc=$?');
    UTL_FILE.put_line(fich_salida_load, '    if [ $rc -ne 0 ]');
    UTL_FILE.put_line(fich_salida_load, '    then');
    UTL_FILE.put_line(fich_salida_load, '        SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
    UTL_FILE.put_line(fich_salida_load, '        echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_load, '        ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '        exit 1;');
    UTL_FILE.put_line(fich_salida_load, '    fi');
    UTL_FILE.put_line(fich_salida_load, '    return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK()');
    UTL_FILE.put_line(fich_salida_load, '{');
    UTL_FILE.put_line(fich_salida_load, '    insert_record_monitoreo ' || 'load_he_' || nombre_proceso || '.sh 1 0 "${TOT_INSERTADOS}" 0 0 "${TOT_LEIDOS}" "${TOT_RECHAZADOS}" "${FCH_DATOS}" "${FCH_CARGA}" "${INICIO_PASO_TMR}"' || ' >> "${' || NAME_DM || '_TRAZAS}"/load_he_' || nombre_proceso || '_"${FECHA_HORA}"' || '.log 2>&' || '1');
    UTL_FILE.put_line(fich_salida_load, '    rc=$?');
    UTL_FILE.put_line(fich_salida_load, '    if [ $rc -ne 0 ]');
    UTL_FILE.put_line(fich_salida_load, '    then');
    UTL_FILE.put_line(fich_salida_load, '        SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
    UTL_FILE.put_line(fich_salida_load, '        echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_load, '        ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '        exit 1;');
    UTL_FILE.put_line(fich_salida_load, '    fi');
    UTL_FILE.put_line(fich_salida_load, '    return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# shellcheck disable=SC1091');
    UTL_FILE.put_line(fich_salida_load, '. "${HOME_PRODUCCION}"/' || NAME_DM || '/COMUN/Shell/Entorno/Entorno' || NAME_DM || '_' || PAIS || '.sh');
    UTL_FILE.put_line(fich_salida_load, '# Comprobamos si el numero de parametros es el correcto');
    UTL_FILE.put_line(fich_salida_load, 'if [ $# -ne 2 ] && [ $# -ne 3 ]; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="Número de parámetros de entrada incorrecto. Uso: ${0} <fch_carga> [<fch_datos>] <forzado>"');
    UTL_FILE.put_line(fich_salida_load, '  echo "${SUBJECT}"');        
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '# Recogida de parametros');
    UTL_FILE.put_line(fich_salida_load, 'if [ $# -eq 3 ]; then');
    UTL_FILE.put_line(fich_salida_load, '  FCH_CARGA=${1}');
    UTL_FILE.put_line(fich_salida_load, '  FCH_DATOS=${2}');
    UTL_FILE.put_line(fich_salida_load, '  BAN_FORZADO=${3}');
    UTL_FILE.put_line(fich_salida_load, 'else');
    UTL_FILE.put_line(fich_salida_load, '  FCH_CARGA=${1}');
    UTL_FILE.put_line(fich_salida_load, '  FCH_DATOS=${1}');
    UTL_FILE.put_line(fich_salida_load, '  BAN_FORZADO=${2}');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    --UTL_FILE.put_line(fich_salida_load, '# Comprobamos si el numero de parametros es el correcto');
    --UTL_FILE.put_line(fich_salida_load, 'if [ $# -ne 3 ] ; then');
    --UTL_FILE.put_line(fich_salida_load, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
    --UTL_FILE.put_line(fich_salida_load, '  echo "${SUBJECT}"');        
    --UTL_FILE.put_line(fich_salida_load, '  exit 1');
    --UTL_FILE.put_line(fich_salida_load, 'fi');
    --UTL_FILE.put_line(fich_salida_load, '# Recogida de parametros');
    --UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA=${1}');
    --UTL_FILE.put_line(fich_salida_load, 'FCH_DATOS=${2}');
    --UTL_FILE.put_line(fich_salida_load, 'BAN_FORZADO=${3}');
    UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_$(date +%Y%m%d_%H%M%S)');      
    UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_load, 'if [ ! -d "${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}" ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  mkdir "${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}"');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > "${' || NAME_DM || '_TRAZAS}"/load_he_' || nombre_proceso || '_"${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_load, '# shellcheck disable=SC2129');
    UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: $(date +%d/%m/%Y\ %H:%M:%S)"  >> "${' || NAME_DM || '_TRAZAS}"/load_he_' || nombre_proceso || '_"${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_load, '# shellcheck disable=SC2129');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> "${' || NAME_DM || '_TRAZAS}/load_he_' || nombre_proceso || '_${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_load, '# shellcheck disable=SC2129');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> "${' || NAME_DM || '_TRAZAS}/load_he_' || nombre_proceso || '_${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> "${' || NAME_DM || '_TRAZAS}/load_he_' || nombre_proceso || '_${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=' || 'load_ne_' || nombre_proceso || '.sh');
    UTL_FILE.put_line(fich_salida_load, '');
    --UTL_FILE.put_line(fich_salida_load, '################################################################################');
    --UTL_FILE.put_line(fich_salida_load, '# Cuentas  Produccion / Desarrollo                                             #');
    --UTL_FILE.put_line(fich_salida_load, '################################################################################');
    --UTL_FILE.put_line(fich_salida_load, 'if [ "$(/sbin/ifconfig -a | grep ''192.168.2.'' | awk ''{print $2}'')" = "192.168.2.109" ]||[ "$(/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}'')" = "10.225.173.184" ]; then');
    --UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
    --UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
    --UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_Usuario_ReportesBI.txt)');
    --UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
    --UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_ReportesBI.txt)');
    --UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TelefonosMantto.txt)');
    --UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
    --UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TELEFONOS_USUARIOS.txt)');
    --UTL_FILE.put_line(fich_salida_load, 'else');
    --UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
    --UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
    --UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_Usuario_ReportesBI.txt)');
    --UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
    --UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_ReportesBI.txt)');
    --UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TelefonosMantto.txt)');
    --UTL_FILE.put_line(fich_salida_load, '  # shellcheck disable=SC2034');
    --UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TELEFONOS_USUARIOS.txt)');
    --UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=$(');
    UTL_FILE.put_line(fich_salida_load, 'psql -h "$HOST" -p "$PORT" -U "$BD_USUARIO" -d "$DB_NAME" -t --no-align -c "');
    UTL_FILE.put_line(fich_salida_load, 'SELECT to_char(clock_timestamp(), ''YYYYMMDDHH24MISS'')"');
    UTL_FILE.put_line(fich_salida_load, ')');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '# Llamada a PostgreSQL');
    UTL_FILE.put_line(fich_salida_load, 'psql -h "$HOST" -p "$PORT" -U "$BD_USUARIO" -d "$DB_NAME" -c "call ' || OWNER_TC || '.prc_' || nombre_proceso || ' (''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');" >> "${' || NAME_DM || '_TRAZAS}/load_he_' || nombre_proceso || '_${FECHA_HORA}"' || '.log ' ||  '2>&' || '1');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el postgres en la llamada a prc_' || nombre_proceso || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '  echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_he' || '_' || nombre_proceso || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_load, '# shellcheck disable=SC2005');
    UTL_FILE.put_line(fich_salida_load, '  echo "$(date)" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_he' || '_' || nombre_proceso || '_${FECHA_HORA}.log"');
    UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, 'echo "El proceso ' || nombre_fich_carga || ' se ha realizado correctamente." >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_he_' || nombre_proceso || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_load, 'exit 0');
    UTL_FILE.put_line(fich_salida_load, '');
    /* (20170817) Angel Ruiz.Fin */
    /******/
    /* FIN DE LA GENERACION DEL sh de CARGA */
    /******/
    
    /*************************/
    /******/
    /* INICIO DE LA GENERACION DEL sh de EXCHANGE */
    /******/
    /******/
    /* EN ESTA VERSIÓN NO HAY EXCHANGE YA QUE TODO SE HACE EN EL lh_*/
     /******/
    /******/
    /* FIN DE LA GENERACION DEL sh de EXCHANGE */
    /******/
    
    /*************************/
    UTL_FILE.FCLOSE (fich_salida_load);
    --UTL_FILE.FCLOSE (fich_salida_exchange);
    UTL_FILE.FCLOSE (fich_salida_pkg);
  end loop;
  close MTDT_TABLA;
end;

