DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      TRIM(SOURCE) "SOURCE",
      TRIM(INTERFACE_NAME) "INTERFACE_NAME",
      TRIM(COUNTRY) "COUNTRY",
      TRIM(TYPE) "TYPE",
      TRIM(SEPARATOR) "SEPARATOR",
      TRIM(LENGTH) "LENGTH",
      TRIM(FREQUENCY) "FREQUENCY",
      TRIM(DELAYED) "DELAYED",
      TRIM(HISTORY) "HISTORY",
      MARCA,
      HUSO,
      trim(TYPE_VALIDATION) "TYPE_VALIDATION",  
      trim(FILE_VALIDATION) "FILE_VALIDATION"   /* (20160818) Angel Ruiz. NF. Validar en otro directorio */
    FROM MTDT_INTERFACE_SUMMARY    
    WHERE SOURCE <> 'SA' -- Este origen es el que se ha considerado para las dimensiones que son de integracion ya que se cargan a partir de otras dimensiones de SA 
    and CONCEPT_NAME in ('ACCOUNT', 'ACCOUNT_FIX'
    , 'SUBSCRIBER', 'SUBSCRIBER_FIX', 'CSTMR', 'CSTMR_FIX', 'AR_CTC_PRFL', 'AR_CTC_PRFL_FIX'
    , 'CNL_FIX', 'CSTMR_CLSS', 'CSTMR_CLSS_FIX', 'GEO_AREA', 'GEO_AREA_FIX', 'PD', 'PD_FIX', 'PYMT_ENT'
    , 'PYMT_ENT_FIX', 'RTLR', 'RTLR_FIX'
    );
    --and trim(CONCEPT_NAME) in ('CUENTA', 'PARQUE_ABO_PRE');
    --and TRIM(CONCEPT_NAME) in ('RECARGAS_MVNO', 'CANAL', 'CADENA', 'SUBTIPO_CANAL', 'MEDIO_RECARGA', 'ERROR_RECARGA');
    --AND DELAYED = 'S';
    --WHERE CONCEPT_NAME NOT IN ( 'EMPRESA', 'ESTADO_CEL', 'FINALIZACION_LLAMADA', 'POSICION_TRAZO_LLAMADA', 'TRONCAL', 'TIPO_REGISTRO', 'MSC');
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      lower(TRIM(CONCEPT_NAME)) "CONCEPT_NAME",
      TRIM(SOURCE) "SOURCE",
      lower(TRIM(COLUMNA)) "COLUMNA",
      TRIM(KEY) "KEY",
      TRIM(TYPE) "TYPE",
      TRIM(LENGTH) "LENGTH",
      TRIM(NULABLE) "NULABLE",
      POSITION,
      TRIM(FORMAT) "FORMAT"
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      TRIM(CONCEPT_NAME) = concep_name_in and
      TRIM(SOURCE) = source_in
    ORDER BY POSITION;

      reg_summary dtd_interfaz_summary%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col PLS_INTEGER;
      num_column PLS_INTEGER;
      v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;
      
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
      lista_pos                                    list_posiciones := list_posiciones (); 
      
      fich_salida                                 UTL_FILE.file_type;
      fich_salida_sh                          UTL_FILE.file_type;
      nombre_fich                              VARCHAR(40);
      nombre_fich_sh                        VARCHAR(40);  
      tipo_col                                      VARCHAR(1000);
      nombre_interface_a_cargar   VARCHAR2(150);
      nombre_flag_a_cargar            VARCHAR2(150);
      nombre_fich_descartados     VARCHAR2(150);
      pos_ini_pais                             PLS_integer;
      pos_fin_pais                             PLS_integer;
      pos_ini_fecha                           PLS_integer;
      pos_fin_fecha                           PLS_integer;
      pos_ini_hora                              PLS_integer;
      pos_fin_hora                              PLS_integer;
      OWNER_SA                             VARCHAR2(60);
      OWNER_T                                VARCHAR2(60);
      OWNER_DM                            VARCHAR2(60);
      OWNER_MTDT                       VARCHAR2(60);
      NAME_DM                                VARCHAR(60);
      PAIS                              VARCHAR2(60);
      TABLESPACE_SA               VARCHAR2(60);
      TABLESPACE_SA_IDX        VARCHAR2(60);
      nombre_proceso                      VARCHAR(30);
      parte_entera                              VARCHAR2(60);
      parte_decimal                           VARCHAR2(60);
      long_parte_entera                    PLS_integer;
      long_parte_decimal                  PLS_integer;
      mascara                                     VARCHAR2(250);
      nombre_fich_cargado               VARCHAR2(1) := 'N';
      v_ulti_pos                        PLS_integer;
      

  function procesa_campo_formateo (cadena_in in varchar2, nombre_campo_in in varchar2) return varchar2
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
    dbms_output.put_line ('Entro en procesa_campo_formateo');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco el nombre del campo = */
      sustituto := ':' || nombre_campo_in;
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_campo_formateo. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, nombre_campo_in, pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (nombre_campo_in));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + (length (':' || nombre_campo_in));
        dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
        pos := pos_ant;
      end loop;
    end if;
    return cadena_resul;
  end;

  
BEGIN
  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO TABLESPACE_SA_IDX FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA_IDX';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO v_REQ_NUMER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';
  SELECT VALOR INTO PAIS FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PAIS_DM';
  /* (20141219) FIN*/

  OPEN dtd_interfaz_summary;
  LOOP
    FETCH dtd_interfaz_summary
    INTO reg_summary;
    EXIT WHEN dtd_interfaz_summary%NOTFOUND; 
    nombre_fich := 'stg' || '_' || reg_summary.CONCEPT_NAME || '.load';
    nombre_fich_sh := 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '.sh';
    fich_salida := UTL_FILE.FOPEN ('SALIDA',nombre_fich,'W');
    fich_salida_sh := UTL_FILE.FOPEN ('SALIDA',nombre_fich_sh,'W');
    /* Angel Ruiz (20141223) Hecho porque hay paquetes que no compilan */
    if (length(reg_summary.CONCEPT_NAME) < 24) then
      nombre_proceso := 'stg_' || reg_summary.CONCEPT_NAME;
    else
      nombre_proceso := reg_summary.CONCEPT_NAME;
    end if;
    UTL_FILE.put_line(fich_salida, 'LOAD CSV');
    UTL_FILE.put_line(fich_salida, '    FROM ''__RUTA_AL_FICHERO_CSV__'' WITH ENCODING ISO-8859-1');
    UTL_FILE.put_line(fich_salida, '        HAVING FIELDS');
    UTL_FILE.put_line(fich_salida, '        (');
    /* Escribimos los campos del fichero plano -i */
    IF reg_summary.TYPE = 'S'             /*  El fichero posee un separador de campos */
    THEN
      primera_col := 1;
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      loop
        FETCH dtd_interfaz_detail
          INTO reg_datail;
            EXIT WHEN dtd_interfaz_detail%NOTFOUND;
            if primera_col = 1 then
              if (reg_datail.COLUMNA != 'file_name') THEN
                /* El campo FILE_NAME no viene en el fichero plano, sino que se ha de añadir en el loader */
                /* por lo tanto este campo no tengo que añadirlo en esta sección que hace referencia a los campos del fichero plano */
                UTL_FILE.put_line (fich_salida, '            ' || reg_datail.COLUMNA || ' [trim both whitespace]');
              end if; 
              primera_col := 0;
            else
              if (reg_datail.COLUMNA != 'file_name') THEN
                /* El campo FILE_NAME no viene en el fichero plano, sino que se ha de añadir en el loader */
                /* por lo tanto este campo no tengo que añadirlo en esta sección que hace referencia a los campos del fichero plano */
                UTL_FILE.put_line (fich_salida, '            ,' || reg_datail.COLUMNA || ' [trim both whitespace]');
              end if;
            end if;
      end loop;
      CLOSE dtd_interfaz_detail;
      UTL_FILE.put_line(fich_salida, '        )');
      /* Escribimos los campos del fichero plano -f */
      UTL_FILE.put_line(fich_salida, '    INTO __URL_DESTINO__');
      UTL_FILE.put_line(fich_salida, '    TARGET TABLE ' || OWNER_SA || '.sth_' || lower(reg_summary.CONCEPT_NAME) || '_' || '__FCH_DATOS__');
      UTL_FILE.put_line(fich_salida, '    TARGET COLUMNS');
      UTL_FILE.put_line(fich_salida, '    (');
      primera_col := 1;
      nombre_fich_cargado := 'N';
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      loop
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            if (reg_datail.COLUMNA = 'file_name') then
              /* El campo FILE_NAME no viene en el fichero plano, sino que se ha de añadir en el loader */
              tipo_col := ' text using "__INTERFACE_NAME__"';
              nombre_fich_cargado := 'Y';
            else
              --tipo_col := ' integer using (empty-string-to-null ' || reg_datail.COLUMNA || ')';
              tipo_col := ' text using (empty-string-to-null ' || reg_datail.COLUMNA || ')';
            end if;
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := ' float using (empty-string-to-null ' || reg_datail.COLUMNA || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := ' float using (empty-string-to-null ' || reg_datail.COLUMNA || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            if (reg_datail.LENGTH = 14) then
              tipo_col := ' timestamp using (date-with-no-separator ' || reg_datail.COLUMNA || ')';     
            else
              tipo_col := ' timestamp using (date-with-no-separator ' || reg_datail.COLUMNA || ')';
            end if;
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := ' float using (empty-string-to-null ' || reg_datail.COLUMNA || ')';
            dbms_output.put_line('Tipo de columna: ' || tipo_col);
            --tipo_col :='';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := ' time using (time-with-no-separator ' || reg_datail.COLUMNA || ')';
        END CASE;

        if primera_col = 1 then
          UTL_FILE.put_line (fich_salida, '        ' || reg_datail.COLUMNA || tipo_col);
          primera_col := 0;
        else
          UTL_FILE.put_line (fich_salida, '        , ' || reg_datail.COLUMNA || tipo_col);
        end if;
      end loop;
      CLOSE dtd_interfaz_detail;
      /* Anyado el campo CVE_DATE que tampoco viene en el fichero plano.*/
      /* Lo añado al final del todo porque es un campo que tampoco viene especificado */
      /* como campo en la excel */
      UTL_FILE.put_line(fich_salida, '        , cve_dia bigint using "__FCH_DATOS__"');
      UTL_FILE.put_line(fich_salida, '    )');
      UTL_FILE.put_line(fich_salida, '    WITH truncate');
      UTL_FILE.put_line(fich_salida, '    , fields terminated by ''|''');
      UTL_FILE.put_line(fich_salida, '    , fields optionally enclosed by ''"''');
      UTL_FILE.put_line(fich_salida, '    , fields escaped by backslash-quote');
      UTL_FILE.put_line(fich_salida, '    SET');
      /* 2024-09-16 (Angel Ruiz) Anyado el BEFORE and AFTER loading para postgres -i */
      if (reg_summary.HISTORY IS NULL) then      
      UTL_FILE.put_line(fich_salida, '        work_mem to ''32 MB'', maintenance_work_mem to ''64 MB'';');
      else
        UTL_FILE.put_line(fich_salida, '        work_mem to ''32 MB'', maintenance_work_mem to ''64 MB''');
        UTL_FILE.put_line(fich_salida, '    BEFORE LOAD DO');
        UTL_FILE.put_line(fich_salida, '        $$ drop table if exists ' || OWNER_SA || '.sth_' || lower(reg_summary.CONCEPT_NAME) || '_' || '__FCH_DATOS__'   || '; $$,');
        UTL_FILE.put_line(fich_salida, '        $$ create table ' || OWNER_SA || '.sth_' || lower(reg_summary.CONCEPT_NAME) || '_' || '__FCH_DATOS__'   || ' (');
        UTL_FILE.put_line(fich_salida, '            like ' || OWNER_SA || '.sth_' || lower(reg_summary.CONCEPT_NAME) || ' including defaults including constraints) TABLESPACE ' || TABLESPACE_SA || '; $$');
        UTL_FILE.put_line(fich_salida, '');
        UTL_FILE.put_line(fich_salida, '    AFTER LOAD DO');
        UTL_FILE.put_line(fich_salida, '        $$ alter table ' || OWNER_SA || '.sth_' || lower(reg_summary.CONCEPT_NAME) || ' attach partition ' || OWNER_SA || '.sth_' || lower(reg_summary.CONCEPT_NAME) || '_' || '__FCH_DATOS__');
        UTL_FILE.put_line(fich_salida, '          for values from (__FCH_DATOS__) to (__FCH_DATOS_MAS_UNO__);');
        UTL_FILE.put_line(fich_salida, '        $$;');
        UTL_FILE.put_line(fich_salida, '');
      end if;
      /* 2024-09-16 (Angel Ruiz) Anyado el BEFORE and AFTER loading para postgres -f */
    END IF;
    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/

    /* (20160818) Angel Ruiz. NF: Puede existir una ruta alternativa para cargar fichero*/
    /* ya que por motivos de validacion se quiere cargar otro fichero */
    if (reg_summary.FILE_VALIDATION is null) then
    /* Se lleva a cabo la carga del fichero normal */
      nombre_interface_a_cargar := reg_summary.INTERFACE_NAME;
      pos_ini_pais := instr(reg_summary.INTERFACE_NAME, '_XXX_');
      if (pos_ini_pais > 0) then
        pos_fin_pais := pos_ini_pais + length ('_XXX_');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_pais -1) || '_' || reg_summary.COUNTRY || '_' || substr(nombre_interface_a_cargar, pos_fin_pais);
      end if;
      pos_ini_fecha := instr(reg_summary.INTERFACE_NAME, '_YYYYMMDD');
      if (pos_ini_fecha > 0) then
        pos_fin_fecha := pos_ini_fecha + length ('_YYYYMMDD');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '_${FCH_DATOS}' || substr(nombre_interface_a_cargar, pos_fin_fecha);
      end if;
      /* (20160225) Angel Ruiz */
      pos_ini_hora := instr(nombre_interface_a_cargar, 'HH24MISS');
      if (pos_ini_hora > 0) then
        pos_fin_hora := pos_ini_hora + length ('HH24MISS');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_hora -1) || '*' || substr(nombre_interface_a_cargar, pos_fin_hora);
      end if;
      pos_ini_fecha := instr(reg_summary.INTERFACE_NAME, 'DDMMAA');
      if (pos_ini_fecha > 0) then
        pos_fin_fecha := pos_ini_fecha + length ('DDMMAA');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '${FCH_DATOS_DOS_DIGITOS}' || substr(nombre_interface_a_cargar, pos_fin_fecha);
      end if;
      /*****************************/
      nombre_flag_a_cargar := substr (nombre_interface_a_cargar, 1, instr(nombre_interface_a_cargar, '.')) || 'flag';
      nombre_fich_descartados := substr (nombre_interface_a_cargar, 1, instr(nombre_interface_a_cargar, '.')) || 'bad';
    else
    /* Se lleva a cabo la carga del fichero alternativo */
      nombre_interface_a_cargar := reg_summary.FILE_VALIDATION;
      pos_ini_pais := instr(reg_summary.FILE_VALIDATION, '_XXX_');
      if (pos_ini_pais > 0) then
        pos_fin_pais := pos_ini_pais + length ('_XXX_');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_pais -1) || '_' || reg_summary.COUNTRY || '_' || substr(nombre_interface_a_cargar, pos_fin_pais);
      end if;
      pos_ini_fecha := instr(reg_summary.FILE_VALIDATION, '_YYYYMMDD');
      if (pos_ini_fecha > 0) then
        pos_fin_fecha := pos_ini_fecha + length ('_YYYYMMDD');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '_${FCH_DATOS}' || substr(nombre_interface_a_cargar, pos_fin_fecha);
      end if;
      /* (20160225) Angel Ruiz */
      pos_ini_hora := instr(nombre_interface_a_cargar, 'HH24MISS');
      if (pos_ini_hora > 0) then
        pos_fin_hora := pos_ini_hora + length ('HH24MISS');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_hora -1) || '*' || substr(nombre_interface_a_cargar, pos_fin_hora);
      end if;
      /*****************************/
      nombre_flag_a_cargar := substr (nombre_interface_a_cargar, 1, instr(nombre_interface_a_cargar, '.')) || 'flag';
      nombre_fich_descartados := REGEXP_SUBSTR(substr (nombre_interface_a_cargar, 1, instr(nombre_interface_a_cargar, '.')) || 'bad', '[A-Za-z_0-9$}{]+\.bad$');
    end if;
    UTL_FILE.put_line(fich_salida_sh, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_sh, '#############################################################################');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Millicom                                                                  #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Archivo    :       load_stg_' ||  reg_summary.CONCEPT_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Autor      : SYNAPSYS                                                     #');
    UTL_FILE.put_line(fich_salida_sh, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || '.        #');
    UTL_FILE.put_line(fich_salida_sh, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Ejecucion  :                                                              #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Historia : 09-Septiembre-2024 -> Creacion                                 #');
    UTL_FILE.put_line(fich_salida_sh, '# Caja de Control - M :                                                     #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
    UTL_FILE.put_line(fich_salida_sh, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Caducidad del Requerimiento :                                             #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Dependencias :                                                            #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Usuario:                                                                  #');   
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Telefono:                                                                 #');   
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '#############################################################################');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '#Obtiene los password de base de datos                                         #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinFallido()');
    UTL_FILE.put_line(fich_salida_sh, '{');
    UTL_FILE.put_line(fich_salida_sh, '    insert_record_monitoreo ' || 'load_stg_' || reg_summary.CONCEPT_NAME || '.sh 1 1 0 0 0 0 0 "${FCH_CARGA}" "${FCH_DATOS}" "${INICIO_PASO_TMR}"' || ' >> "${' || NAME_DM || '_TRAZAS}"/load_stg_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}"' || '.log 2>' || '&' || '1');
    UTL_FILE.put_line(fich_salida_sh, '    rc=$?');
    UTL_FILE.put_line(fich_salida_sh, '    if [ $rc -ne 0 ]');
    UTL_FILE.put_line(fich_salida_sh, '    then');
    UTL_FILE.put_line(fich_salida_sh, '        SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
    UTL_FILE.put_line(fich_salida_sh, '        echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_sh, '        ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '        exit 1;');
    UTL_FILE.put_line(fich_salida_sh, '    fi');
    UTL_FILE.put_line(fich_salida_sh, '    return 0');
    UTL_FILE.put_line(fich_salida_sh, '}');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinOK()');
    UTL_FILE.put_line(fich_salida_sh, '{');
    UTL_FILE.put_line(fich_salida_sh, '    insert_record_monitoreo ' || 'load_stg_' || reg_summary.CONCEPT_NAME || '.sh 1 0 "${TOT_INSERTADOS}" 0 0 "${TOT_LEIDOS}" "${TOT_RECHAZADOS}" "${FCH_CARGA}" "${FCH_DATOS}" "${INICIO_PASO_TMR}"' || ' >> "${' || NAME_DM || '_TRAZAS}"/load_stg_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}"' || '.log 2>&' || '1');
    UTL_FILE.put_line(fich_salida_sh, '    rc=$?');
    UTL_FILE.put_line(fich_salida_sh, '    if [ $rc -ne 0 ]');
    UTL_FILE.put_line(fich_salida_sh, '    then');
    UTL_FILE.put_line(fich_salida_sh, '        SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
    UTL_FILE.put_line(fich_salida_sh, '        echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_sh, '        ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '        exit 1;');
    UTL_FILE.put_line(fich_salida_sh, '    fi');
    UTL_FILE.put_line(fich_salida_sh, '    return 0');
    UTL_FILE.put_line(fich_salida_sh, '}');

    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# shellcheck disable=SC1091');
    UTL_FILE.put_line(fich_salida_sh, '. "${HOME_PRODUCCION}"/' || NAME_DM || '/COMUN/Shell/Entorno/Entorno' || NAME_DM || '_' || PAIS || '.sh');
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos si el numero de parametros es el correcto');
    UTL_FILE.put_line(fich_salida_sh, 'if [ $# -ne 3 ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
    UTL_FILE.put_line(fich_salida_sh, '  echo "${SUBJECT}"');        
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '# Recogida de parametros');
    UTL_FILE.put_line(fich_salida_sh, 'FCH_CARGA=${1}');
    UTL_FILE.put_line(fich_salida_sh, 'FCH_DATOS=${2}');
    --UTL_FILE.put_line(fich_salida_sh, 'FCH_DATOS_DOS_DIGITOS=`echo ${FCH_DATOS} | awk ''{ printf "%s%s%s", substr($1,3,2), substr($1,5,2), substr($1,7,2) ; }''`');
    --UTL_FILE.put_line(fich_salida_sh, 'FCH_DATOS_DOS_DIGITOS=`echo ${FCH_DATOS} | awk ''{ printf "%s%s%s", substr($1,7,2), substr($1,5,2), substr($1,3,2) ; }''`');
    UTL_FILE.put_line(fich_salida_sh, 'BAN_FORZADO=${3}');    
    UTL_FILE.put_line(fich_salida_sh, 'FECHA_HORA=${FCH_DATOS}_$(date +%Y%m%d_%H%M%S)');
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos si existe el directorio de Trazas para fecha de datos');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ! -d "${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}" ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  mkdir "${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}"');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, 'echo "${0}" > "${' || NAME_DM || '_TRAZAS}"/load_stg_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, '# shellcheck disable=SC2129');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Inicia Proceso: $(date +%d/%m/%Y\ %H:%M:%S)"  >> "${' || NAME_DM || '_TRAZAS}"/load_stg_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> "${' || NAME_DM || '_TRAZAS}"/load_stg_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> "${' || NAME_DM || '_TRAZAS}"/load_stg_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}"' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Forzado: ${BAN_FORZADO}"  >> "${' || NAME_DM || '_TRAZAS}"/load_stg_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}"' || '.log ');
    
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    --UTL_FILE.put_line(fich_salida_sh, 'REQ_NUM="' || v_REQ_NUMER || '"');
    --UTL_FILE.put_line(fich_salida_sh, 'REQ_NUM="Req89208"');
    UTL_FILE.put_line(fich_salida_sh, 'INTERFAZ="' || 'load_stg_' || reg_summary.CONCEPT_NAME || '.sh"');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# Cuentas  Produccion / Desarrollo                                             #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, 'if [ "$(/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}'')" = "10.225.173.102" ]||[ "$(/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}'')" = "10.225.173.184" ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_sh, '  # shellcheck disable=SC2034');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_Usuario_ReportesBI.txt)');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_ReportesBI.txt)');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_DWH=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TelefonosMantto.txt)');
    UTL_FILE.put_line(fich_salida_sh, '  # shellcheck disable=SC2034');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TELEFONOS_USUARIOS.txt)');
    UTL_FILE.put_line(fich_salida_sh, 'else');
    UTL_FILE.put_line(fich_salida_sh, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_sh, '  # shellcheck disable=SC2034');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_Usuario_ReportesBI.txt)');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL=$(cat "${' || NAME_DM || '_CONFIGURACION}"/Correos_Mtto_ReportesBI.txt)');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_DWH=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TelefonosMantto.txt)');
    UTL_FILE.put_line(fich_salida_sh, '  # shellcheck disable=SC2034');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_USUARIOS=$(cat "${' || NAME_DM || '_CONFIGURACION}"/TELEFONOS_USUARIOS.txt)');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, 'ULT_PASO_EJECUTADO=$(');
    UTL_FILE.put_line(fich_salida_sh, '  psql -h "$HOST" -p "$PORT" -U "$BD_USUARIO" -d "$DB_NAME" -t --no-align -c "');
    UTL_FILE.put_line(fich_salida_sh, '  SELECT COALESCE(MAX(' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PASO), 0)');
    UTL_FILE.put_line(fich_salida_sh, 'FROM');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO');
    UTL_FILE.put_line(fich_salida_sh, 'JOIN');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_PROCESO ON ' || OWNER_MTDT || '.MTDT_PROCESO.CVE_PROCESO = ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PROCESO');
    UTL_FILE.put_line(fich_salida_sh, 'JOIN');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_PASO ON ' || OWNER_MTDT || '.MTDT_PASO.CVE_PROCESO = ' || OWNER_MTDT || '.MTDT_PROCESO.CVE_PROCESO');
    UTL_FILE.put_line(fich_salida_sh, 'WHERE');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.FCH_CARGA = to_date(''${FCH_CARGA}'', ''YYYYMMDD'') AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.FCH_DATOS = to_date(''${FCH_DATOS}'', ''YYYYMMDD'') AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_PROCESO.NOMBRE_PROCESO = ''${0}'' AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_RESULTADO = 0;"');
    UTL_FILE.put_line(fich_salida_sh, ')');
    UTL_FILE.put_line(fich_salida_sh, 'echo "El último paso ejecutado es: ${ULT_PASO_EJECUTADO}" >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, 'if [ "${ULT_PASO_EJECUTADO}" -eq 1 ] && [ "${BAN_FORZADO}" = "N" ]');
    UTL_FILE.put_line(fich_salida_sh, 'then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Ya se ejecutaron Ok todos los pasos de este proceso."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');        
    UTL_FILE.put_line(fich_salida_sh, '  echo date >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, '  exit 0');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'INICIO_PASO_TMR=$(');
    UTL_FILE.put_line(fich_salida_sh, 'psql -h "$HOST" -p "$PORT" -U "$BD_USUARIO" -d "$DB_NAME" -t --no-align -c "');
    UTL_FILE.put_line(fich_salida_sh, 'SELECT to_char(now(), ''YYYYMMDDHH24MISS'')"');
    UTL_FILE.put_line(fich_salida_sh, ')');
    UTL_FILE.put_line(fich_salida_sh, 'echo "El proceso se inicia: ${INICIO_PASO_TMR}" >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, '');
    /* (20160818) Angel Ruiz. NF: Puede existir una ruta alternativa para cargar fichero*/
    /* ya que por motivos de validacion se quiere cargar otro fichero */
    if (reg_summary.FILE_VALIDATION is null) then
    /* Se trata de una carga normal */
    /* (20150225) ANGEL RUIZ. Aparecen HH24MISS como parte del nombre en el DM Distribucion */
    /* (20150827) ANGEL RUIZ. He comentado el IF de despues porque no funcionaba cuando el fichero viene sin HHMMSS*/
    --if (pos_ini_hora   > 0) then
      /* (20160712) Angel Ruiz. CAMBIO TEMPORAL... */
      UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_CARGA=$(ls -1 "${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar ||'")');
      --UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_CARGA=`ls -1 /DWH/requerimientos/salidasmanual/Req96817/ONIX_' || reg_summary.CONCEPT_NAME || '/datos/' || nombre_interface_a_cargar ||'`');
      /* (20160712) Angel Ruiz. FIN CAMBIO TEMPORAL... */
      --UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_FLAG=`ls -1 ${' || NAME_DM || '_FUENTE}/${FCH_DATOS}/' || nombre_flag_a_cargar ||'`');
    --end if;
    else
    /* Se trata de cargar el fichero alternativo para validacion */
      --UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_CARGA=`ls -1 ${' || NAME_DM || '_FUENTE}/${FCH_DATOS}/' || nombre_interface_a_cargar ||'`');
      --UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_CARGA=`ls -1 /DWH/requerimientos/salidasmanual/Req96817/' || reg_summary.CONCEPT_NAME || '/datos/' || nombre_interface_a_cargar ||'`');
      UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_CARGA=$(ls -1 ' || nombre_interface_a_cargar ||')'); /* nombre_interface_a_cargar ya lleva la ruta incluida en este caso */
    end if;
    /****************************/
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos que los ficheros a cargar existen');
    UTL_FILE.put_line(fich_salida_sh, 'if [ "${NOMBRE_FICH_CARGA:-SIN_VALOR}" = "SIN_VALOR" ] ; then');
    if (reg_summary.FREQUENCY = 'E') then
      UTL_FILE.put_line(fich_salida_sh, 'echo "Se trata de una carga de fichero eventual." >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');
      /* Se trata de una carga eventual, por lo que a veces el fichero puede no venir y entonces no debe acabar con error */
      if (reg_summary.FILE_VALIDATION is null) then
        UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen fichero para cargar. El fichero es de carga eventual. No hay error.' || '${' || NAME_DM || '_FUENTE}/${FCH_DATOS}/' || nombre_interface_a_cargar || '."');
      else
        UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen fichero para cargar. El fichero es de carga eventual. No hay error.' || nombre_interface_a_cargar || '."');
      end if;
      /* (20160818) Angel Ruiz. FIN NF: Puede existir una ruta alternativa para cargar fichero */      
      UTL_FILE.put_line(fich_salida_sh, '    echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo date >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinOK');
      UTL_FILE.put_line(fich_salida_sh, '    echo "El proceso ha acabado correctamente." >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');
      UTL_FILE.put_line(fich_salida_sh, '    exit 0');
    else
      /* Se trata de una carga eventual, por lo que a veces el fichero puede no venir y entonces no debe acabar con error */
      if (reg_summary.FILE_VALIDATION is null) then
        UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen ficheros para cargar. ' || '${' || NAME_DM || '_FUENTE}/${FCH_DATOS}/' || nombre_interface_a_cargar || '."');
      else
        UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen ficheros para cargar. ' || nombre_interface_a_cargar || '."');
      end if;
      /* (20160818) Angel Ruiz. FIN NF: Puede existir una ruta alternativa para cargar fichero */
      UTL_FILE.put_line(fich_salida_sh, '    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '    echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo date >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '    echo "El proceso ha acabado con error." >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');      
      UTL_FILE.put_line(fich_salida_sh, '    exit 1');
    end if;
    UTL_FILE.put_line(fich_salida_sh, 'else');
    UTL_FILE.put_line(fich_salida_sh, '  for FILE in ${NOMBRE_FICH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, '  do');
    UTL_FILE.put_line(fich_salida_sh, '    # shellcheck disable=SC2001');
    UTL_FILE.put_line(fich_salida_sh, '    NAME_FLAG=$(echo "$FILE" | sed -e ''s/\.[Dd][Aa][Tt]/\.flag/'')');
    UTL_FILE.put_line(fich_salida_sh, '    if [ ! -f "${FILE}" ] || [ ! -f "${NAME_FLAG}" ] ; then');    
    UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}: No existe fichero o su fichero de flag a cargar. ' || '${FILE}' || '."');
    UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '      echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');    
    UTL_FILE.put_line(fich_salida_sh, '      echo date >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, '      InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_sh, '      echo "El proceso ha acabado con error." >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, '      exit 1');    
    UTL_FILE.put_line(fich_salida_sh, '    fi');
    UTL_FILE.put_line(fich_salida_sh, '  done');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    /*(20160715) Angel Ruiz. Nueva Funcionalidad. Escribir el nombre del fichero cargado en una columna de la tabla de Staging */
    if (nombre_fich_cargado = 'Y') then
    /* (20150605) Angel Ruiz. AÑADIDO PARA CHEQUEAR LA CALIDAD DEL DATO */
      UTL_FILE.put_line(fich_salida_sh, '# Cargamos los ficheros');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'for FILE in ${NOMBRE_FICH_CARGA}');
      UTL_FILE.put_line(fich_salida_sh, 'do');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS=$(basename "${FILE}")');
      --UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_CTL=`basename ${FILE%.*}`.ctl');
      UTL_FILE.put_line(fich_salida_sh, '  # shellcheck disable=SC2001');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_LDR=$(echo "${NOMBRE_FICH_DATOS}" | sed -e ''s/\.[Dd][Aa][Tt]/\.load/'')');
      UTL_FILE.put_line(fich_salida_sh, '  # shellcheck disable=SC2001');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_LDR_TRZ=$(echo "${NOMBRE_FICH_DATOS}" | sed -e ''s/\.[Dd][Aa][Tt]/\.log/'')');
      UTL_FILE.put_line(fich_salida_sh, '  FILE_NAME_ESCAPADO=$(echo "${FILE}" | sed ''s/\//\\\//g'')');
      --UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS_T=$(echo ${NOMBRE_FICH_DATOS} | sed -e ''s/\.[Dd][Aa][Tt]/_/'')');
      /* (20180705) Angel Ruiz. NF. Limpieza de posibles lineas en blanco */
      UTL_FILE.put_line(fich_salida_sh, '  # Suprimimos posibles lineas en blanco y comillas dobles');
      UTL_FILE.put_line(fich_salida_sh, '  sed -e ''/^ *$/d'' -e ''/"/d'' "${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/${NOMBRE_FICH_DATOS}" > "${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/${NOMBRE_FICH_DATOS}".tmp');
      UTL_FILE.put_line(fich_salida_sh, '  mv "${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/${NOMBRE_FICH_DATOS}".tmp "${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/${NOMBRE_FICH_DATOS}"');
      /* (20180705) Angel Ruiz. FIN NF. Limpieza de posibles lineas en blanco */
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  echo "Fichero a cargar es: ${FILE}"' || ' >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
      --UTL_FILE.put_line(fich_salida_sh, '  cat ${' || NAME_DM || '_CTL}/ctl_sa_' || reg_summary.CONCEPT_NAME || '.ctl | sed "s/MY_FILE/${NOMBRE_FICH_DATOS}/g" > ' || '${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      /* (20160818) Angel Ruiz. NF: Puede existir una ruta alternativa para cargar fichero*/
      /* ya que por motivos de validacion se quiere cargar otro fichero */
      if (reg_summary.FILE_VALIDATION is null) then
      /* Se carga el fichero normal */
        UTL_FILE.put_line(fich_salida_sh, '  sed -e "s/__URL_DESTINO__/postgresql:\/\/${BD_USUARIO}@${HOST}:${PORT}\/${DB_NAME}?sslmode=prefer/g" -e "s/__RUTA_AL_FICHERO_CSV__/${FILE_NAME_ESCAPADO}/g" -e "s/__INTERFACE_NAME__/${NOMBRE_FICH_DATOS}/g" -e "s/__FCH_DATOS__/${FCH_DATOS}/g" -e "s/__FCH_DATOS_MAS_UNO__/$(("${FCH_DATOS}"+1))/g" "${' || NAME_DM || '_LDR}"/stg_' || reg_summary.CONCEPT_NAME || '.load > ' || '"${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"');
        UTL_FILE.put_line(fich_salida_sh, '  # Llamada a pgloader');
        --UTL_FILE.put_line(fich_salida_sh, '  pgloader "${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"' || ' > "${' || NAME_DM || '_TRAZAS}/${NOMBRE_FICH_LDR_TRZ}"' || ' 2>&' || '1');
        UTL_FILE.put_line(fich_salida_sh, '  pgloader "${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"' || ' >> "${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log' || ' 2>&' || '1'); 

      else
      /* Se carga el fichero alternativo para validacion */
        UTL_FILE.put_line(fich_salida_sh, '  sed -e "s/__URL_DESTINO__/postgresql:\/\/${BD_USUARIO}@${HOST}:${PORT}\/${DB_NAME}?sslmode=prefer/g" -e "s/__RUTA_AL_FICHERO_CSV__/${FILE_NAME_ESCAPADO}/g" -e "s/__INTERFACE_NAME__/${NOMBRE_FICH_DATOS}/g" -e "s/__FCH_DATOS__/${FCH_DATOS}/g" -e "s/__FCH_DATOS_MAS_UNO__/$(("${FCH_DATOS}"+1))/g" "${' || NAME_DM || '_LDR}"/stg_' || reg_summary.CONCEPT_NAME || '.load > '  || '"${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"');
        UTL_FILE.put_line(fich_salida_sh, '  # Llamada a pgloader');
        --UTL_FILE.put_line(fich_salida_sh, '  pgloader "${' || NAME_DM || '_LDR}/${NOMBRE_FICH_LDR}"' || ' >> "${' || NAME_DM || '_TRAZAS}/${NOMBRE_FICH_LDR_TRZ}"' || ' 2>&' || '1'); 
        UTL_FILE.put_line(fich_salida_sh, '  pgloader "${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"' || ' >> "${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log' || ' 2>&' || '1'); 
      end if;
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: Surgio un error en el pgloader en la carga de la tabla de staging ' || 'sah_' || reg_summary.CONCEPT_NAME || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '    echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo date >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
      UTL_FILE.put_line(fich_salida_sh, '    #Borramos el fichero ctl generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '    rm "${' || NAME_DM || '_LDR}/${NOMBRE_FICH_LDR}"');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '    echo "El proceso ha acabado con error." >> ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log');      
      UTL_FILE.put_line(fich_salida_sh, '    exit 1');    
      UTL_FILE.put_line(fich_salida_sh, '  fi');    
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  #Borramos el fichero .load generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '  rm "${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  REG_RECHAZADOS=$(grep "Total import time" ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log' || ' | awk -F'' '' '' { printf "%d",$4 } '')');
      UTL_FILE.put_line(fich_salida_sh, '  REG_RECHAZADOS=$((REG_RECHAZADOS/2)) # Se divide por 2 porque por alguna razón es el doble.');
      UTL_FILE.put_line(fich_salida_sh, '  REG_INSERTADOS=$(grep "Total import time" ' || '"${' || NAME_DM || '_TRAZAS}"/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log' || ' | awk -F'' '' '' { printf "%d",$5 } '')');
      UTL_FILE.put_line(fich_salida_sh, '  REG_LEIDOS=$((REG_INSERTADOS+REG_RECHAZADOS))');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_LEIDOS=$((TOT_LEIDOS+REG_LEIDOS))');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_INSERTADOS=$((TOT_INSERTADOS+REG_INSERTADOS))');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_RECHAZADOS=$((TOT_RECHAZADOS+REG_RECHAZADOS))');
      UTL_FILE.put_line(fich_salida_sh, '');
      
      UTL_FILE.put_line(fich_salida_sh, 'done');
      /* (20150605) FIN */
    else
      UTL_FILE.put_line(fich_salida_sh, 'TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, '# Llamada a sqlldr');
      /* (20160818) Angel Ruiz. NF: Puede existir una ruta alternativa para cargar fichero*/
      /* ya que por motivos de validacion se quiere cargar otro fichero */
      if (reg_summary.FILE_VALIDATION is null) then
      /* Se carga el fichero normal */
        UTL_FILE.put_line(fich_salida_sh, '  sed -e "s/__URL_DESTINO__/postgresql://${BD_USUARIO}@${HOST}:${PORT}/${DB_NAME}?sslmode=prefer" -e "s/__RUTA_AL_FICHERO_CSV__/${FILE}/g" -e "s/__INTERFACE_NAME__/${NOMBRE_FICH_DATOS}/g" -e "s/__FCH_DATOS__/${FCH_DATOS}/g" -e "s/__FCH_DATOS_MAS_UNO__/$(("${FCH_DATOS}"+1))/g" "${' || NAME_DM || '_LDR}"/stg_' || reg_summary.CONCEPT_NAME || '.load > '  || '"${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"');
        UTL_FILE.put_line(fich_salida_sh, '  # Llamada a pgloader');
        UTL_FILE.put_line(fich_salida_sh, '  pgloader "${' || NAME_DM || '_LDR}/${NOMBRE_FICH_LDR}"' || ' >> "${' || NAME_DM || '_TRAZAS}/load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log' || ' 2>&' || '1'); 
        UTL_FILE.put_line(fich_salida_sh, '  #Borramos el fichero .load generado en vuelo.');
        UTL_FILE.put_line(fich_salida_sh, '  rm "${' || NAME_DM || '_LDR}/${NOMBRE_FICH_LDR}"');
      else
      /* Se carga el fichero alternativo para validacion */
        UTL_FILE.put_line(fich_salida_sh, '  sed -e "s/__URL_DESTINO__/postgresql://${BD_USUARIO}@${HOST}:${PORT}/${DB_NAME}?sslmode=prefer" -e "s/__RUTA_AL_FICHERO_CSV__/${FILE}/g" -e "s/__INTERFACE_NAME__/${NOMBRE_FICH_DATOS}/g" -e "s/__FCH_DATOS__/${FCH_DATOS}/g" -e "s/__FCH_DATOS_MAS_UNO__/$(("${FCH_DATOS}"+1))/g" "${' || NAME_DM || '_LDR}"/stg_' || reg_summary.CONCEPT_NAME || '.load > '  || '"${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"');
        UTL_FILE.put_line(fich_salida_sh, '  # Llamada a pgloader');
        UTL_FILE.put_line(fich_salida_sh, '  pgloader "${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"' || ' >> "${' || NAME_DM || '_TRAZAS}/load_stg' || '_' || reg_summary.CONCEPT_NAME || '_"${FECHA_HORA}".log' || ' 2>&' || '1'); 
        UTL_FILE.put_line(fich_salida_sh, '  #Borramos el fichero .load generado en vuelo.');
        UTL_FILE.put_line(fich_salida_sh, '  rm "${' || NAME_DM || '_TMP}/${NOMBRE_FICH_LDR}"');
      end if;
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlloader en la carga de la tabla de staging ' || 'SA_' || reg_summary.CONCEPT_NAME || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '  echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');    
      UTL_FILE.put_line(fich_salida_sh, '  echo date >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
      UTL_FILE.put_line(fich_salida_sh, '  InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '  exit 1');    
      UTL_FILE.put_line(fich_salida_sh, 'fi');    
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'REG_RECHAZADOS=$(grep "Total import time" ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log | awk -F'' '' '' { printf "%d",$4 } '')');
      UTL_FILE.put_line(fich_salida_sh, 'REG_RECHAZADOS=$((REG_RECHAZADOS/2)) # Se divide por 2 porque por alguna razón es el doble.');
      UTL_FILE.put_line(fich_salida_sh, 'REG_INSERTADOS=$(grep "Total import time" ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log | awk -F'' '' '' { printf "%d",$4 } '')');
      UTL_FILE.put_line(fich_salida_sh, 'REG_LEIDOS=$((REG_INSERTADOS+REG_RECHAZADOS))');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_LEIDOS=$((TOT_LEIDOS + REG_LEIDOS))');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_INSERTADOS=$((TOT_INSERTADOS + REG_INSERTADOS))');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_RECHAZADOS=$((TOT_RECHAZADOS} + REG_RECHAZADOS))');
      UTL_FILE.put_line(fich_salida_sh, '');
    end if;
    /*(20160715) Angel Ruiz. Nueva Funcionalidad.*/
    UTL_FILE.put_line(fich_salida_sh, '# shellcheck disable=SC2129');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Resumen de la carga:" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Total registros leídos: ${TOT_LEIDOS}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Total registros insertados: ${TOT_INSERTADOS}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Total registros rechazados: ${TOT_RECHAZADOS}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, '# Insertamos que el proceso y el paso se han Ejecutado Correctamente');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinOK');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo en la carga de SA_' || reg_summary.CONCEPT_NAME || '. Error  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo "${SUBJECT}" >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, '  echo date >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '# Movemos el fichero cargado a /' || NAME_DM || '/MEX/DESTINO');    
    UTL_FILE.put_line(fich_salida_sh, 'if [ ! -d "${' || NAME_DM || '_DESTINO}/${FCH_CARGA}" ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  mkdir "${' || NAME_DM || '_DESTINO}/${FCH_CARGA}"');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    /* (20160818) Angel Ruiz. NF: Puede existir una ruta alternativa para cargar fichero*/
    /* ya que por motivos de validacion se quiere cargar otro fichero */
    if (reg_summary.FILE_VALIDATION is null) then
      /* (20160712) Angel Ruiz. CAMBIO TEMPORAL ... */
      UTL_FILE.put_line(fich_salida_sh, '# shellcheck disable=SC2129');
      UTL_FILE.put_line(fich_salida_sh, 'mv "${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || '" "${' || NAME_DM || '_DESTINO}/${FCH_DATOS}" >> "${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log ' || '2>&' || '1');    
      UTL_FILE.put_line(fich_salida_sh, 'mv "${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_flag_a_cargar || '" "${' || NAME_DM || '_DESTINO}/${FCH_DATOS}" >> "${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log ' || '2>&' || '1');    
      /* (20160712) Angel Ruiz. FIN CAMBIO TEMPORAL*/
    else
      UTL_FILE.put_line(fich_salida_sh, '# shellcheck disable=SC2129');
      UTL_FILE.put_line(fich_salida_sh, 'mv ' || nombre_interface_a_cargar || ' "${' || NAME_DM || '_DESTINO}/${FCH_CARGA}" >> "${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log ' || '2>&' || '1');    
      UTL_FILE.put_line(fich_salida_sh, 'mv ' || nombre_flag_a_cargar || ' "${' || NAME_DM || '_DESTINO}/${FCH_CARGA}" >> "${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log ' || '2>&' || '1');    
    end if;
    UTL_FILE.put_line(fich_salida_sh, 'echo "La carga de la tabla ' ||  'sah_' || reg_summary.CONCEPT_NAME || ' se ha realizado correctamente." >> ' || '"${' || NAME_DM || '_TRAZAS}/' || 'load_stg' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}".log');
    UTL_FILE.put_line(fich_salida_sh, 'exit 0');    
    /******/
    /* FIN DE LA GENERACION DEL sh de CARGA */
    /******/
      
    UTL_FILE.FCLOSE (fich_salida);
    UTL_FILE.FCLOSE (fich_salida_sh);
            
  END LOOP;
  CLOSE dtd_interfaz_summary;
END;
