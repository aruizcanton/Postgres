-- *********************************************************************** 
-- *********************************************************************** 
-- SCRIPT QUE GENERA LOS CREATES DE LAS TABLAS DEL MODELO LOGICO PARA MySQL
-- Autor: Angel Ruiz
-- Fecha: 20151202.
-- *********************************************************************** 
-- *********************************************************************** 

DECLARE
  /* (20150907) Angel Ruiz . NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */

    /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR c_mtdt_modelo_logico_TABLA
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLESPACE) "TABLESPACE",
      TRIM(CI) "CI",
      TRIM(PARTICIONADO) "PARTICIONADO"
    FROM MTDT_MODELO_SUMMARY
    WHERE TRIM(CI) <> 'P'
    ;    /* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */

    
  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(COLUMN_NAME) "COLUMN_NAME",
      DATA_TYPE,
      PK,
      TRIM(NULABLE) "NULABLE",
      TRIM(VDEFAULT) "VDEFAULT"
    FROM MTDT_MODELO_DETAIL
    WHERE
      TABLE_NAME = table_name_in
    ORDER BY POSITION;
  /* (20150907) Angel Ruiz . FIN NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */

  r_mtdt_modelo_logico_TABLA                                          c_mtdt_modelo_logico_TABLA%rowtype;
  r_mtdt_modelo_logico_COLUMNA                                    c_mtdt_modelo_logico_COLUMNA%rowtype;
  
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  lista_pk                                      list_columns_primary := list_columns_primary ();
  list_pk_with_partition_key                    list_columns_primary := list_columns_primary ();
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
  primera_col INTEGER;
  cadena_values VARCHAR2(5000);
  concept_name VARCHAR2 (30);
  nombre_tabla_reducido VARCHAR2(30);
  v_nombre_particion VARCHAR2(30);
  pos_abre_paren PLS_integer;
  pos_cierra_paren PLS_integer;
  longitud_des varchar2(5);
  longitud_des_numerico PLS_integer;
  v_tipo_particionado VARCHAR2(10);
  v_campo_parti_en_pk    PLS_integer;
  v_CVE_DIA_es_col        PLS_integer;
  v_FCT_DT_KEY_es_col     PLS_integer;
  v_CVE_MES_es_col      PLS_integer;
  v_CVE_WEEK_es_col     PLS_integer;
  v_nombre_campo_particionado VARCHAR2(30);
  
  OWNER_SA          VARCHAR2(60);
  OWNER_T           VARCHAR2(60);
  OWNER_DM          VARCHAR2(60);
  OWNER_MTDT        VARCHAR2(60);
  TABLESPACE_DIM    VARCHAR2(60);
  NAME_DM                            VARCHAR(60);  
  
BEGIN

  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO TABLESPACE_DIM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_DIM';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';  
  
  /* (20141219) FIN*/

  SELECT COUNT(*) INTO num_filas FROM MTDT_MODELO_SUMMARY;
  /* COMPROBAMOS QUE TENEMOS FILAS EN NUESTRA TABLA MTDT_MODELO_LOGICO  */
  IF num_filas > 0 THEN
    /* hay filas en la tabla y por lo tanto el proceso tiene cosas que hacer  */
    --DBMS_OUTPUT.put_line('set echo on;');
    --DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
    OPEN c_mtdt_modelo_logico_TABLA;
    LOOP
      /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
      FETCH c_mtdt_modelo_logico_TABLA
      INTO r_mtdt_modelo_logico_TABLA;
      EXIT WHEN c_mtdt_modelo_logico_TABLA%NOTFOUND;
      --nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
      --nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, instr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, '_')+1); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
      nombre_tabla_reducido := r_mtdt_modelo_logico_TABLA.TABLE_NAME;  /* En postgres no existe el limitante de 30 caracteres para el nombre de la tabla */
      v_CVE_DIA_es_col:=0;
      v_CVE_MES_es_col:=0;
      v_FCT_DT_KEY_es_col:=0;
      DBMS_OUTPUT.put_line('CREATE TABLE IF NOT EXISTS ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      DBMS_OUTPUT.put_line('(');
      concept_name := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5);
      OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      primera_col := 1;
      v_tipo_particionado := 'S';  /* (20150821) Angel Ruiz. Por defecto la tabla no estara particionada */
      LOOP
        FETCH c_mtdt_modelo_logico_COLUMNA
        INTO r_mtdt_modelo_logico_COLUMNA;
        EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
        /* COMENZAMOS EL BUCLE QUE GENERARA LAS COLUMNAS */
        IF primera_col = 1 THEN /* Si es primera columna */
          IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
            CASE
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 OR INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                /* (20200302) Angel Ruiz. NF. Se trata de un valor AUTOINCREMENT */
                if (UPPER(TRIM(r_mtdt_modelo_logico_COLUMNA.VDEFAULT)) = 'AUTO') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL' || ' NOT NULL');
                else
                  if ((r_mtdt_modelo_logico_TABLA.CI = 'I' or r_mtdt_modelo_logico_TABLA.CI = 'F')  and ((r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5)) or (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID'))) then
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      --DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL AUTO_INCREMENT');
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                    else
                      --DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' AUTO_INCREMENT');
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                    end if;
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then
                      /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */
                      /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/                      
                      if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      else
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      end if;                    
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                      end if;
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                      end if;
                    end if;
                  end if;
                end if;
                /* (20200302) Angel Ruiz. NF FIN. Se trata de un valor AUTOINCREMENT */
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                end if;
              ELSE  /* se trata de Fecha  */
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                end if;
            END CASE;
          ELSE    /* NO TIENE VALOR POR DEFECTO */
            CASE
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 OR INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                if ((r_mtdt_modelo_logico_TABLA.CI = 'F' or r_mtdt_modelo_logico_TABLA.CI = 'I') and ((r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5)) or (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID'))) then
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                  end if;
                else
                  if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then
                    /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */
                    /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/
                    if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                        v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                    else
                        v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                    end if;                    
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' NOT NULL');
                    else
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                    end if;
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                    else
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                    end if;
                  end if;
                end if;
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                end if;
              ELSE  /* se trata de Fecha  */
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP'  || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' );
                end if;
            END CASE;
          END IF;
          primera_col := 0;
        ELSE  /* si no es primera columna */
          IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
            CASE 
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 or INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                /* (20200302) Angel Ruiz. NF. Se trata de un valor AUTOINCREMENT */
                if (UPPER(TRIM(r_mtdt_modelo_logico_COLUMNA.VDEFAULT)) = 'AUTO') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL' || ' NOT NULL');
                else              
                  if ((r_mtdt_modelo_logico_TABLA.CI = 'I' or r_mtdt_modelo_logico_TABLA.CI = 'F')  and ((r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5)) or (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID'))) then
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      --DBMS_OUTPUT.put_line(', `' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '`' || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL AUTO_INCREMENT');
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                    else
                      --DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' AUTO_INCREMENT');
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                    end if;
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then
                      /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */
                      /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/                      
                      if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      else
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      end if;
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                      end if;
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                      end if;
                    end if;
                  end if;
                end if;
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                end if;
              ELSE  /* se trata de Fecha  */
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                end if;
              END CASE;
          ELSE  /* Si no existen valores por defecto */
            CASE 
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 or INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                if (r_mtdt_modelo_logico_TABLA.CI = 'N' and (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID')) then
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                  end if;
                else
                  if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then
                    /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */                  
                    /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/                    
                    if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                        v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                    else
                        v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                    end if;
                    /* (20250126) Angel Ruiz. FIN*/
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' NOT NULL');
                    else
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                    end if;
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                    else
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                    end if;
                  end if;
                end if;
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                end if;
              ELSE  /* se trata de Fecha  */
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP');
                end if;
              END CASE;
          END IF;
        END IF;
        /* (20151217) Angel Ruiz. BUG. Si se crea un campo AUTOINCREMENT, este debe estar en la PK */
        /* ya que si no habra un error */
        --IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' then
        IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' or ((r_mtdt_modelo_logico_TABLA.CI = 'I' or r_mtdt_modelo_logico_TABLA.CI = 'F') and r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5)) then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
        END IF;
        /* (20150821) ANGEL RUIZ. FUNCIONALIDAD PARA PARTICIONADO */
        if ((regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) AND 
        (upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_DIA' or upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_DAY' or upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'DAY')) then 
          /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_DIA ==> PARTICIONADO DIARIO */
          v_tipo_particionado := 'D';   /* Particionado Diario */
          v_CVE_DIA_es_col := 1;
        end if;
        if (regexp_count(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '^?+_FCT$',1,'i') >0 AND 
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'FCT_DT_KEY') then 
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
          if (r_mtdt_modelo_logico_TABLA.PARTICIONADO = 'M24') then
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
        /* (20150821) ANGEL RUIZ. FIN FUNCIONALIDAD PARA PARTICIONADO */
      END LOOP; 
      CLOSE c_mtdt_modelo_logico_COLUMNA;
      IF lista_pk.COUNT > 0 THEN
        /* ANGEL RUIZ (20240923) Trato el particionado desde el punto de vista de PostgreSQL -i */

        DBMS_OUTPUT.put_line(',' || ' PRIMARY KEY (');
        /* Si la tabla esta particionada, hay que incluir el campo de particionado en la PK */
        /* y además pongo la clave de particionado en el primer lugar */
        list_pk_with_partition_key.delete;
        IF v_tipo_particionado != 'S' THEN
          /* La tabla esta particionada */
          list_pk_with_partition_key.EXTEND;
          list_pk_with_partition_key(list_pk_with_partition_key.LAST) := v_nombre_campo_particionado; /* Guardo los campos de la PK que son campos de particionado */
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            if (lista_pk(indx) != v_nombre_campo_particionado) then
              list_pk_with_partition_key.EXTEND;
              list_pk_with_partition_key(list_pk_with_partition_key.LAST) := lista_pk (indx); /* Guardo los campos de la PK que no son campos de particionado */
            end if;
          end loop;
        END IF;
        /* Despues escribo los campos de la PK dependiendo de si la tabla esta particionada o no */
        if v_tipo_particionado = 'S' then
          /* La tabla no está particionada */
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          loop
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          end loop;
        else
          /* La tabla está particionada */
          FOR indx IN list_pk_with_partition_key.FIRST .. list_pk_with_partition_key.LAST
          loop
            IF indx = list_pk_with_partition_key.LAST THEN
              DBMS_OUTPUT.put_line(list_pk_with_partition_key (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(list_pk_with_partition_key (indx) || ',');
            END IF;
          end loop;
        end if;
      ELSE
        /* No hay PK definida en la especificación */
        /* Tenemos que mirar si hay un campo de particionado */
        IF v_tipo_particionado != 'S' THEN
          /* La tabla esta particionada */
          DBMS_OUTPUT.put_line(',' || ' PRIMARY KEY (');
          DBMS_OUTPUT.put_line(v_nombre_campo_particionado || ')');
        END IF;
      END IF;
      DBMS_OUTPUT.put_line(')');  /* Parentesis final del create */
      if ((regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0)
        or (regexp_count(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '^?+_FCT',1,'i') >0)
      )  then  /* Se trata de una tabla de HECHOS  */
        --  /* Hay que particonarla */
        if (v_tipo_particionado = 'D') then
          /* Se trata de un particionado diario */
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (' || v_nombre_campo_particionado || ')');
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
          else
            DBMS_OUTPUT.put_line(';');
          end if;
        elsif (v_tipo_particionado = 'M') then
          /* Se trata de un particionado Mensual */
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_MES)');
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
          else
            DBMS_OUTPUT.put_line(';');
          end if;
        elsif (v_tipo_particionado = 'W') then
          /* (20200129) Angel Ruiz */
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (' || v_nombre_campo_particionado || ')');
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
          else
            DBMS_OUTPUT.put_line(';');
          end if;
        elsif (v_tipo_particionado = 'M24') then
          /* (20150918) Angel Ruiz. N.F.: Se trata de implementar el particionado para BSC donde hay 24 particiones siempre */
          /* Las particiones se crean una vez y asi permanecen ya que el espacio de analisis se extiende 24 meses */
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_MES)');
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
          else
            DBMS_OUTPUT.put_line(';');
          end if;
          /* (20150918) Angel Ruiz. Fin N.F*/
        else
          /* (20180615). Angel Ruiz. No hay ningún campo CVE_DIA O CVE_MES, por lo que no se particiona */
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
          else
            DBMS_OUTPUT.put_line(';');
          end if;
        end if;
      elsif (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), '??A_',1,'i') >0)  then  /* Se trata de una tabla de HECHOS AGREGADOS  */
        if (v_tipo_particionado = 'M') then
          --  /* Hay que particonarla */
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE);
          end if;
          
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_MES)');
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
          else
            DBMS_OUTPUT.put_line(';');
          end if;
        end if;
      else
        if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
          DBMS_OUTPUT.put_line ('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
        else
          DBMS_OUTPUT.put_line (';');
        end if;
      end if;
      
      /* (20250124) Angel Ruiz. Modifico para distribuir las tablas de dimensiones */
      if ((regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, '_')), '^?+D_',1,'i') >0)
        or (regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME, '^?+_DIM',1,'i') >0)
      )  then  /* Se trata de una dimension  */
        DBMS_OUTPUT.put_line ('SELECT create_reference_table(''' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ''');');
      end if;
      /* (20250124) Angel Ruiz. Fin */
      --DBMS_OUTPUT.put_line(';');
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      DBMS_OUTPUT.put_line('');
      /***************************/
      /* AHORA CREAMOS LA TABLA TEMPORAL PERO SOLO PARA AQUELLAS QUE NO SE VAN A CARGAR COMO CARGA INICIAL */
      if (r_mtdt_modelo_logico_TABLA.CI = 'N' or r_mtdt_modelo_logico_TABLA.CI = 'F' or r_mtdt_modelo_logico_TABLA.CI = 'I') then
        /* Aquellas que no tienen ningún tipo de carga inicial */
        --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ' CASCADE CONSTRAINTS;');
        --DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
        DBMS_OUTPUT.put_line('CREATE TABLE IF NOT EXISTS ' || OWNER_DM || '.' || 'T_' || nombre_tabla_reducido);
        DBMS_OUTPUT.put_line('(');
        concept_name := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5);
        OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
        primera_col := 1;
        LOOP
          FETCH c_mtdt_modelo_logico_COLUMNA
          INTO r_mtdt_modelo_logico_COLUMNA;
          EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
          /* COMENZAMOS EL BUCLE QUE GENERARA LAS COLUMNAS */
          IF primera_col = 1 THEN /* Si es primera columna */
            IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
              CASE
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 OR INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                  /* (20200302) Angel Ruiz. NF. Se trata de un valor AUTOINCREMENT */
                  if (UPPER(TRIM(r_mtdt_modelo_logico_COLUMNA.VDEFAULT)) = 'AUTO') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL' || ' NOT NULL');
                  else
                    if (r_mtdt_modelo_logico_TABLA.CI = 'N' and (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID')) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                      end if;
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then
                        /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */                  
                        /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/                        
                        if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                            v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                        else
                            v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                        end if;
                        /* (20250126) Angel Ruiz. FIN*/
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                          DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                        else
                          DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                        end if;
                      else
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                          DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                        else
                          DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                        end if;
                      end if;
                    end if;
                  end if;
                  /* (20200302) Angel Ruiz. NF. Se trata de un valor AUTOINCREMENT */
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                  end if;
                ELSE  /* se trata de Fecha  */
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                  end if;
              END CASE;
            ELSE
              CASE
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 OR INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                  if (r_mtdt_modelo_logico_TABLA.CI = 'N' and (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID')) then
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT'  || ' NOT NULL');
                    else
                      DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                    end if;
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' OR r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then
                      /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */                  
                      /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/                      
                      if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      else
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      end if;                      
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                      end if;
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                      end if;
                    end if;
                  end if;
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                  end if;
                ELSE  /* se trata de Fecha  */
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP'  || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' );
                  end if;
              END CASE;
            END IF;
            primera_col := 0;
          ELSE  /* si no es primera columna */
            IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
              CASE 
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 OR INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                  /* (20200302) Angel Ruiz. NF. Se trata de un valor AUTOINCREMENT */
                  if (UPPER(TRIM(r_mtdt_modelo_logico_COLUMNA.VDEFAULT)) = 'AUTO') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'SERIAL' || ' NOT NULL');
                  else
                    if (r_mtdt_modelo_logico_TABLA.CI = 'N' and (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID')) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                      end if;
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then
                        /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */                  
                        /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/
                        if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                            v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                        else
                            v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                        end if;
                        /* (20250126) Angel Ruiz. FIN*/
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                          DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                        else
                          DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                        end if;
                      else
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                          DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                        else
                          DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                        end if;
                      end if;
                    end if;
                  end if;
                  /* (20200302) Angel Ruiz. NF. Se trata de un valor AUTOINCREMENT */
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                  end if;
                ELSE  /* se trata de Fecha  */
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                  end if;
                END CASE;
            ELSE
              CASE
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0 OR INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DECIMAL') > 0) THEN
                  if (r_mtdt_modelo_logico_TABLA.CI = 'N' and (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) || '_ID')) then
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT'  || ' NOT NULL');
                    else
                      DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                    end if;
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY' OR r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_MES' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'DAY' or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_WEEK') then                
                      /* (20250126) Angel Ruiz. Las tablas DWF_ llevan el campo CVE_DIA y también el FCT_DT_KEY por lo que he de poner el siguiente if para que tome el campo correcto */                  
                      /* v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;*/
                      if (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_DIA' and regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')), '^?+F_',1,'i') >0) then
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      else
                          v_nombre_campo_particionado:= r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
                      end if;
                      /* (20250126) Angel Ruiz. FIN */
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT' || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'BIGINT');
                      end if;
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                      else
                        DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                      end if;
                    end if;
                  end if;
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')) || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'VARCHAR' || substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')));
                  end if;
                ELSE  /* se trata de Fecha  */
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP'  || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'TIMESTAMP' );
                  end if;
              END CASE;
            END IF;
          END IF;
          /* (20151217) Angel Ruiz. BUG. Si se crea un campo AUTOINCREMENT, este debe estar en la PK */
          /* ya que si no habra un error */
          --IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' then
          IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' or ((r_mtdt_modelo_logico_TABLA.CI = 'N' or r_mtdt_modelo_logico_TABLA.CI = 'F' or r_mtdt_modelo_logico_TABLA.CI = 'I') and (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'CVE_' ||  SUBSTR(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME,5) or r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME = 'FCT_DT_KEY')) then
            lista_pk.EXTEND;
            lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          END IF;
        END LOOP; 
        CLOSE c_mtdt_modelo_logico_COLUMNA;
        IF lista_pk.COUNT > 0 THEN
          DBMS_OUTPUT.put_line(',' || ' PRIMARY KEY (');
          /* Si la tabla esta particionada, hay que incluir el campo de particionado en la PK */
          /* y además pongo la clave de particionado en el primer lugar */
          list_pk_with_partition_key.delete;
          IF v_tipo_particionado != 'S' THEN
            /* La tabla está particionada */
            list_pk_with_partition_key.EXTEND;
            list_pk_with_partition_key(list_pk_with_partition_key.LAST) := v_nombre_campo_particionado; /* Guardo los campos de la PK que son campos de particionado */
            FOR indx IN lista_pk.FIRST .. lista_pk.LAST
            LOOP
              if lista_pk(indx) != v_nombre_campo_particionado then
                list_pk_with_partition_key.EXTEND;
                list_pk_with_partition_key(list_pk_with_partition_key.LAST) := lista_pk (indx); /* Guardo los campos de la PK que no son campos de particionado */
              end if;
            end loop;
          END IF;
          /* Despues escribo los campos de la PK dependiendo de si la tabla esta particionada o no */
          if v_tipo_particionado = 'S' then
            /* La tabla no está particionada */
            FOR indx IN lista_pk.FIRST .. lista_pk.LAST
            loop
              IF indx = lista_pk.LAST THEN
                DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
              ELSE
                DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
              END IF;
            end loop;
          else
            /* La tabla está particionada */
            FOR indx IN list_pk_with_partition_key.FIRST .. list_pk_with_partition_key.LAST
            loop
              IF indx = list_pk_with_partition_key.LAST THEN
                DBMS_OUTPUT.put_line(list_pk_with_partition_key (indx) || ') ');
              ELSE
                DBMS_OUTPUT.put_line(list_pk_with_partition_key (indx) || ',');
              END IF;
            end loop;
          end if;
        END IF;
        DBMS_OUTPUT.put_line(')');  /* Parentesis final del create */
        if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
          DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
        else
          DBMS_OUTPUT.put_line(';');
        end if;
        /* (20250124) Angel Ruiz. Modifico para distribuir las tablas de dimensiones */
        if ((regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, '_')), '^?+D_',1,'i') >0)
            or (regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME, '^?+_DIM',1,'i') >0)
        )  then  /* Se trata de una dimension  */      
          DBMS_OUTPUT.put_line ('SELECT create_reference_table(''' || 'T_' || nombre_tabla_reducido || ''');');    
        end if;
        /* (20250124) Angel Ruiz. Fin */        
      end if;
      
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      DBMS_OUTPUT.put_line('');
      
      
      /****************************************************************************************************/
      /* Viene la parte donde se generan los INSERTS por defecto y la SECUENCIA */
      /****************************************************************************************************/
      /* (20150826) ANGEL RUIZ. Cambio la creacion de la secuencia para que se cree secuencia para todas las tablas DIMENSIONES o HECHOS */
      --if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        --DBMS_OUTPUT.put_line('DROP SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5) || ';');
        --DBMS_OUTPUT.put_line('CREATE SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5));
        --DBMS_OUTPUT.put_line('MINVALUE 1 START WITH 1 INCREMENT BY 1;');
        --DBMS_OUTPUT.put_line('');        
      --end if;
      if (r_mtdt_modelo_logico_TABLA.CI = 'N' or r_mtdt_modelo_logico_TABLA.CI = 'F' or r_mtdt_modelo_logico_TABLA.CI = 'I') then
        /* Generamos los inserts para aquellas tablas que no son de carga inicial */
        --if (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4) ,'??D_',1,'i') >0 or regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), 'DMT_',1,'i') >0 
        --or regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), 'DWD_',1,'i') >0) then
        if (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')) ,'^?+D_',1,'i') >0 or regexp_count(trim(r_mtdt_modelo_logico_TABLA.TABLE_NAME), '^?+_DIM$',1,'i') >0 
        ) then
          DBMS_OUTPUT.put_line('');
          DBMS_OUTPUT.put_line('BEGIN;');
          /* Primero el INSERT "NO APLICA" */
          --DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                    cadena_values := '-1';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := '-1';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := 'N';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := 'NA';
                        else
                          cadena_values := 'NULL';
                        end if;
                      else
                        cadena_values := '''NA#''';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := '''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NA#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := '''NA''';
                          when longitud_des_numerico = 1 then
                            cadena_values := '''N''';
                        end case;
                      else
                        cadena_values := 'NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                      cadena_values := 'now()';
                  ELSE
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := '-1';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := 'now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 8) then
                          cadena_values := '''NO APLICA''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := '''NA#''';
                        else
                          cadena_values := '''N''';
                        end if;
                      end if;
                    else
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := '-1';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := 'now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 8) then
                          cadena_values := '''NO APLICA''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := '''NA#''';
                        else
                          --cadena_values := '''N''';
                          cadena_values := 'NULL';
                        end if;
                      end if;
                    end if;
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', -1';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3),'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0 ) then
                      cadena_values := cadena_values || ', -1';
                    else
                        if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                            cadena_values := cadena_values || ', ''N''';
                          elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                            cadena_values := cadena_values || ', ''NA''';
                          else
                            cadena_values := cadena_values || ', NULL';
                          end if;
                        else
                          cadena_values := cadena_values || ', ''NA#''';
                        end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := cadena_values || ', ''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NA#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := cadena_values || ', ''NA''';
                          when longitud_des_numerico = 1 then
                            cadena_values := cadena_values || ', ''N''';
                        end case;
                      else
                        cadena_values := cadena_values || ', NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                      cadena_values := cadena_values || ', now()';
                  ELSE
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := cadena_values || ', -2';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := cadena_values || ', now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 8) then
                          cadena_values := cadena_values || ', ''NO APLICA''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := cadena_values || ', ''NA#''';
                        else
                          cadena_values := cadena_values || ', ''N''';
                        end if;
                      end if;
                    else
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := cadena_values || ', -1';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := cadena_values || ', now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 8) then
                          cadena_values := cadena_values || ', ''NO APLICA''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := cadena_values || ', ''NA#''';
                        else
                          --cadena_values := cadena_values || ', ''N''';
                          cadena_values := cadena_values || ', NULL';
                        end if;
                      end if;
                    end if;
                END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente insert "GENERICO" */
          --DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'CVE_',1,'i') >0 THEN
                    cadena_values := '-2';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := '-2';
                    else
                        if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                            cadena_values := '''G''';
                          elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                            cadena_values := '''GE''';
                          else
                            cadena_values := 'NULL';
                          end if;
                        else
                          cadena_values := '''GE#''';
                        end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := '''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''GE#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := '''GE''';
                          when longitud_des_numerico = 1 then
                            cadena_values := '''G''';
                        end case;
                      else
                        cadena_values := 'NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                      cadena_values := 'now()';
                  ELSE
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := '-2';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := 'now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 7) then
                          cadena_values := '''GENERICO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := '''GE#''';
                        else
                          cadena_values := '''G''';
                        end if;
                      end if;
                    else
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := '-2';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := 'now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 7) then
                          cadena_values := '''GENERICO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := '''GE#''';
                        else
                          --cadena_values := '''G''';
                          cadena_values := 'NULL';
                        end if;
                      end if;
                    end if;
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', -2';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := cadena_values || ', -2';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := cadena_values || ', ''G''';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := cadena_values || ', ''GE''';
                        else
                          cadena_values := cadena_values || ', NULL';
                        end if;
                      else
                        cadena_values := cadena_values || ', ''GE#''';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := cadena_values || ', ''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''GE#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := cadena_values || ', ''GE''';
                          when longitud_des_numerico = 1 then
                            cadena_values := cadena_values || ', ''G''';
                        end case;
                      else
                        cadena_values := cadena_values || ', NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                      cadena_values := cadena_values || ', now()';
                  ELSE
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := cadena_values || ', -2';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := cadena_values || ', now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 7) then
                          cadena_values := cadena_values || ', ''GENERICO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := cadena_values || ', ''GE#''';
                        else
                          cadena_values := cadena_values || ', ''G''';
                        end if;
                      end if;
                    else
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := cadena_values || ', -2';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := cadena_values || ', now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 7) then
                          cadena_values := cadena_values || ', ''GENERICO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := cadena_values || ', ''GE#''';
                        else
                          --cadena_values := cadena_values || ', ''G''';
                          cadena_values := cadena_values || ', NULL';
                        end if;
                      end if;
                    end if;
                END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente INSERT "NO INFORMADO" */
          --DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                    cadena_values := '-3';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values :=  '-1';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := '''N''';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := '''NI''';
                        else
                          cadena_values := 'NULL';
                        end if;
                      else
                        cadena_values := '''NI#''';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := '''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NI#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := '''NI''';
                          when longitud_des_numerico = 1 then
                            cadena_values := '''N''';
                        end case;
                      else
                        cadena_values := 'NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                      cadena_values := 'now()';
                  ELSE
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := '-1';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := 'now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 11) then
                          cadena_values := '''NO INFORMADO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := '''NI#''';
                        else
                          cadena_values := '''N''';
                        end if;
                      end if;
                    else
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := '-3';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := 'now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 11) then
                          cadena_values := '''NO INFORMADO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := '''NI#''';
                        else
                          --cadena_values := '''N''';
                          cadena_values := 'NULL';                      
                        end if;
                      end if;
                    end if;
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', -3';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := cadena_values || ', -3';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := cadena_values || ', ''N''';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := cadena_values || ', ''NI''';
                        else
                          cadena_values := cadena_values || ', NULL';
                        end if;
                      else                  
                        cadena_values := cadena_values || ', ''NI#''';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := cadena_values || ', ''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NI#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := cadena_values || ', ''NI''';
                          when longitud_des_numerico = 1 then
                            cadena_values := cadena_values || ', ''N''';
                        end case;
                      else
                        cadena_values := cadena_values || ', NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'FCH_',1,'i') >0 THEN
                      cadena_values := cadena_values || ', now()';
                  ELSE
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := cadena_values || ', -3';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := cadena_values || ', now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 11) then
                          cadena_values := cadena_values || ', ''NO INFORMADO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := cadena_values || ', ''NI#''';
                        else
                          cadena_values := cadena_values || ', ''N''';
                        end if;
                      end if;
                    else
                      if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                        cadena_values := cadena_values || ', -3';
                      elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                        cadena_values := cadena_values || ', now()';
                      else
                        /* VARCHAR */
                        pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                        pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                        longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                        longitud_des_numerico := to_number(longitud_des);
                        if (longitud_des_numerico > 11) then
                          cadena_values := cadena_values || ', ''NO INFORMADO''';
                        elsif (longitud_des_numerico > 2) then
                          cadena_values := cadena_values || ', ''NI#''';
                        else
                          --cadena_values := cadena_values || ', ''N''';
                          cadena_values := cadena_values || ', NULL';
                        end if;
                      end if;
                    end if;
                END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          DBMS_OUTPUT.put_line('COMMIT;');
          DBMS_OUTPUT.put_line('');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
        end if;
      end if;
      /**********************/
      /**********************/
      
      
    END LOOP;
    CLOSE c_mtdt_modelo_logico_TABLA;
  END IF;
END;

