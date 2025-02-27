DECLARE
    CURSOR c_mtdt_modelo_logico_TABLA
    IS
        SELECT 
        TRIM(TABLE_NAME) "TABLE_NAME",
        TRIM(TABLESPACE) "TABLESPACE",
        TRIM(CI) "CI"
        FROM MTDT_MODELO_SUMMARY
        WHERE TRIM(CI) <> 'P'   /* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */
        and SUBSTR(TRIM(TABLE_NAME), 1, 4) <> 'TRN_'  /* Las tablas de transformación no se generan */
        ;
    
    CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
    IS
        SELECT 
        TRIM(TABLE_NAME) "TABLE_NAME",
        TRIM(COLUMN_NAME) "COLUMN_NAME",
        DATA_TYPE,
        PK,
        TRIM(VDEFAULT) "VDEFAULT"
        FROM MTDT_MODELO_DETAIL
        WHERE
        TRIM(TABLE_NAME) = table_name_in
        order by POSITION;

    /* (20150907) Angel Ruiz . FIN NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */
    r_mtdt_modelo_logico_TABLA                                          c_mtdt_modelo_logico_TABLA%rowtype;
    r_mtdt_modelo_logico_COLUMNA                                    c_mtdt_modelo_logico_COLUMNA%rowtype;
    
    TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
    lista_pk                                      list_columns_primary := list_columns_primary (); 
    num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
    longitud_campo INTEGER;
    clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
    primera_col INTEGER;
    cadena_values VARCHAR2(500);
    concept_name VARCHAR2 (30);
    nombre_tabla_reducido VARCHAR2(30);
    pos_abre_paren PLS_integer;
    pos_cierra_paren PLS_integer;
    longitud_des varchar2(5);
    longitud_des_numerico PLS_integer;
    
    OWNER_SA                             VARCHAR2(60);
    OWNER_T                                VARCHAR2(60);
    OWNER_DM                            VARCHAR2(60);
    OWNER_DWH                           VARCHAR2(60);
    OWNER_MTDT                          VARCHAR2(60);
    TABLESPACE_DIM                VARCHAR2(60);
    NAME_DM                            VARCHAR(60);  
  
BEGIN

    /* (20141219) ANGEL RUIZ*/
    /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
    SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
    SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
    SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
    SELECT VALOR INTO OWNER_DWH FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DWH';
    SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
    SELECT VALOR INTO TABLESPACE_DIM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_DIM';
    SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';  
    
    /* (20141219) FIN*/

    SELECT COUNT(*) INTO num_filas FROM MTDT_MODELO_SUMMARY;
    /* COMPROBAMOS QUE TENEMOS FILAS EN NUESTRA TABLA MTDT_MODELO_LOGICO  */
    IF num_filas > 0 THEN
        /* hay filas en la tabla y por lo tanto el proceso tiene cosas que hacer  */
        OPEN c_mtdt_modelo_logico_TABLA;
        LOOP
        /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
        FETCH c_mtdt_modelo_logico_TABLA
        INTO r_mtdt_modelo_logico_TABLA;
        EXIT WHEN c_mtdt_modelo_logico_TABLA%NOTFOUND;
        --nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
        --nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, (instr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, '_')+1)); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
        --nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, instr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, '_')+1); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
        nombre_tabla_reducido := r_mtdt_modelo_logico_TABLA.TABLE_NAME; /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */

        DBMS_OUTPUT.put_line('REVOKE ALL ON ' || OWNER_DWH || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' FROM ' || OWNER_DM || ';');
        --DBMS_OUTPUT.put_line('');
        /***************************/
        
        END LOOP;
        CLOSE c_mtdt_modelo_logico_TABLA;
    END IF;
    DBMS_OUTPUT.put_line('');        
END;

