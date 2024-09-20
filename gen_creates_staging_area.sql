-- *********************************************************************** 
-- *********************************************************************** 
-- SCRIPT QUE GENERA LOS CREATES DE LAS TABLAS DEL MODELO LOGICO PARA MySQL
-- Autor: Angel Ruiz
-- Fecha: 20151211.
-- *********************************************************************** 
-- *********************************************************************** 
  DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      INTERFACE_NAME,
      TYPE,
      SEPARATOR,
      DELAYED
  FROM MTDT_INTERFACE_SUMMARY
  WHERE HISTORY IS NULL 
  --and
--  TRIM(CONCEPT_NAME) in ('USERS', 'COURSES', 'CATEGORIES', 'GROUPS', 'BRANCHES', 'TESTS', 'TEST_ANSWERS'
--  , 'SURVEY', 'SURVEYANSWERS', 'BRANCHES_COURSES', 'BRANCHES_USERS', 'CATEGORIES_COURSES', 'COURSE_USERS'
--  , 'COURSE_UNITS', 'GROUPS_COURSES', 'GROUPS_USERS', 'USER_CERTIFICATIONS', 'USER_BADGES', 'USER_PROGRESS_UNIT'
--  , 'PROFILE', 'RASGOS', 'ROLES', 'OPS', 'CONSUMER_PREFER', 'WARNINGS', 'CONSUMPTION_PREFER', 'FORMULARIO', 'EVENTS');
  --TRIM(CONCEPT_NAME) in ('VENTAS_USUARIO', 'VENTAS_MESA', 'VENTAS_TIPO_PAGO', 'PURCHASE', 'PROVIDER_IDENTITY', 'PROVIDER_ADDRESS', 'PROVIDER_CONTACT',
  --'PRODUCT', 'OFFER', 'CATEGORY', 'ORDERS', 'USER', 'PRODUCT_CATEGORY')
  ;
  CURSOR dtd_interfaz_summary_history
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      INTERFACE_NAME,
      TYPE,
      SEPARATOR,
      DELAYED,
      HISTORY
    FROM MTDT_INTERFACE_SUMMARY
    where HISTORY is not null;
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      COLUMNA,
      KEY,
      TYPE,
      LENGTH,
      NULABLE,
      PARTITIONED,
      POSITION
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      trim(CONCEPT_NAME) = trim(concep_name_in) and
      SOURCE = source_in
      order by POSITION;
      
      

  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  TYPE list_columns_partitioned  IS TABLE OF VARCHAR(30);
  TYPE list_tablas_RE IS TABLE OF VARCHAR(30);
  

  reg_summary              dtd_interfaz_summary%rowtype;
  
  reg_summary_history      dtd_interfaz_summary_history%rowtype;
  
  reg_datail               dtd_interfaz_detail%rowtype;
  
  primera_col              INTEGER;
  v_nombre_particion       VARCHAR2(30);

  lista_pk                 list_columns_primary := list_columns_primary (); 
  tipo_col                 VARCHAR(70);
  lista_par                list_columns_partitioned := list_columns_partitioned();
  v_lista_tablas_RE        list_tablas_RE := list_tablas_RE();
  lista_campos_particion   VARCHAR(250);
  no_encontrado            VARCHAR(1);
  subset                   VARCHAR(1);
  OWNER_SA                 VARCHAR2(60);
  OWNER_T                  VARCHAR2(60);
  OWNER_DM                 VARCHAR2(60);
  OWNER_MTDT               VARCHAR2(60);
  TABLESPACE_SA            VARCHAR2(60);
  NAME_DM                            VARCHAR(60);
  nombre_tabla_reducido    VARCHAR2(30);
  v_existe_tablas_RE       integer:=0;
  v_encontrado             VARCHAR2(1):='N';
    
      


BEGIN
  /* (20150119) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';  
  /* (20150119) FIN*/
  
  /* (20151117) Angel Ruiz. NF. Generacion de los creates de tablas SAD y SADH*/
  FOR nombre_tabla_HF in (
      SELECT distinct substr(table_name, 4) nombre_tabla
      FROM MTDT_TC_SCENARIO
      WHERE TABLE_TYPE = 'I'
      AND REINYECTION = 'Y')
  LOOP
    v_existe_tablas_RE:=1;
    v_lista_tablas_RE.EXTEND;
    v_lista_tablas_RE (v_lista_tablas_RE.last) := nombre_tabla_HF.nombre_tabla;
  END LOOP;
  /* (20151117) Angel Ruiz. FIN NF. Generacion de los creates de tablas SAD y SADH*/
  
  --DBMS_OUTPUT.put_line('set echo on;');
  --DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
  OPEN dtd_interfaz_summary;
  LOOP
    FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND;
      --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' CASCADE CONSTRAINTS;');
      DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME);
      DBMS_OUTPUT.put_line('(');
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      primera_col := 1;
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        IF primera_col = 1 THEN /* Si es primera columna */
          CASE
          WHEN reg_datail.TYPE = 'SE' THEN
            tipo_col := 'SERIAL';          
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'TIMESTAMPTZ';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'TIMETZ';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
          END IF;
          primera_col := 0;
        ELSE  /* si no es primera columna */
          CASE 
          WHEN reg_datail.TYPE = 'SE' THEN
            tipo_col := 'SERIAL';
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'TIMESTAMPTZ';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'TIMETZ';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '           ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '           '  || tipo_col);
          END IF;
        END IF;
        IF upper(reg_datail.KEY) = 'S'  then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := reg_datail.COLUMNA;
        END IF;
        IF reg_datail.PARTITIONED = 'S' then
          lista_par.EXTEND;
          lista_par(lista_par.LAST) := reg_datail.COLUMNA;
        END IF;
      END LOOP;
      CLOSE dtd_interfaz_detail;
      DBMS_OUTPUT.put_line(')'); /* Parentesis final del create*/
      
      /* tomamos el campo por el que va a estar particionada la tabla */
      if lista_par.COUNT > 0 then
        FOR indx IN lista_par.FIRST .. lista_par.LAST
        LOOP
          IF indx = lista_par.FIRST THEN
            lista_campos_particion:= lista_par (indx);
          ELSE
            lista_campos_particion:=lista_campos_particion || ', ' || lista_par (indx);
          END IF;
        END LOOP;
        DBMS_OUTPUT.put_line('PARTITION BY RANGE (' || lista_campos_particion || ');');   
        if (length(reg_summary.CONCEPT_NAME) <= 18) then
          v_nombre_particion := 'SA_' || reg_summary.CONCEPT_NAME;
        else
          v_nombre_particion := reg_summary.CONCEPT_NAME;
        end if;
        /* (20190925) ANGEL RUIZ. BUG. Si no hay tablespace no se escribe */
        if (TABLESPACE_SA is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || TABLESPACE_SA || ';');
        end if;

        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-60,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-60,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-59,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-59,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-59,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-58,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-58,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-58,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-57,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-57,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-57,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-56,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-56,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-56,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-55,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-55,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-55,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-54,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-54,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-54,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-53,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-53,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-53,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-52,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-52,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-52,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-51,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-51,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-51,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-50,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-50,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-50,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-49,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-49,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-49,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-48,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-48,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-48,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-47,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-47,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-47,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-46,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-46,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-46,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-45,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-45,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-45,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-44,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-44,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-44,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-43,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-43,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-43,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-42,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-42,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-42,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-41,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-41,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-41,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-40,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-40,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-40,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-39,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-39,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-39,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-38,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-38,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-38,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-37,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-37,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-37,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-36,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-36,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-36,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-35,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-35,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-35,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-34,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-34,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-34,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-33,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-33,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-33,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-32,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-32,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-32,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-31,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-31,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-31,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-30,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-30,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-30,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-29,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-29,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-29,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-28,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-28,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-28,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-27,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-27,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-27,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-26,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-26,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-26,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-25,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-25,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-25,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-24,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-24,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-24,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-23,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-23,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-23,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-22,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-22,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-22,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-21,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-21,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-21,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-20,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-20,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-20,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-19,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-19,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-19,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-18,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-18,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-18,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-17,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-17,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-17,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-16,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-16,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-16,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-15,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-15,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-15,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-14,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-14,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-14,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-13,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-13,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-13,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-12,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-12,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-12,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-11,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-11,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-11,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-10,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-10,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-10,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-9,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-9,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-9,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-8,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-8,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-8,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-7,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-7,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-7,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-6,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-6,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-6,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-5,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-5,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-5,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-4,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-4,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-4,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-3,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-3,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-3,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-2,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-2,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate-1,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate-1,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+1,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+1,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+2,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+2,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+3,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+3,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+3,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+4,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+4,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+4,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+5,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+5,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+5,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+6,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+6,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+6,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+7,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+7,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+7,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+8,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+8,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+8,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+9,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+9,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+9,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+10,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+10,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+10,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+11,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+11,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+11,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+12,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+12,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+12,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+13,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+13,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+13,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+14,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+14,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+14,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+15,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+15,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+15,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+16,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+16,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+16,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+17,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+17,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+17,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+18,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+18,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+18,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+19,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+19,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+19,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+20,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+20,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+20,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+21,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+21,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+21,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+22,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+22,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+22,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+23,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+23,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+23,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+24,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+24,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+24,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+25,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+25,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+25,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+26,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+26,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+26,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+27,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+27,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+27,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+28,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+28,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+28,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+29,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+29,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+29,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+30,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+30,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+30,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+31,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+31,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+31,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+32,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+32,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+32,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+33,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+33,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+33,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+34,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+34,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+34,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+35,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+35,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+35,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+36,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+36,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+36,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+37,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+37,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+37,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+38,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+38,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+38,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+39,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+39,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+39,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+40,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+40,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+40,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+41,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+41,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+41,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+42,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+42,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+42,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+43,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+43,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+43,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+44,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+44,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+44,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+45,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+45,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+45,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+46,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+46,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+46,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+47,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+47,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+47,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+48,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+48,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+48,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+49,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+49,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+49,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+50,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+50,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+50,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+51,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+51,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+51,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+52,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+52,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+52,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+53,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+53,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+53,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+54,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+54,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+54,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+55,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+55,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+55,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+56,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+56,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+56,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+57,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+57,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+57,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+58,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+58,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+58,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+59,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+59,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+59,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+60,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+60,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+60,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+61,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+61,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+61,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+62,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+62,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+62,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+63,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+63,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+63,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+64,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+64,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+64,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+65,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+65,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+65,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+66,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+66,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+66,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+67,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+67,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+67,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+68,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+68,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+68,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+69,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+69,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+69,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+70,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+70,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+70,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+71,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+71,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+71,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+72,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+72,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+72,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+73,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+73,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+73,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+74,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+74,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+74,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+75,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+75,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+75,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+76,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+76,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+76,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+77,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+77,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+77,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+78,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+78,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+78,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+79,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+79,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+79,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+80,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+80,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+80,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+81,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+81,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+81,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+82,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+82,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+82,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+83,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+83,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+83,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+84,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+84,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+84,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+85,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+85,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+85,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+86,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+86,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+86,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+87,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+87,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+87,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+88,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+88,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+88,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+89,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+89,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+89,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+90,'YYYYMMDD') || ');');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+90,'YYYYMMDD') || ' PARTITION OF ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' FOR VALUES FROM (' || TO_CHAR(sysdate+90,'YYYYMMDD') || ') TO (' || TO_CHAR(sysdate+91,'YYYYMMDD') || ');');
      end if;
      /* COMIENZO LA GESTION DE LA CREACION DE INDICES LOCALES O GLOBALES */
      IF (lista_pk.COUNT > 0 and lista_par.COUNT = 0) THEN
        /* tenemos una tabla normal no particionada */
        DBMS_OUTPUT.put_line('CREATE INDEX ON ' || OWNER_SA || '.' || 'SA_'  || reg_summary.CONCEPT_NAME || ' (');

        FOR indx IN lista_pk.FIRST .. lista_pk.LAST
        LOOP
          IF indx = lista_pk.LAST THEN
            DBMS_OUTPUT.put_line(lista_pk (indx) || ');');
          ELSE
            DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
          END IF;
        END LOOP;
      END IF;
      IF (lista_pk.COUNT > 0 and lista_par.COUNT > 0) THEN 
        /* Tenemos una tabla particionada y con PK */
        /* En posgresql creamos un índice por la clave de particionado y otro por la PK */
        /* Creamos el índice de la key de particionado */
        DBMS_OUTPUT.put_line('CREATE INDEX ON ' || OWNER_SA || '.' || 'SA_'  || reg_summary.CONCEPT_NAME || ' (');
        FOR indx IN lista_par.FIRST .. lista_par.LAST
        LOOP
          IF indx = lista_par.LAST THEN
            DBMS_OUTPUT.put_line(lista_par (indx) || ');');
          ELSE
            DBMS_OUTPUT.put_line(lista_par (indx) || ', ');
          END IF;
        END LOOP;
        /* Creamos el índice de la PK */
        DBMS_OUTPUT.put_line('CREATE INDEX ON ' || OWNER_SA || '.' || 'SA_'  || reg_summary.CONCEPT_NAME || ' (');
        FOR indx IN lista_pk.FIRST .. lista_pk.LAST
        LOOP
          IF indx = lista_pk.LAST THEN
            DBMS_OUTPUT.put_line(lista_pk (indx) || ');');
          ELSE
            if indx = lista_pk.FIRST then
              FOR indy IN lista_par.FIRST .. lista_par.LAST
              LOOP
                DBMS_OUTPUT.put_line(lista_par(indy) || ', ');
              END LOOP;
            end if;
            DBMS_OUTPUT.put_line(lista_pk (indx) || ', ');
          END IF;
        END LOOP;
      ELSE
        if (lista_pk.COUNT = 0 and lista_par.COUNT>0) then
          /* Tenemos el caso de que la tabla no tiene PK pero si esta particionada */
          /* Segun los requerimientos de MySQL podemos crear un indice por los campos del particionado */
          DBMS_OUTPUT.put_line('CREATE INDEX ON ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME || ' (');
          FOR indy IN lista_par.FIRST .. lista_par.LAST
          LOOP
              IF indy = lista_par.LAST THEN
                DBMS_OUTPUT.put_line(lista_par (indy) || '); ');
              ELSE
                DBMS_OUTPUT.put_line(lista_par (indy) || ',');
              END IF;
          END LOOP;
          --DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
        end if;
      END IF;
      DBMS_OUTPUT.put_line('');
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_par.DELETE;
  END LOOP;
  CLOSE dtd_interfaz_summary;
  /****************************************************************/
  /* (20150717) ANGEL RUIZ. NUEVA FUNCIONALIDAD.*/
  /* Las tablas de STAGING pueden tener HISTORICO */
  /* POR LO QUE HAY QUE CREAR LAS TABLAS DE HISTORICO */
  /* Cuando en Postgre tenemos una tabla de histórico */
  /* no vamos a crear ninguna tabla más que la tabla que va a almacenar */
  /* los datos de historico */
  /*****************************************************************************/
  OPEN dtd_interfaz_summary_history;
  LOOP
    FETCH dtd_interfaz_summary_history
      INTO reg_summary_history;
      EXIT WHEN dtd_interfaz_summary_history%NOTFOUND;  
      --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' CASCADE CONSTRAINTS;');
      DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_SA || '.' || 'SAH_' || reg_summary_history.CONCEPT_NAME);
      DBMS_OUTPUT.put_line('(');
      OPEN dtd_interfaz_detail (reg_summary_history.CONCEPT_NAME, reg_summary_history.SOURCE);
      primera_col := 1;
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        IF primera_col = 1 THEN /* Si es primera columna */
          CASE
          WHEN reg_datail.TYPE = 'SE' THEN
            tipo_col := 'SERIAL';                    
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'TIMESTAMPTZ';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'TIMETZ';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
          END IF;
          primera_col := 0;
        ELSE  /* si no es primera columna */
          CASE
          WHEN reg_datail.TYPE = 'SE' THEN
            tipo_col := 'SERIAL';          
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'TIMESTAMPTZ';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'TIMETZ';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          '  || tipo_col);
          END IF;
        END IF;
        IF upper(reg_datail.KEY) = 'S'  then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := reg_datail.COLUMNA;
        END IF;
        IF reg_datail.PARTITIONED = 'S' then
          lista_par.EXTEND;
          lista_par(lista_par.LAST) := reg_datail.COLUMNA;
        END IF;
      END LOOP;
      CLOSE dtd_interfaz_detail;
      /* Ahora miramos si he de crear un campo de particionado para la tabla historica */
      /* o por el contrario la tabla de STAGING ya tenia un campo de particionado */
      if (lista_par.count = 0) then
        /* La tabla de STAGING no esta particionada aunque su historica si debe estarlo*/
        DBMS_OUTPUT.put_line(', CVE_DIA          INTEGER'); /* Añado una columna de particionado */
        lista_par.EXTEND;
        lista_par(lista_par.LAST) := 'CVE_DIA'; /* La añado a la lista de campos por los que particionar mi tabla historica */
      end if;
      DBMS_OUTPUT.put_line(')'); /* Parentesis final del create */
      /* (20190925) ANGEL RUIZ. BUG. Si no hay tablespace no se escribe */      
      
      /* tomamos el campo por el que va a estar particionada la tabla */
      if lista_par.COUNT > 0 then
        FOR indx IN lista_par.FIRST .. lista_par.LAST
        LOOP
          IF indx = lista_par.FIRST THEN
            lista_campos_particion:= lista_par (indx);
          ELSE
            lista_campos_particion:=lista_campos_particion || ',' || lista_par (indx);
          END IF;
        END LOOP;
        DBMS_OUTPUT.put_line('PARTITION BY RANGE (' || lista_campos_particion || ')');   
      end if;
      if (TABLESPACE_SA is not null) then
        DBMS_OUTPUT.put_line('TABLESPACE ' || TABLESPACE_SA);
      end if;          

      DBMS_OUTPUT.put_line(';'); /* FIN CREATE */
      /* COMIENZO LA GESTION DE LA CREACION DE INDICES LOCALES O GLOBALES */
      IF (lista_pk.COUNT > 0 and lista_par.COUNT > 0) THEN 
        /* Tenemos una tabla particionada y con PK */
        /* En posgresql creamos un índice por la clave de particionado y otro por la PK */
        /* Creamos el índice de la key de particionado */
        DBMS_OUTPUT.put_line('CREATE INDEX ON ' || OWNER_SA || '.' || 'SAH_' || reg_summary_history.CONCEPT_NAME || ' (');
        FOR indy IN lista_par.FIRST .. lista_par.LAST
        LOOP
            IF indy = lista_par.LAST THEN
              DBMS_OUTPUT.put_line(lista_par (indy) || '); ');
            ELSE
              DBMS_OUTPUT.put_line(lista_par (indy) || ',');
            END IF;
        END LOOP;

        no_encontrado := 'N'; /* por defecto supongo que todos los campos de particionado forman parte del indice, de ahi no_encontrado = N */
        FOR indy IN lista_par.FIRST .. lista_par.LAST
        LOOP
          /* Para cada uno de los campos de particionado. Normalmente es uno */
          /* busco si estan en los campos del indice */
          subset := 'N';
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF (lista_par(indy) = lista_pk(indx)) THEN
              subset := 'Y';
            END IF;
          END LOOP;
          if (subset = 'N') then
            /* No he encontrado el campo de particionado en los campos que forman el indice */
            no_encontrado := 'Y';
          end if;
        END LOOP;
        /* Creamos el índice de la PK */
        DBMS_OUTPUT.put_line('CREATE INDEX ON ' || OWNER_SA || '.' || 'SAH_'  || reg_summary_history.CONCEPT_NAME || ' (');
        FOR indx IN lista_pk.FIRST .. lista_pk.LAST
        LOOP
          IF indx = lista_pk.LAST THEN
            DBMS_OUTPUT.put_line(lista_pk (indx) || '); ');
          ELSE
            DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
          END IF;
        END LOOP;
      ELSE
        if (lista_pk.COUNT = 0 and lista_par.COUNT>0) then
          /* Tenemos el caso de que la tabla no tiene PK pero si esta particionada */
          /* Segun los requerimientos de MySQL podemos crear un indice por los campos del particionado */
          DBMS_OUTPUT.put_line('CREATE INDEX ON ' || NAME_DM || '.' || 'SAH_' || reg_summary_history.CONCEPT_NAME || ' (');
          FOR indy IN lista_par.FIRST .. lista_par.LAST
          LOOP
              IF indy = lista_par.LAST THEN
                DBMS_OUTPUT.put_line(lista_par (indy) || '); ');
              ELSE
                DBMS_OUTPUT.put_line(lista_par (indy) || ',');
              END IF;
          END LOOP;
          --DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
        end if;
      END IF;
      DBMS_OUTPUT.put_line('');
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_par.DELETE;
  END LOOP;
  CLOSE dtd_interfaz_summary_history;

  /* (20150717) ANGEL RUIZ. FIN.*/
  DBMS_OUTPUT.put_line('quit');
  --DBMS_OUTPUT.put_line('set echo off;');
  --DBMS_OUTPUT.put_line('exit SUCCESS;');
END;

