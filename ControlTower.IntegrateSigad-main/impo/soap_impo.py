import pandas as pd
import pyodbc

def run_soap_impo():
    csv_file_path = 'reporte2.csv'  # Ruta del archivo CSV

    try:
        df = pd.read_csv(csv_file_path)
        print("Datos cargados exitosamente desde el archivo CSV")
        print(df.head())  # Muestra las primeras filas del DataFrame
        print(df.columns)  # Muestra las columnas del DataFrame
    except Exception as e:
        print(f"Error al cargar el archivo CSV: {e}")
        exit()

    # Conexión a la base de datos SQL Server
    try:
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=10.0.0.4;'  # Cambia si es necesario
            'DATABASE=TelleriaDB;'  # Cambia a tu nombre de base de datos
            'UID=Faros;'  # Cambia a tu usuario
            'PWD=5A*107jflx<j;'  # Cambia a tu contraseña
        )

        cursor = connection.cursor()
        print("Conexión a la base de datos SQL Server exitosa")

        # Reemplaza nan por '' en el DataFrame
        df.fillna('', inplace=True)

        # Crear o verificar tabla con todas las columnas como NVARCHAR(255)
        cursor.execute('''
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'WS_Importaciones')
            BEGIN
                CREATE TABLE WS_Importaciones (
                    [FECHA ACEPTACION] DATETIME,
                    [DESPACHO] FLOAT,
                    [FORMA PAGO COB DESC] NVARCHAR(255),
                    [CLAUSULA COMPRAVEN DESC] NVARCHAR(255),
                    [ADUANA NOMBRE] NVARCHAR(255),
                    [PAIS ORIGEN DESCRIPCION] NVARCHAR(255),
                    [REGIMEN IMPORT. DESCRIPCION] NVARCHAR(255),
                    [OPERACION NOMBRE] NVARCHAR(255),
                    [MERCADERIA DESCRIPCION] NVARCHAR(255),
                    [PARTIDA ARANCELARIA] NVARCHAR(255),
                    [COMPANIA TRANSPORTE] NVARCHAR(255),
                    [CONOC EMBARQUE EMISOR] NVARCHAR(255),
                    [PAIS ADQ DESCRIPCION] NVARCHAR(255),
                    [PUERTO EMB DESCRIPCION] NVARCHAR(255),
                    [TIPO TRAMITE] NVARCHAR(255),
                    [VALOR CIF] FLOAT,
                    [VALOR FLETE] FLOAT,
                    [VALOR SEGURO] FLOAT,
                    [VALOR FOB] FLOAT,
                    [VALOR EXFAB] FLOAT,
                    [VALOR DERECHOS] FLOAT,
                    [BULTOS TIPO] NVARCHAR(255),
                    [BULTOS DESCRIPCION] NVARCHAR(255),
                    [BULTOS CANTIDAD] FLOAT,
                    [BULTOS SUBCONTINENTE] NVARCHAR(255),
                    [CONSIGNANTE NOMBRE] NVARCHAR(255),
                    [KILOS BRUTOS] FLOAT,
                    [MANIFIESTO FECHA] NVARCHAR(255),
                    [OBSERVACIONES] NVARCHAR(255),
                    [CLIENTE CODIGO] FLOAT,
                    [RUT CLIENTE] NVARCHAR(255),
                    [CLIENTE NOMBRE] NVARCHAR(255),
                    [DESPACHADOR] NVARCHAR(255),
                    [TOTAL DESEMBOLSOS] FLOAT,
                    [REFERENCIA] NVARCHAR(255),
                    [TOTAL DEREC INTER] FLOAT,
                    [NRO ACEPTACION] NVARCHAR(255),
                    [CONOC EMBARQUE NUMERO] NVARCHAR(255),
                    [CANTIDAD ITEMS] FLOAT,
                    [AFORO] NVARCHAR(255),
                    [VALOR CTA.300 USD] FLOAT,
                    [VALOR CTA223 EN_PESO] FLOAT,
                    [ESTADO DESCRIPCION] NVARCHAR(255),
                    [TIPO CAMBIO] VARCHAR(255),
                    [VIA TRANSPORTE DESC] VARCHAR(255)
                );
            END
        ''')

        print("Tabla 'WS_Importaciones' creada o verificada con éxito")

        for index, row in df.iterrows():
            try:
                # Convertir valores numéricos a cadenas
                values = (
                    row.get('FECHA_ACEPT', ''),
                    row.get('ID_DESPACHO', ''),
                    row.get('NOMBRE_FPAGOCOB', ''),
                    row.get('NOMBRE_COMPRAVEN', ''),
                    row.get('DESC_ADNA_ACEPT', ''),
                    row.get('NOMBRE_PAIS_ORG', ''),
                    row.get('DESC_REGIMP', ''),
                    row.get('DESC_OPER', ''),
                    row.get('MERCADERIA', ''),
                    row.get('ID_PART', ''),
                    row.get('NOMBRE_CIAT', ''),
                    row.get('NOMBRE_EMICEM', ''),
                    row.get('NOMBRE_PAIS_ADQ', ''),
                    row.get('NOMBRE_PUERTO_EMB', ''),
                    row.get('TIPOTRAMITE', ''),
                    str(row.get('VALOR_CIF', '0')),  # Convertir a cadena
                    str(row.get('VALOR_FLE', '0')),
                    str(row.get('VALOR_SEG', '0')),
                    str(row.get('VALOR_FOB', '0')),
                    str(row.get('VALOR_EXFAB', '0')),
                    str(row.get('VALOR_DEREC', '0')),
                    row.get('CLASE_BULTOS', ''),
                    row.get('DESC_BULTOS', ''),
                    str(row.get('QBULTOS', '0')),
                    row.get('SUB_CONTINENTE', ''),
                    row.get('NOMBRE_CONSIG', ''),
                    str(row.get('KILOS_BRT', '0')),
                    row.get('FECHA_MANIF', ''),
                    row.get('OBSERVACIONES', ''),
                    row.get('ID_CLIE', ''),
                    row.get('RUT_CLIE', ''),
                    row.get('RSOCIAL', ''),
                    row.get('ID_DESP', ''),
                    str(row.get('TOT_DESEMBOLSOS', '0')),
                    row.get('REF_CLTE', ''),
                    str(row.get('TOTAL_DEREC_INTER', '0')),
                    row.get('NRO_ACEPT', ''),
                    row.get('NRO_CEMB', ''),
                    str(row.get('QITEMS', '0')),
                    row.get('AFORO', ''),
                    str(row.get('VALOR_CTA300', '0')),
                    str(row.get('VALOR_CTA223_PESO', '0')),
                    row.get('DESC_ESTADO', ''),
                    str(row.get('TIPO_CAMBIO', '0')),
                    row.get('DESC_VIATRAN', '')
                )

                print(f"Insertando fila {index + 1}: {values}")
                cursor.execute('''
                    INSERT INTO WS_Importaciones (
                        [FECHA ACEPTACION], [DESPACHO], [FORMA PAGO COB DESC], [CLAUSULA COMPRAVEN DESC], [ADUANA NOMBRE], 
                        [PAIS ORIGEN DESCRIPCION], [REGIMEN IMPORT. DESCRIPCION], [OPERACION NOMBRE], [MERCADERIA DESCRIPCION], 
                        [PARTIDA ARANCELARIA], [COMPANIA TRANSPORTE], [CONOC EMBARQUE EMISOR], [PAIS ADQ DESCRIPCION], 
                        [PUERTO EMB DESCRIPCION], [TIPO TRAMITE], [VALOR CIF], [VALOR FLETE], [VALOR SEGURO], 
                        [VALOR FOB], [VALOR EXFAB], [VALOR DERECHOS], [BULTOS TIPO], [BULTOS DESCRIPCION], 
                        [BULTOS CANTIDAD], [BULTOS SUBCONTINENTE], [CONSIGNANTE NOMBRE], [KILOS BRUTOS], 
                        [MANIFIESTO FECHA], [OBSERVACIONES], [CLIENTE CODIGO], [RUT CLIENTE], [CLIENTE NOMBRE], 
                        [DESPACHADOR], [TOTAL DESEMBOLSOS], [REFERENCIA], [TOTAL DEREC INTER], [NRO ACEPTACION], 
                        [CONOC EMBARQUE NUMERO], [CANTIDAD ITEMS], [AFORO], [VALOR CTA.300 USD], [VALOR CTA223 EN_PESO], 
                        [ESTADO DESCRIPCION], [TIPO CAMBIO], [VIA TRANSPORTE DESC]
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', values)

            except Exception as e:
                print(f"Error al insertar fila {index + 1}: {e}")

        connection.commit()
        print("Datos insertados exitosamente")

    except Exception as e:
        print(f"Error de conexión o inserción en la base de datos: {e}")
    finally:
        cursor.close()
        connection.close()
        print("Conexión a la base de datos cerrada")
