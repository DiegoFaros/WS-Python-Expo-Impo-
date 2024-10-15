import pandas as pd
import psycopg2
import pyodbc

def run_soap_expo():
    csv_file_path = 'reporte.csv'  # Cambia a la ruta donde guardaste el CSV
    try:
        df = pd.read_csv(csv_file_path)
        print("Datos cargados exitosamente desde el archivo CSV")
        print(df.head())  # Muestra las primeras filas del DataFrame
        print(df.columns)  # Muestra las columnas del DataFrame
    except Exception as e:
        print(f"Error al cargar el archivo CSV: {e}")
        exit()

    # Conexión a la base de datos PostgreSQL
    try:

        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=10.0.0.4;'  # Cambia si es necesario
            'DATABASE=TelleriaDB;'  # Cambia a tu nombre de base de datos
            'UID=Faros;'  # Cambia a tu usuario
            'PWD=5A*107jflx<j;'  # Cambia a tu contraseña
        )

        cursor = connection.cursor()
        print("Conexión a la base de datos PostgreSQL exitosa")

        # Crear o verificar tabla
        cursor.execute(''' IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[WS_Exportaciones]') AND type in (N'U'))
        BEGIN
                        CREATE TABLE WS_Exportaciones (
                [FECHA LEGALIZACION] NVARCHAR(255),
                [DESPACHO] NVARCHAR(255),
                [FORMA PAGO COB DESC] NVARCHAR(255),
                [FORMA PAGO CODIGO] NVARCHAR(255),
                [CLAUSULA COMPRAVEN DESC] NVARCHAR(255),
                [ADUANA NOMBRE] NVARCHAR(255),
                [PAIS ORIGEN DESCRIPCION] NVARCHAR(255),
                [PAIS DESTINO DESCRIPCION] NVARCHAR(255),
                [REGIMEN IMPORT DESCRIPCION] NVARCHAR(255),
                [OPERACION NOMBRE] NVARCHAR(255),
                [MERCADERIA DESCRIPCION] NVARCHAR(255),
                [PARTIDA ARANCELARIA] NVARCHAR(255),
                [PUERTO DESEMBARQUE DESC] NVARCHAR(255),
                [COMPANIA TRANSPORTE] NVARCHAR(255),
                [CONOC EMBARQUE EMISOR] NVARCHAR(255),
                [PAIS ADQ DESCRIPCION] NVARCHAR(255),
                [PUERTO EMB DESCRIPCION] NVARCHAR(255),
                [TIPO TRAMITE] NVARCHAR(255),
                [VALOR CIF] NVARCHAR(255),
                [VALOR FLETE] NVARCHAR(255),
                [VALOR SEG] NVARCHAR(255),
                [VALOR FOB] NVARCHAR(255),
                [VALOR EXFAB] NVARCHAR(255),
                [VALOR DERECHOS] NVARCHAR(255),
                [BULTOS TIPO] NVARCHAR(255),
                [BULTOS DESCRIPCION] NVARCHAR(255),
                [BULTOS CANTIDAD] NVARCHAR(255),
                [BULTOS SUBCONTINENTE] NVARCHAR(255),
                [CONSIGNANTE NOMBRE] NVARCHAR(255),
                [KILOS BRUTOS] NVARCHAR(255),
                [MANIFIESTO FECHA] NVARCHAR(255),
                [OBSERVACIONES] NVARCHAR(255),
                [CLIENTE CODIGO] NVARCHAR(255),
                [RUT CLIENTE] NVARCHAR(255),
                [CLIENTE NOMBRE] NVARCHAR(255),
                [DESPACHADOR] NVARCHAR(255),
                [TOTAL DESEMBOLSOS] NVARCHAR(255),
                [REFERENCIA] NVARCHAR(255),
                [TOTAL DEREC INTER] NVARCHAR(255),
                [NRO ACEPTACION] NVARCHAR(255),
                [CONOC EMBARQUE NUMERO] NVARCHAR(255),
                [CANTIDAD ITEMS] NVARCHAR(255),
                [AFORO] NVARCHAR(255),
                [VALOR CTA 300] NVARCHAR(255),
                [VALOR CTA 223 EN PESO] NVARCHAR(255),
                [ESTADO DESCRIPCION] NVARCHAR(255),
                [TIPO CAMBIO] NVARCHAR(255),
                [VIA TRANSPORTE DESC] NVARCHAR(255),
                [CLAUSULA COMPRAVEN CODIGO] NVARCHAR(255)
                );
            END
        ''')

        print("Tabla 'WS_Exportaciones' creada o verificada con éxito")

        print(len(df.columns))
        # Insertar datos
    # Insertar datos
    # Insertar datos
        # Reemplaza nan por '0' en el DataFrame
        df.fillna('0', inplace=True)

        for index, row in df.iterrows():
            try:
                values = (row['[FECHA LEGALIZACION]'] or '',
                            row['[DESPACHO]'] or '',
                            row['[FORMA PAGO COB DESC]'] or '',
                            row['[FORMA PAGO CODIGO]'] or '',
                            row['[CLAUSULA COMPRAVEN DESC]'] or '',
                            row['[ADUANA NOMBRE]'] or '',
                            row['[PAIS ORIGEN DESCRIPCION]'] or '',
                            row['[PAIS DESTINO DESCRIPCION]'] or '',
                            row['[REGIMEN IMPORT DESCRIPCION]'] or '',
                            row['[OPERACION NOMBRE]'] or '',
                            row['[MERCADERIA DESCRIPCION]'] or '',
                            row['[PARTIDA ARANCELARIA]'] or '',
                            row['[PUERTO DESEMBARQUE DESC]'] or '',
                            row['[COMPANIA TRANSPORTE]'] or '',
                            row['[CONOC EMBARQUE EMISOR]'] or '',
                            row['[PAIS ADQ DESCRIPCION]'] or '',
                            row['[PUERTO EMB DESCRIPCION]'] or '',
                            row['[TIPO TRAMITE]'] or '',
                            str(row['[VALOR CIF]']) or '0',
                            str(row['[VALOR FLETE]']) or '0',
                            str(row['[VALOR SEG]']) or '0',
                            str(row['[VALOR FOB]']) or '0',
                            str(row['[VALOR EXFAB]']) or '0',
                            str(row['[VALOR DERECHOS]']) or '0',
                            row['[BULTOS TIPO]'] or '',
                            row['[BULTOS DESCRIPCION]'] or '',
                            str(row['[BULTOS CANTIDAD]']) or '0',
                            row['[BULTOS SUBCONTINENTE]'] or '',
                            row['[CONSIGNANTE NOMBRE]'] or '',
                            str(row['[KILOS BRUTOS]']) or '0',
                            row['[MANIFIESTO FECHA]'] or '',
                            row['[OBSERVACIONES]'] or '',
                            row['[CLIENTE CODIGO]'] or '',
                            row['[RUT CLIENTE]'] or '',
                            row['[CLIENTE NOMBRE]'] or '',
                            row['[DESPACHADOR]'] or '',
                            str(row['[TOTAL DESEMBOLSOS]']) or '0',
                            row['[REFERENCIA]'] or '',
                            str(row['[TOTAL DEREC INTER]']) or '0',
                            row['[NRO ACEPTACION]'] or '',
                            row['[CONOC EMBARQUE NUMERO]'] or '',
                            str(row['[CANTIDAD ITEMS]']) or '0',
                            row['[AFORO]'] or '',
                            str(row['[VALOR CTA 300]']) or '0',
                            str(row['[VALOR CTA 223 EN PESO]']) or '0',
                            row['[ESTADO DESCRIPCION]'] or '',
                            str(row['[TIPO CAMBIO]']) or '0',
                            row['[VIA TRANSPORTE DESC]'] or '',
                            row['[CLAUSULA COMPRAVEN CODIGO]'] or ''
                )

                print(f"Intentando insertar: {values} (número de elementos: {len(values)})")
                cursor.execute('''
                    INSERT INTO WS_Exportaciones (
                            [FECHA LEGALIZACION],
                            [DESPACHO],
                            [FORMA PAGO COB DESC],
                            [FORMA PAGO CODIGO],
                            [CLAUSULA COMPRAVEN DESC],
                            [ADUANA NOMBRE],
                            [PAIS ORIGEN DESCRIPCION],
                            [PAIS DESTINO DESCRIPCION],
                            [REGIMEN IMPORT DESCRIPCION],
                            [OPERACION NOMBRE],
                            [MERCADERIA DESCRIPCION],
                            [PARTIDA ARANCELARIA],
                            [PUERTO DESEMBARQUE DESC],
                            [COMPANIA TRANSPORTE],
                            [CONOC EMBARQUE EMISOR],
                            [PAIS ADQ DESCRIPCION],
                            [PUERTO EMB DESCRIPCION],
                            [TIPO TRAMITE],
                            [VALOR CIF],
                            [VALOR FLETE],
                            [VALOR SEG],
                            [VALOR FOB],
                            [VALOR EXFAB],
                            [VALOR DERECHOS],
                            [BULTOS TIPO],
                            [BULTOS DESCRIPCION],
                            [BULTOS CANTIDAD],
                            [BULTOS SUBCONTINENTE],
                            [CONSIGNANTE NOMBRE],
                            [KILOS BRUTOS],
                            [MANIFIESTO FECHA],
                            [OBSERVACIONES],
                            [CLIENTE CODIGO],
                            [RUT CLIENTE],
                            [CLIENTE NOMBRE],
                            [DESPACHADOR],
                            [TOTAL DESEMBOLSOS],
                            [REFERENCIA],
                            [TOTAL DEREC INTER],
                            [NRO ACEPTACION],
                            [CONOC EMBARQUE NUMERO],
                            [CANTIDAD ITEMS],
                            [AFORO],
                            [VALOR CTA 300],
                            [VALOR CTA 223 EN PESO],
                            [ESTADO DESCRIPCION],
                            [TIPO CAMBIO],
                            [VIA TRANSPORTE DESC],
                            [CLAUSULA COMPRAVEN CODIGO]
                    )

                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                ''', values)
            except Exception as e:
                print(f"Error al insertar datos en la base de datos en la fila {index}: {e}")

        # Confirmar cambios
        connection.commit()
        print("Datos insertados en la base de datos con éxito")

    except Exception as e:
        print(f"Error en la conexión a la base de datos: {e}")
    finally:
        # Cerrar conexión
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Conexión a la base de datos cerrada")
