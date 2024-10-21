from zeep import Client, Transport
from requests.auth import HTTPBasicAuth
import requests
import xml.etree.ElementTree as ET
import pandas as pd

def run_database_expo():
    # URL del WSDL y credenciales
    wsdl_url = 'https://sigad.telleria.cl/SigadIEDTelleria/ws/ListadoSelectivo?wsdl'
    username = 'htl.faros'
    password = 'RFzYN8n97vZMUsu6'

    # Crear una sesión de requests
    session = requests.Session()
    # Establecer la autenticación básica en la sesión
    session.auth = HTTPBasicAuth(username, password)

    # Usar la sesión para la autenticación en el transporte
    transport = Transport(session=session, timeout=600)

    # Crear un cliente SOAP utilizando Zeep y el transporte autenticado
    client = Client(wsdl=wsdl_url, transport=transport)

    # Configurar la consulta
    consulta_reporte = {
        'usuarioSigad': 'silicato, mariana',
        'nombreReporte': 'Info Estad Expo',
        'operacion': 'expo'
    }

    # Realizar la consulta al servicio SOAP
    try:
        response = client.service.consultaReporteMensual(**consulta_reporte)
        response_xml = response
    except Exception as e:
        print(f'Error al realizar la consulta SOAP: {e}')
        exit()

    # Guardar la respuesta XML en un archivo
    xml_file_path = 'response_large.xml'
    try:
        with open(xml_file_path, 'w', encoding='utf-8') as f:
            f.write(response_xml)
        print(f"Respuesta XML guardada exitosamente en '{xml_file_path}'")
    except Exception as e:
        print(f"Error al guardar el archivo XML: {e}")
        exit()

    # Procesar el archivo XML
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error al parsear el archivo XML: {e}")
        exit()

    def clean_text(text):
        if text is not None:
            text = text.strip()  # Elimina espacios en blanco
            # Verificar si el texto puede convertirse a float
            try:
                # Si se puede convertir a float, reemplazar comas por puntos
                return str(float(text.replace(',', '.')))
            except ValueError:
                # Si no es un número, devolver el texto original
                return text
        return text  # Retorna None si el texto es None

    # Extraer los datos del XML a una lista de diccionarios, eliminando espacios
    data = []
    for registro in root.findall('.//Registro'):
        fields = {
            '[FECHA LEGALIZACION]': clean_text(registro.find('FECHA_LEGALIZACION').text) if registro.find('FECHA_LEGALIZACION') is not None else None,
            '[DESPACHO]': clean_text(registro.find('ID_DESPACHO').text) if registro.find('ID_DESPACHO') is not None else None,
            '[FORMA PAGO COB DESC]': clean_text(registro.find('NOMBRE_FPAGOCOB').text) if registro.find('NOMBRE_FPAGOCOB') is not None else None,
            '[FORMA PAGO CODIGO]': clean_text(registro.find('ID_FPAGO').text) if registro.find('ID_FPAGO') is not None else None,
            '[CLAUSULA COMPRAVEN DESC]': clean_text(registro.find('NOMBRE_COMPRAVEN').text) if registro.find('NOMBRE_COMPRAVEN') is not None else None,
            '[ADUANA NOMBRE]': clean_text(registro.find('DESC_ADNA_ACEPT').text) if registro.find('DESC_ADNA_ACEPT') is not None else None,
            '[PAIS DESTINO DESCRIPCION]': clean_text(registro.find('NOMBRE_PAIS_DES').text) if registro.find('NOMBRE_PAIS_DES') is not None else None,
            '[PAIS ORIGEN DESCRIPCION]': clean_text(registro.find('NOMBRE_PAIS_ORG').text) if registro.find('NOMBRE_PAIS_ORG') is not None else None,
            '[REGIMEN IMPORT DESCRIPCION]': clean_text(registro.find('DESC_REGIMP').text) if registro.find('DESC_REGIMP') is not None else None,
            '[OPERACION NOMBRE]': clean_text(registro.find('DESC_OPER').text) if registro.find('DESC_OPER') is not None else None,
            '[MERCADERIA DESCRIPCION]': clean_text(registro.find('MERCADERIA').text) if registro.find('MERCADERIA') is not None else None,
            '[PARTIDA ARANCELARIA]': clean_text(registro.find('ID_PART').text) if registro.find('ID_PART') is not None else None,
            '[PUERTO DESEMBARQUE DESC]': clean_text(registro.find('NOMBRE_PUERTO_DES').text) if registro.find('NOMBRE_PUERTO_DES') is not None else None,
            '[COMPANIA TRANSPORTE]': clean_text(registro.find('NOMBRE_CIAT').text) if registro.find('NOMBRE_CIAT') is not None else None,
            '[CONOC EMBARQUE EMISOR]': clean_text(registro.find('NOMBRE_EMICEM').text) if registro.find('NOMBRE_EMICEM') is not None else None,
            '[PAIS ADQ DESCRIPCION]': clean_text(registro.find('NOMBRE_PAIS_ADQ').text) if registro.find('NOMBRE_PAIS_ADQ') is not None else None,
            '[PUERTO EMB DESCRIPCION]': clean_text(registro.find('NOMBRE_PUERTO_EMB').text) if registro.find('NOMBRE_PUERTO_EMB') is not None else None,
            '[TIPO TRAMITE]': clean_text(registro.find('TIPOTRAMITE').text) if registro.find('TIPOTRAMITE') is not None else None,
            '[VALOR CIF]': clean_text(str(registro.find('VALOR_CIF').text)) if registro.find('VALOR_CIF') is not None else '0',
            '[VALOR FLETE]': clean_text(str(registro.find('VALOR_FLE').text)) if registro.find('VALOR_FLE') is not None else '0',
            '[VALOR SEG]': clean_text(str(registro.find('VALOR_SEG').text)) if registro.find('VALOR_SEG') is not None else '0',
            '[VALOR FOB]': clean_text(str(registro.find('VALOR_FOB').text)) if registro.find('VALOR_FOB') is not None else '0',
            '[VALOR EXFAB]': clean_text(str(registro.find('VALOR_EXFAB').text)) if registro.find('VALOR_EXFAB') is not None else '0',
            '[VALOR DERECHOS]': clean_text(str(registro.find('VALOR_DEREC').text)) if registro.find('VALOR_DEREC') is not None else '0',
            '[BULTOS TIPO]': clean_text(registro.find('CLASE_BULTOS').text) if registro.find('CLASE_BULTOS') is not None else None,
            '[BULTOS DESCRIPCION]': clean_text(registro.find('DESC_BULTOS').text) if registro.find('DESC_BULTOS') is not None else None,
            '[BULTOS CANTIDAD]': clean_text(str(registro.find('QBULTOS').text)) if registro.find('QBULTOS') is not None else '0',
            '[BULTOS SUBCONTINENTE]': clean_text(registro.find('SUB_CONTINENTE').text) if registro.find('SUB_CONTINENTE') is not None else None,
            '[CONSIGNANTE NOMBRE]': clean_text(registro.find('NOMBRE_CONSIG').text) if registro.find('NOMBRE_CONSIG') is not None else None,
            '[KILOS BRUTOS]': clean_text(str(registro.find('KILOS_BRT').text)) if registro.find('KILOS_BRT') is not None else '0',
            '[MANIFIESTO FECHA]': clean_text(registro.find('FECHA_MANIF').text) if registro.find('FECHA_MANIF') is not None else None,
            '[OBSERVACIONES]': clean_text(registro.find('OBSERVACIONES').text) if registro.find('OBSERVACIONES') is not None else None,
            '[CLIENTE CODIGO]': clean_text(registro.find('ID_CLIE').text) if registro.find('ID_CLIE') is not None else None,
            '[RUT CLIENTE]': clean_text(registro.find('RUT_CLIE').text) if registro.find('RUT_CLIE') is not None else None,
            '[CLIENTE NOMBRE]': clean_text(registro.find('RSOCIAL').text) if registro.find('RSOCIAL') is not None else None,
            '[DESPACHADOR]': clean_text(registro.find('ID_DESP').text) if registro.find('ID_DESP') is not None else None,
            '[TOTAL DESEMBOLSOS]': clean_text(str(registro.find('TOT_DESEMBOLSOS').text)) if registro.find('TOT_DESEMBOLSOS') is not None else '0',
            '[REFERENCIA]': clean_text(registro.find('REF_CLTE').text) if registro.find('REF_CLTE') is not None else None,
            '[TOTAL DEREC INTER]': clean_text(str(registro.find('TOTAL_DEREC_INTER').text)) if registro.find('TOTAL_DEREC_INTER') is not None else '0',
            '[NRO ACEPTACION]': clean_text(registro.find('NRO_ACEPT').text) if registro.find('NRO_ACEPT') is not None else None,
            '[CONOC EMBARQUE NUMERO]': clean_text(registro.find('NRO_CEMB').text) if registro.find('NRO_CEMB') is not None else None,
            '[CANTIDAD ITEMS]': clean_text(str(registro.find('QITEMS').text)) if registro.find('QITEMS') is not None else '0',
            '[AFORO]': clean_text(registro.find('AFORO').text) if registro.find('AFORO') is not None else None,
            '[VALOR CTA 300]': clean_text(str(registro.find('VALOR_CTA300').text)) if registro.find('VALOR_CTA300') is not None else '0',
            '[VALOR CTA 223 EN PESO]': clean_text(str(registro.find('VALOR_CTA223_PESO').text)) if registro.find('VALOR_CTA223_PESO') is not None else '0',
            '[ESTADO DESCRIPCION]': clean_text(registro.find('DESC_ESTADO').text) if registro.find('DESC_ESTADO') is not None else None,
            '[TIPO CAMBIO]': clean_text(str(registro.find('TIPO_CAMBIO').text)) if registro.find('TIPO_CAMBIO') is not None else '0',
            '[VIA TRANSPORTE DESC]': clean_text(registro.find('DESC_VIATRAN').text) if registro.find('DESC_VIATRAN') is not None else None,
            '[CLAUSULA COMPRAVEN CODIGO]': clean_text(str(registro.find('ID_COMPRAVEN').text)) if registro.find('ID_COMPRAVEN') is not None else None
        }


        data.append(fields)


    df = pd.DataFrame(data)
    csv_file_path = 'reporte.csv'  # Cambia a una ruta válida en tu sistema
    try:
        df.to_csv(csv_file_path, index=False)
        print(f"Archivo CSV guardado exitosamente en '{csv_file_path}'")
    except Exception as e:
        print(f"Error al guardar el archivo CSV: {e}")
