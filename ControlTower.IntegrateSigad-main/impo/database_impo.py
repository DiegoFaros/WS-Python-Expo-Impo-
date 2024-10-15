import zeep
import pandas as pd
import xmltodict
from requests.auth import HTTPBasicAuth
from zeep.transports import Transport
from requests import Session
import csv


def run_database_impo():
    # Configuración de las credenciales y del servicio SOAP
    wsdl_url = 'https://sigad.telleria.cl/SigadIEDTelleria/ws/ListadoSelectivo?wsdl'
    username = 'htl.faros'
    password = 'RFzYN8n97vZMUsu6'

    session = Session()
    session.auth = HTTPBasicAuth(username, password)
    transport = Transport(session=session)
    client = zeep.Client(wsdl=wsdl_url, transport=transport)

    # Realizar la solicitud SOAP
    consulta_reporte = {
        'usuarioSigad': 'silicato, mariana',
        'nombreReporte': 'Info Estad Impo',
        'operacion': 'impo'
    }

    response = client.service.consultaReporteMensual(**consulta_reporte)

    xml_content = response

    if isinstance(xml_content, str):
        parsed_data = xmltodict.parse(xml_content)

        # Extraer los registros
        registros = parsed_data['Listado']['Registro']

        # Función para limpiar y formatear números, reemplazando puntos por comas
        def clean_and_format(value):
            if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                return value.replace('.', ',')
            return value

        # Extraer solo el valor de '#text' si está presente en un diccionario
        def extraer_texto(value):
            if isinstance(value, dict) and '#text' in value:
                return value['#text']
            return ''

        # Crear lista de registros procesados
        data = []

        for registro in registros:
            # Formatear cada registro y extraer el texto o dejar vacío si no tiene valor
            registro_modificado = {key: clean_and_format(extraer_texto(value)) for key, value in registro.items()}
            data.append(registro_modificado)

        # Guardar los datos formateados en un archivo CSV
        csv_file_path = 'reporte2.csv'
        df = pd.DataFrame(data)
        df.to_csv(csv_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

        print(f"Los datos se han guardado correctamente en '{csv_file_path}'")
