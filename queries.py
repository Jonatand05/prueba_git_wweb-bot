import mysql.connector
from mysql.connector import OperationalError, Error as MySQLError
from openpyxl import Workbook 
import os
from dotenv import load_dotenv

load_dotenv()

config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
}

def connectdb(base_datos):
    try:
        config['database'] = base_datos
        conexion = mysql.connector.connect(**config)
        return conexion
    except OperationalError as e:
        raise Exception(f"Error al conectarse a la base de datos: {e}")
    except MySQLError as e:
        raise Exception(f"Error en la consulta: {e}")
    except Exception as e:
        raise Exception(f"Error inesperado: {e}")

class Queries:
    # La consulta por lotes no cambia, excepto que ahora se pasará a `generate_report_excel`
    def get_db_consult(self, data, batch_size=1000):
        try:
            fecha_inicio, fecha_fin, base_datos = data
            conexion = connectdb(base_datos)

            conexion.autocommit = True
            cursor = conexion.cursor(dictionary=True)

            query = """SELECT id, fecha_inicio, fecha_fin, total, id_cliente 
                       FROM promisess 
                       WHERE fecha_inicio BETWEEN %s AND %s 
                       ORDER BY fecha_inicio"""
            cursor.execute(query, (fecha_inicio, fecha_fin))

            while True:
                ventas = cursor.fetchmany(size=batch_size)
                if not ventas:
                    break
                yield ventas #generador

            cursor.close()
            conexion.close()

        except OperationalError as e:
            raise Exception(f"Error al conectarse a la base de datos: {e}")
        except MySQLError as e:
            raise Exception(f"Error de MySQL: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado: {e}")

    # funcion para generar el reporte
    def generate_report_excel(self, data, generar_report, first_batch):
        fecha_inicio, fecha_fin, base_datos = data

        # Crear el libro de trabajo
        wb = Workbook()
        wb.remove(wb.active)  # Eliminar la hoja predeterminada 

        # diccionario para almacenar ventas por fecha
        ventas_por_fecha = {}

        # Procesar el primer bloque
        for venta in first_batch:
            fecha = venta['fecha_inicio'].strftime('%Y-%m-%d')
            if fecha not in ventas_por_fecha:
                ventas_por_fecha[fecha] = []
            ventas_por_fecha[fecha].append([
                venta['id'],
                venta['fecha_inicio'].strftime('%Y-%m-%d'),
                venta['fecha_fin'].strftime('%Y-%m-%d'),
                venta['total'],
                venta['id_cliente']
            ])

        # Procesar los siguientes bloques del generador
        for ventas_bloque in generar_report:
            for venta in ventas_bloque:
                fecha = venta['fecha_inicio'].strftime('%Y-%m-%d')
                if fecha not in ventas_por_fecha:
                    ventas_por_fecha[fecha] = []
                ventas_por_fecha[fecha].append([
                    venta['id'],
                    venta['fecha_inicio'].strftime('%Y-%m-%d'),
                    venta['fecha_fin'].strftime('%Y-%m-%d'),
                    venta['total'],
                    venta['id_cliente']
                ])

        # Crear una hoja por cada fecha única con ventas
        for fecha, ventas in ventas_por_fecha.items():
            ws = wb.create_sheet(title=fecha)
            ws.append(['ID VENTA', 'FECHA INICIO', 'FECHA FIN', 'TOTAL', 'ID CLIENTE'])
            for venta in ventas:  # Agregar todas las ventas para esa fecha
                ws.append(venta)

        # Guardar el archivo Excel
        nombre_excel = f"reporte_ventas_{base_datos}_{fecha_inicio.strftime('%Y%m%d')}_al_{fecha_fin.strftime('%Y%m%d')}.xlsx"
        wb.save(nombre_excel)

        return nombre_excel