from ast import Dict
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.parametros.gerencia.model.datos_personales_model import UsuarioDatosPersonales
from app.database.db import session
from typing import List
from app.parametros.gerencia.esquema.archivo_esquema import Archivo
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError

class Gerencia:
    def __init__(self,data:List[Archivo]) -> None:
        self.__gerencia_excel = [archivo.model_dump() for archivo in data]
        # todas las gerencias existentes en la base de datos
        self.__obtener_gerencia_existente = self.obtener()
        # gerencia que me envia el usuario a traves del excel
        self.__gerencia = self.gerencia_usuario_procesada()


    def obtener(self):
        informacion_gerencia = session.query(ProyectoUnidadGerencia).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_gerencia]
        return gerencia_data

    def registrar_informacion(self):
        try:
            # data_gerencia = [archivo.model_dump() for archivo in data]
            
            novedades_de_gerencia = self.comparacion_gerencia()
            # informacion a insertar
            lista_insert = novedades_de_gerencia.get("insert")
            gerencia_update = novedades_de_gerencia.get("update")
            sin_cambios = novedades_de_gerencia.get("excepciones")
            
            print(f'Insertar: {lista_insert}')
            print(f'Actualizar: {gerencia_update}')
            print(f'Excepciones: {sin_cambios}')
            # session.bulk_insert_mappings(ProyectoUnidadGerencia, novedades_de_gerencia)
            # session.bulk_update_mappings(ProyectoUnidadGerencia,gerencia_update)
            # session.commit()
            
            return "proceso realizado con éxito."
        except SQLAlchemyError as e:
            session.rollback()
            raise(e)
        finally:
            session.close()

    def comparacion_gerencia(self):
        try:
            print(len(self.__obtener_gerencia_existente) == len(self.__gerencia))
            if len(self.__obtener_gerencia_existente) == len(self.__gerencia):
                return "listas iguales"
            else:
                gerencias_nuevas, gerencias_actualizacion,no_sufieron_cambios = self.filtrar_gerencias()
            return {"insert": gerencias_nuevas, "update": gerencias_actualizacion,'excepciones':no_sufieron_cambios}
        except Exception as e:
            raise (f"Error al realizar la comparacion: {str(e)}")


    
    def filtrar_gerencias(self):
        gerencias_nuevas = self.filtrar_gerencias_nuevas()
        gerencias_actualizacion = self.obtener_gerencias_actualizacion()
        excepciones_gerencia = self.obtener_no_sufrieron_cambios()

        actualizacion = [
            item for item in gerencias_actualizacion
            if {'unidad_gerencia_id_erp': item['unidad_gerencia_id_erp'], 'nombre': item['nombre']} not in excepciones_gerencia
        ]
        return gerencias_nuevas, actualizacion, excepciones_gerencia
    

    #  esta funcion sirve para validar lo que se envia en excel contra lo que recibe en la base de datos
    #  sacando asi los valores nuevos qeu no existen ninguno en la base de datos es decir se insertan
    def filtrar_gerencias_nuevas(self):
            gerencias_nuevas = [item for item in self.__gerencia if (
                item["id_usuario"] is not None and
                (item["unidad_gerencia_id_erp"] or item["nombre"]) not in {(g["unidad_gerencia_id_erp"] or g["nombre"]) for g in self.__obtener_gerencia_existente}
            )]
            return gerencias_nuevas

    # Aqui se obtiene los que se pueden actualizar en la gerencia es decir los que han sufrido cambios
    def obtener_gerencias_actualizacion(self):
        gerencias_actualizacion = [
                {
                    'id': g["id"],
                    'unidad_gerencia_id_erp': item["unidad_gerencia_id_erp"],
                    'nombre': item["nombre"],
                    'id_usuario': item["id_usuario"],
                }
                for item in self.__gerencia
                for g in self.__obtener_gerencia_existente

                if (
                    item["id_usuario"] is not None and
                    item["unidad_gerencia_id_erp"].strip().lower() == g["unidad_gerencia_id_erp"].strip().lower()
                )
            ]
        return gerencias_actualizacion

    # esto son los que no han tenido nada de cambios pero lo han querido enviar a actualizar
    #  se lleva un registro de estos
    def obtener_no_sufrieron_cambios(self):
            no_sufieron_cambios = [
                {
                    'unidad_gerencia_id_erp': item["unidad_gerencia_id_erp"],
                    'nombre': item["nombre"],
                }
                for item in self.__gerencia
                for g in self.__obtener_gerencia_existente
                if (
                    (item["nombre"].upper()) == (g["nombre"].upper())
                )
            ]
            return no_sufieron_cambios
    

    #  a traves de esta funcion me va a devolver la gerencia compeleta pero con el id_usuario ya que se realiza 
    #  una consulta para obtener los id_usuario con la cedula, es decir con el nit del usuario
    def gerencia_usuario_procesada(self):
        try:
            resultados = []
            for gerencia in self.__gerencia_excel:
                usuario = self.query_usuario(gerencia['NIT'])
                if usuario:
                    resultados.append({
                        "unidad_gerencia_id_erp": gerencia['unidad_gerencia_id_erp'],
                        "nombre": gerencia['nombre'],
                        "id_usuario": usuario.to_dict().get("id_usuario")
                    })
                else:
                    resultados.append({
                        "unidad_gerencia_id_erp": gerencia['unidad_gerencia_id_erp'],
                        "nombre": gerencia['nombre'],
                        "id_usuario": None
                    })
            return resultados
        except Exception as e:
            session.rollback()
            raise (f"Error al realizar la operacion: {str(e)}")

    def query_usuario(self, identificacion):
        return session.query(UsuarioDatosPersonales).filter(
            and_(
                UsuarioDatosPersonales.identificacion == identificacion,
                UsuarioDatosPersonales.estado == 1
            )
        ).first()



