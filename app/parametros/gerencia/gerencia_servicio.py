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
        self.__gerencia_excel = data
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
            novedades_de_gerencia = self.comparacion_gerencia()
            # informacion a insertar
            lista_insert = novedades_de_gerencia.get("insert")
            gerencia_update = novedades_de_gerencia.get("update")
            sin_cambios = novedades_de_gerencia.get("excepciones")
            excepciones_id_usuario = novedades_de_gerencia.get("id_usuario")
            
            historial_registro = self.insertar_inforacion(lista_insert)
            historial_actualizar = self.actualizar_informacion(gerencia_update)
            
            
            historial_registro_gerencia = {
                "historial": {
                    "gerencia_registradas": historial_registro,
                    "gerencias_actualizadas": historial_actualizar,
                    "gerencias_no_actualizadas": sin_cambios,
                    "gerencia_nit_no_existe": excepciones_id_usuario
                }
            }
       
            return historial_registro_gerencia
        
        except SQLAlchemyError as e:
            session.rollback()
            raise(e)
        finally:
            session.close()

    def comparacion_gerencia(self):
        try:
            gerencias_nuevas, gerencias_actualizacion, no_sufieron_cambios, excepciones_id_usuario = self.filtrar_gerencias()
            
            resultado = {
                "insert": gerencias_nuevas,
                "update": gerencias_actualizacion,
                'excepciones': no_sufieron_cambios,
                'id_usuario': excepciones_id_usuario
            }

            return resultado
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}")


    
    def filtrar_gerencias(self):
        gerencias_nuevas = self.filtrar_gerencias_nuevas()
        gerencias_actualizacion = self.obtener_gerencias_actualizacion()
        excepciones_gerencia = self.obtener_no_sufrieron_cambios()
        excepciones_id_usuario = self.excepciones_id_usuario()

        actualizacion = [
            item for item in gerencias_actualizacion
            if {'unidad_gerencia_id_erp': item['unidad_gerencia_id_erp'], 'nombre': item['nombre']} not in excepciones_gerencia
        ]
        
        return gerencias_nuevas, actualizacion, excepciones_gerencia,excepciones_id_usuario
    

    #  esta funcion sirve para validar lo que se envia en excel contra lo que recibe en la base de datos
    #  sacando asi los valores nuevos qeu no existen ninguno en la base de datos es decir se insertan
    def filtrar_gerencias_nuevas(self):
            gerencias_nuevas = [item for item in self.__gerencia if (
                item["responsable_id"] is not None and
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
                    'responsable_id': item["responsable_id"],
                }
                for item in self.__gerencia
                for g in self.__obtener_gerencia_existente

                if (
                    item["responsable_id"] is not None and
                    item["unidad_gerencia_id_erp"].strip().lower() == g["unidad_gerencia_id_erp"].strip().lower()
                )
            ]
        return gerencias_actualizacion

    # esto son los que no han tenido nada de cambios pero lo han querido enviar a actualizar
    #  se lleva un registro de estos
    # esta validacion es para los usuarios que tienen items repetidos
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
    
    # esto son los que no han tenido nada de cambios pero lo han querido enviar a actualizar
    #  se lleva un registro de estos
    # esta validacion es para los usuarios que tienen items repetidos
    def excepciones_id_usuario(self):
        id_usuario_no_existe = [
        {
            'unidad_gerencia_id_erp': item["unidad_gerencia_id_erp"],
            'nombre': item["nombre"],
        }
        for item in self.__gerencia
        if (
            (item["responsable_id"]) is None
        )
    ]
        return id_usuario_no_existe
    

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
                        "responsable_id": usuario.to_dict().get("id_usuario")
                    })
                else:
                    resultados.append({
                        "unidad_gerencia_id_erp": gerencia['unidad_gerencia_id_erp'],
                        "nombre": gerencia['nombre'],
                        "responsable_id": None
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


    def insertar_inforacion(self,novedades_de_gerencia:List):
        
        if len(novedades_de_gerencia)>0:
            
            session.bulk_insert_mappings(ProyectoUnidadGerencia, novedades_de_gerencia)
            session.commit()
            return novedades_de_gerencia
        
        return 'No se han registrado datos'
    
    def actualizar_informacion(self,actualizacion_gerencia):
        if len(actualizacion_gerencia)>0:
            
            session.bulk_update_mappings(ProyectoUnidadGerencia, actualizacion_gerencia)
            session.commit()
            
            return actualizacion_gerencia
        
        return 'No se han actualizado datos'
