from app.database.db import session
from sqlalchemy import and_
from app.parametros.gerencia.model.datos_personales_model import UsuarioDatosPersonales
from app.parametros.gerencia.model.usuario_auth_model import UsuarioAuth
from sqlalchemy.orm import aliased
import pandas as pd

class GestorExcel:
    def __init__(self,columnas=None) -> None:
        self.columnas = columnas
    
    
    def transformacion_estados(self,duplicados_estado:dict):
        return duplicados_estado['estado'] if 'estado' in duplicados_estado and isinstance(duplicados_estado['estado'], list) else [duplicados_estado.get('estado', duplicados_estado['estado'])]
    
    def filtro_de_excpeciones(self,cliente_filtro,filtro_actualizacion,excepciones:pd.DataFrame):
    
        
        if len(filtro_actualizacion) > 0 and len(cliente_filtro) > 0:
            
            df_ceco_filtro = pd.DataFrame(filtro_actualizacion)
            df_cliente = pd.DataFrame(cliente_filtro)
            df_filtrado = excepciones[
               ~excepciones[self.columnas].isin(df_ceco_filtro[self.columnas].to_dict('list')).all(axis=1) &
                ~excepciones[self.columnas].isin(df_cliente[self.columnas].to_dict('list')).all(axis=1)
            ]
            
            filtro_combinado = df_filtrado.to_dict(orient='records')
            return {'respuesta': filtro_combinado, 'estado': 3} 
        
        elif len(filtro_actualizacion) > 0:
            df_ceco_filtro = pd.DataFrame(filtro_actualizacion)
            df_filtrado = excepciones[~excepciones[self.columnas].isin(df_ceco_filtro[self.columnas].to_dict('list')).all(axis=1)]
            filtro_ceco = df_filtrado.to_dict(orient='records')
            return {'respuesta': filtro_ceco, 'estado': 3} 
        
        elif len(cliente_filtro) > 0:
            df_cliente = pd.DataFrame(cliente_filtro)
            df_filtrado = excepciones[~excepciones[self.columnas].isin(df_cliente[self.columnas].to_dict('list')).all(axis=1)]
            
            filtro_cliente_actualizacion = df_filtrado.to_dict(orient='records')

            return {'respuesta': filtro_cliente_actualizacion, 'estado': 3} 
        else:
            return {'respuesta': [], 'estado': 0}
    
    def obtener_id_usuario(self, identificacion):
        usuario_auth_alias = aliased(UsuarioAuth)
        usuario_datos_personales_alias = aliased(UsuarioDatosPersonales)

        id_usuario = (
            session.query(usuario_auth_alias.id)
            .join(
                usuario_datos_personales_alias,
                usuario_datos_personales_alias.id_usuario == usuario_auth_alias.id,
            )
            .filter(
                and_(
                    usuario_datos_personales_alias.identificacion == identificacion,
                    usuario_auth_alias.estado == 1,
                )
            )
            .first()
        )

        resultado_id_usuario = {'id_usuario': id_usuario[0]} if id_usuario is not None else {'id_usuario': None}
        print(resultado_id_usuario)
        return resultado_id_usuario